#!/usr/bin/env python3
"""8084 行情数据新鲜度探针

【为什么需要】评审 P1-7：
- 原来的 market8084-watchdog 只探 `/api/v1/stats` 能不能返回 → **服务正常但数据恒为空测不出来**
  （tickflow 那次"静默返回空数据"就是这种：stats 照样 200）
- `market8084_watchdog.log` 一直是 0 字节 → 这条链路从没被验证过

【探什么】三个互补指标：
1. **各采集源最近一次成功的时间** —— 某个活跃源长时间没成功 = 它静默坏了
2. **近 1 小时失败率** —— 大量 failed = 采集链路在退化
3. **核心表行数增量** —— 表不再增长 = 根本没在写入（比源级更硬的证据）

【告警】超过阈值 → 推飞书（与 cert-renew 同一机制：降权到 zsd 调 hermes send）
"""
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

DB_PATH = "/home/zsd/financial-data-service/market_service/data/market.db"
LOG_PATH = "/home/zsd/financial-data-service/logs/market-data-freshness.log"
# ── 推送目标：**只从环境变量读，代码里不硬编码** ──
# 干跑观察期不提供 PROBE_FEISHU_TARGET → 目标为空 → **物理上无法推送**。
# 这比"记得关掉 DRY_RUN 开关"可靠：开关被误改也只是多打一条日志，
# 而目标为空时 notify() 直接返回，任何路径都发不出去。
HERMES_BIN = os.environ.get("PROBE_HERMES_BIN", "/home/zsd/.local/bin/hermes")
FEISHU_TARGET = os.environ.get("PROBE_FEISHU_TARGET", "")

# —— 阈值（偏宽松，优先不误报）——
ACTIVE_SOURCE_MAX_STALE_MIN = 180   # 活跃源最近成功超过 3 小时 → 异常
FAIL_RATE_WINDOW_MIN = 60            # 失败率统计窗口 1 小时
FAIL_RATE_THRESHOLD = 0.85          # 全局失败率 > 85% → 异常（保留参考）

# ---- 指标 2b：按 source 分组的失败率（2026-10-26 评审盲点 1）----
# 原指标 2 是**全源混合**，被成功的大头稀释：实测"总计 107 条, 失败 1 条, 失败率 0.9%"
# 而 eastmoney_em 那个小时是 3 次全失败 —— 混合后完全看不见。
# 改为每个源单独算：样本 >= MIN_SAMPLES 才判（样本太少不判，避免小源噪声），
# 且成功率 < 1-SUCCESS_RATE_THRESHOLD 才算异常。
PER_SOURCE_MIN_SAMPLES = 20        # 每源最小样本数，低于此不判
PER_SOURCE_SUCCESS_RATE_THRESHOLD = 0.80   # 成功率 < 80% → 异常
# ⚠️ 分组失败率必须用**长窗口**：低频源每小时只采 1~4 次
#    （实测 eastmoney_em 24h 共 96 条 ≈ 4 条/小时），
#    1 小时窗口下样本永远 < 20 → 判据形同虚设。
PER_SOURCE_WINDOW_MIN = 1440       # 24 小时
REPEAT_ERROR_WINDOW_MIN = 1440      # 24 小时（重复错误需要时间累积）

# ---- 指标 5：重复错误率（2026-10-26 评审盲点 3，静默失败最有效的特征签名）----
# 同一 (source, data_type) 的失败 error_message 高度重复
# → 说明接口**稳定返回无效内容**，不是网络抖动。
# 实测：estimate_nav 504 次错误一字不差(100% 重复)、eastmoney_em 31/31、xueqiu 16/16
#      —— 一次就能全部抓出来。
REPEAT_ERROR_MIN_COUNT = 8          # 同一错误至少出现这么多次才判
REPEAT_ERROR_MIN_RATIO = 0.7        # 重复占比 >= 70% 才判（错误高度雷同）

# ---- 已知暂停的源（2026-10-26 Chief 拍板停用 xueqiu_hot 订阅）----
# 根因：阿里云 WAF 的 JS 挑战（302 跳 g.alicdn.com/.../antidom.js），
#       首页能 200 但不下发 xq_a_token，curl/requests 全部绕不过。
# 处理：**不告警**（否则变成常态噪声），但**保留可追溯性**（日志里显式列出）。
# 将来若要恢复，优先级：① 浏览器 Cookie  ② playwright（已在 venv 与 ~/.cache 中）
# ── 已知受限源登记表 ─────────────────────────────────────────────
# ⚠️ 这不是"硬编码忽略"。每条豁免都必须登记 **原因 + 生效时间 + 复核方式**，
#    且豁免行为本身会写进日志 —— 这样"人工豁免"是**可审计**的，不等于悄悄放过。
#
# 三类，语义完全不同，绝不能混为一谈：
#
#   paused_by_decision（人为暂停）
#     源被人为停用（Chief 拍板）。不告警，但日志明确标 [已知暂停] 保留可追溯性。
#     无限期，需人工复核后才恢复。
#
#   time_windowed（时段受限）
#     源只在交易时段有意义（收盘后数据源主动断连是正常的）。
#     **交易时段内的失败仍然告警** —— 只有非交易时段才豁免。
#     随窗口滚动自动恢复正常，无需人工。
#
#   fixed_watching（已修复，观察中）
#     源已修复，但统计窗口里还含修复前的历史失败。
#     **只豁免生效时间点之前的失败**，之后的失败照常告警。
#     随窗口滚动自动恢复正常。
#
# 字段：kind / reason / since（豁免的起始时间戳）/ review（复核方式）
KNOWN_SOURCE_EXCEPTIONS = {
    "xueqiu": {
        "kind": "paused_by_decision",
        "reason": "阿里云 WAF JS 挑战（302 跳 antidom.js），"
                  "2026-10-26 经 Chief 拍板暂停订阅；Hermes 实际不消费该源",
        "since": None,          # 无限期，人工复核
        "review": "需要时用浏览器 Cookie 恢复（优先级①）或 playwright（②），"
                  "见 8084采集健康现状.md §7.0.3c",
    },
    "eastmoney_em": {
        "kind": "time_windowed",
        "reason": "东财在收盘后(15:00+)对本机出口 IP 主动断连；"
                  "资金流是日内数据，收盘后不采集是正确的",
        "since": None,          # 由交易时段判定，非固定时间点
        "review": "随统计窗口滚动自动恢复正常；"
                  "交易时段内的失败仍会告警（那才是真故障）",
    },
    ("chain", "estimate_nav"): {
        "kind": "fixed_watching",
        "reason": "东财 fundgz 估值接口已下线，2026-10-26 22:22 换新浪源；"
                  "24h 统计窗口内仍含换源前的历史失败（失败记录的 source 是聚合链 chain，"
                  "真实数据类型在 data_type）",
        "since": 1792990940.0,  # 2026-10-26 22:22 换源生效时刻
        "review": "随统计窗口滚动自动恢复正常；换源后的失败照常告警",
    },
    "estimate_nav": {
        "kind": "fixed_watching",
        "reason": "东财 fundgz 估值接口已下线，2026-10-26 22:22 换新浪源；"
                  "24h 统计窗口内仍含换源前的历史失败",
        "since": 1792990940.0,  # 2026-10-26 22:22 换源生效时刻
        "review": "随统计窗口滚动自动恢复正常；换源后的失败照常告警",
    },
}

# 兼容旧名（保留以便其它脚本/文档引用）
KNOWN_PAUSED_SOURCES = {
    k: v["reason"] for k, v in KNOWN_SOURCE_EXCEPTIONS.items()
    if v["kind"] == "paused_by_decision" and isinstance(k, str)
}
ROW_GROWTH_WINDOW_MIN = 60          # 行数增量统计窗口 1 小时
MIN_ROW_GROWTH = 1                   # 1 小时内至少应新增 1 行

# 核心源：这些是主力数据源，断了影响最大
CORE_SOURCES = ["akshare", "community_enhanced", "rss", "zhihu", "sina"]
# 核心表：这些表必须有数据写入
CORE_TABLES = ["price_data", "news_data"]
# 新闻类表：行数受 UNIQUE 去重约束影响，不用行数增量判存活（见指标 4）
NEWS_TABLES = {"news_data"}

# 行情类源/表：只在交易日有意义（周末与节假日不更新是正常的）
MARKET_SOURCES = {"akshare", "sina", "eastmoney_em", "tickflow", "massive", "tencent"}
MARKET_TABLES = {"price_data", "minute_data"}

CALENDAR_CACHE = "/home/zsd/financial-data-service/market_service/data/trade_calendar.json"
CALENDAR_MAX_AGE_H = 24 * 7   # 日历缓存 7 天

# ⚠️ 2026-10-26 修复：news_data 曾经用「行数增量」判存活 —— 这是错的。
#    news_data.link 是 UNIQUE 约束，采集器抓到的新闻若与库里已有条目重复会被直接跳过，
#    **采集全部成功但行数不涨**（安静时段/重复推送时段是常态）。
#    2026-10-26 01:29 的误报就是这么来的：collection_log 73 条全 success、失败率 0%，
#    但 news_data 近 60 分钟新增 0 行 → 被误判成「数据停止写入」。
#    正确判据 = 新闻类采集器**成功次数**（rss / zhihu / news_aggregator / sina），
#    而不是表行数。行数只作为参考信息打印，不参与告警判定。
NEWS_SOURCES = {"rss", "zhihu", "news_aggregator", "sina", "zhihu_global", "zhihu_hot_list"}
# 新闻类采集器：近 N 分钟内至少成功 N 次才算存活（安静时段容忍度高）
NEWS_COLLECTOR_WINDOW_MIN = 60
NEWS_COLLECTOR_MIN_SUCCESS = 1


_CALENDAR_CACHE = {"days": None, "loaded_at": 0.0}


def _load_trade_calendar():
    """加载 A 股交易日历（akshare tool_trade_date_hist_sina），带本地缓存。

    ⚠️ 2026-10-26 修复：原来 is_trading_day 只用 weekday() < 5 粗判，
    法定节假日（如国庆/春节）会全部误判成交易日 → 假期里行情源不更新 → 持续误报。
    现在用真实日历。取不到（断网/akshare 异常）时**降级回退到 weekday 判断**，
    保证探针本身不会因为日历不可用而挂掉。
    """
    import json
    now_ts = time.time()
    if _CALENDAR_CACHE["days"] and now_ts - _CALENDAR_CACHE["loaded_at"] < CALENDAR_MAX_AGE_H * 3600:
        return _CALENDAR_CACHE["days"]

    # 1) 磁盘缓存
    try:
        if os.path.exists(CALENDAR_CACHE) and \
                now_ts - os.path.getmtime(CALENDAR_CACHE) < CALENDAR_MAX_AGE_H * 3600:
            with open(CALENDAR_CACHE, encoding="utf-8") as f:
                days = set(json.load(f))
            if days:
                _CALENDAR_CACHE["days"] = days
                _CALENDAR_CACHE["loaded_at"] = now_ts
                return days
    except Exception:
        pass

    # 2) 联网刷新
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        days = {str(d) for d in df["trade_date"].tolist()}
        if days:
            os.makedirs(os.path.dirname(CALENDAR_CACHE), exist_ok=True)
            with open(CALENDAR_CACHE, "w", encoding="utf-8") as f:
                json.dump(sorted(days), f)
            _CALENDAR_CACHE["days"] = days
            _CALENDAR_CACHE["loaded_at"] = now_ts
            return days
    except Exception as e:
        log(f"  [WARN] 交易日历获取失败（降级为 weekday 判断）: {e}")
    return None


def is_trading_day(now=None):
    """是否交易日（真实 A 股交易日历，含法定节假日）

    2026-10-26 修复：原实现只判 weekday() < 5，节假日期间必然误报。
    """
    now = now or datetime.now(timezone(timedelta(hours=8)))
    cal = _load_trade_calendar()
    if cal is not None:
        return now.strftime("%Y-%m-%d") in cal
    # 降级路径：拿不到日历时用周一至周五粗判（宁可误报也不漏报）
    return now.weekday() < 5


def selfcheck_trading_day(now=None):
    """自检：与 check_trading_day.py 交叉验证交易日判断

    为什么需要：探针自己加载日历缓存（data/trade_calendar.json），
    而 Hermes 的 6 个 cron 用的是 ~/.hermes/scripts/check_trading_day.py。
    两者数据源独立 —— **如果那个脚本挂了，探针察觉不到**。
    所以每轮顺便跑一次它做交叉验证：
      - 脚本执行失败（挂了/超时）→ WARN，自检失效要能发现
      - 两者结论不一致 → WARN，日历不同步会让 cron 和探针各说各话
    超时保护 20s：脚本缓存失效时会联网抓，实测曾 >120s，不能拖慢探针。
    """
    now = now or datetime.now(timezone(timedelta(hours=8)))
    script = "/home/zsd/.hermes/scripts/check_trading_day.py"
    if not os.path.exists(script):
        log(f"  [自检] WARN: 找不到 {script}，无法交叉验证交易日判断")
        return
    try:
        out = subprocess.run(
            [sys.executable, script, "--date", now.strftime("%Y-%m-%d")],
            capture_output=True, text=True, timeout=20,
        ).stdout.strip()
    except subprocess.TimeoutExpired:
        log("  [自检] WARN: check_trading_day.py 执行超时(>20s)，自检失效")
        return
    except Exception as e:
        log(f"  [自检] WARN: check_trading_day.py 执行异常: {e}")
        return

    if not out:
        log("  [自检] WARN: check_trading_day.py 无输出（可能已挂），自检失效")
        return

    script_says_trading = out.startswith("TRADING_DAY")
    i_say_trading = is_trading_day(now)
    if script_says_trading != i_say_trading:
        log(f"  [自检] WARN: 交易日判断不一致 —— 脚本={out} 探针="
            f"{'TRADING_DAY' if i_say_trading else 'HOLIDAY'}（日历不同步）")
    else:
        log(f"  [自检] OK: 交易日判断一致 ({out})")


def exception_applies(src, kind, event_ts=None, now=None, data_type=None):
    """判断某条异常是否落在"已知受限源"的豁免范围内

    返回 (是否豁免, 说明文字)。**豁免必须可审计** —— 说明文字会写进日志。

    三类语义：
      paused_by_decision → 始终豁免（人为暂停，需人工复核）
      time_windowed    → 仅在**非交易时段**豁免（交易时段内失败是真故障）
      fixed_watching   → 仅豁免 since 之前的事件（换源后的失败照常告警）
    """
    now = now or datetime.now(timezone(timedelta(hours=8)))
    # 先按 (source, data_type) 精确匹配，再退回按 source 匹配
    # （聚合链的失败记录 source='chain'，真正的数据类型在 data_type 上）
    ex = KNOWN_SOURCE_EXCEPTIONS.get((src, data_type)) if data_type else None
    if ex is None:
        ex = KNOWN_SOURCE_EXCEPTIONS.get(src)
    if not ex:
        return False, ""
    k = ex["kind"]
    if k == "paused_by_decision":
        return True, f"[已知暂停] {ex['reason']}｜复核方式: {ex['review']}"
    if k == "time_windowed":
        # 交易时段内的失败是真故障，不豁免
        if in_trading_hours(now):
            return False, ""
        return True, (f"[已知时段受限] {ex['reason']}｜"
                      f"复核方式: {ex['review']}")
    if k == "fixed_watching":
        since = ex.get("since")
        # event_ts 可能是字符串（collection_log.started_at 是文本时间戳）
        if isinstance(event_ts, str):
            try:
                event_ts = datetime.strptime(event_ts[:19], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone(timedelta(hours=8))).timestamp()
            except ValueError:
                event_ts = None
        if since is not None and event_ts is not None and event_ts < since:
            return True, (f"[已知已修复·观察中] {ex['reason']}｜"
                          f"复核方式: {ex['review']}")
        # 事件在修复之后 → 不豁免（照常告警）
        return False, ""
    return False, ""


def source_window_start(src, data_type, default_since_ts, now=None):
    """确定某个源的统计窗口起点

    - fixed_watching 类（已修复观察中）：用 since（换源生效时刻）作为起点
      → 只统计修复后的数据，历史失败不影响判据
    - 其它：用统一的默认窗口
    """
    ex = KNOWN_SOURCE_EXCEPTIONS.get((src, data_type)) or KNOWN_SOURCE_EXCEPTIONS.get(src)
    if ex and ex["kind"] == "fixed_watching" and ex.get("since"):
        return ex["since"]
    return default_since_ts


def in_trading_hours(now=None):
    """是否处于 A 股交易时段（09:15-15:05，真实交易日）

    行情类数据（price/minute 表、行情源）在收盘后本就不更新 ——
    凌晨或盘后检查"最近成功时间/行数增量"必然误报。
    因此行情类指标**只在交易时段内**才有意义。

    ⚠️ 2026-10-26 修复：原来用 weekday() >= 5 判周末，法定节假日会被当成交易日。
    现在走真实交易日历。
    """
    now = now or datetime.now(timezone(timedelta(hours=8)))
    if not is_trading_day(now):
        return False
    mins = now.hour * 60 + now.minute
    return (9 * 60 + 15) <= mins <= (15 * 60 + 5)


def _trading_days_elapsed(src_date_str, ref_date_str):
    """从 src_date 到 ref_date 经过了多少个交易日（不含 src 当天，含 ref 当天）。

    用途：行情源在周末/节假日不更新是正常的。用墙钟时间算"多久没成功"会
    在**下一个交易日开盘后立刻误报**（周末停更 → 周一 09:20 判定为 STALE）。
    改成按交易日计：只有"漏掉了整个交易日"才算真异常。
    """
    cal = _load_trade_calendar()
    if cal is None:
        return None
    try:
        s = datetime.strptime(src_date_str[:10], "%Y-%m-%d").date()
        r = datetime.strptime(ref_date_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    if s >= r:
        return 0
    n = 0
    d = s + timedelta(days=1)
    while d <= r:
        if d.strftime("%Y-%m-%d") in cal:
            n += 1
        d += timedelta(days=1)
    return n


# 行情源最多允许漏掉几个交易日（>0 表示上一个交易日没跑不算异常）
MARKET_SRC_MAX_MISSED_SESSIONS = 1


def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line)


# 干跑观察期开关（评审纪律：上线前先只记录不推送）
#   PROBE_DRY_RUN=1 → 只记录判定与依据，不推飞书
#   观察满「1 个完整交易日 + 1 个非交易日 + 1 次凌晨非交易时段」且零误报后，
#   由 probe_dryrun_review.py 复核并切换为推送模式。
DRY_RUN = os.environ.get("PROBE_DRY_RUN", "1") not in ("0", "", "false", "False")


def notify(msg):
    """飞书告警（降权到 zsd：hermes 会检查自身目录属主）

    **干跑期物理上发不出去**（两道独立的锁）：
      1. DRY_RUN 为真 → 只记日志，直接返回
      2. FEISHU_TARGET 为空（干跑期不提供该环境变量）→ 直接返回
    即使有人误改了 DRY_RUN，第 2 道锁仍然拦住 —— 不依赖"记得关开关"。
    """
    if DRY_RUN:
        log(f"[DRY-RUN] 本应推送的告警（干跑观察期，未实际发送）:\n{msg}")
        return
    if not FEISHU_TARGET:
        log("[DRY-RUN] 未配置 PROBE_FEISHU_TARGET，推送目标为空，物理上无法发送（干跑观察期）")
        return
    if not os.access(HERMES_BIN, os.X_OK):
        log(f"告警发送失败：{HERMES_BIN} 不可执行")
        return
    try:
        subprocess.run(
            ["/usr/bin/sudo", "-u", "zsd", "env", "HOME=/home/zsd",
             "PATH=/home/zsd/.local/bin:/usr/local/bin:/usr/bin:/bin",
             HERMES_BIN, "send", "-t", FEISHU_TARGET, msg],
            capture_output=True, text=True, timeout=40,
        )
        log("已推送飞书告警")
    except Exception as e:
        log(f"告警发送异常: {e}")


def parse_ts(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:26] if "." in s else s, fmt).replace(tzinfo=timezone(timedelta(hours=8)))
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main():
    problems = []
    now = datetime.now(timezone(timedelta(hours=8)))
    log(f"--- 模式: {'干跑观察期(不推送)' if DRY_RUN else '推送模式'} ---")
    trading_day = is_trading_day(now)
    selfcheck_trading_day(now)
    market_open = in_trading_hours(now)
    log(f"--- 交易日: {'是' if trading_day else '否'} | "
        f"交易时段内: {'是' if market_open else '否（非交易时段放宽行情类指标）'} ---")

    if not os.path.exists(DB_PATH):
        log(f"FAIL: 数据库不存在 {DB_PATH}")
        notify(f"【8084 行情数据新鲜度告警】数据库不存在：{DB_PATH}")
        return 1

    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
        cur = con.cursor()

        # ---- 指标 1：各活跃源最近成功时间 ----
        cutoff = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT source, MAX(started_at) FROM collection_log
            WHERE started_at >= ? AND status='success'
            GROUP BY source
        """, (cutoff,))
        fresh = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("""
            SELECT DISTINCT source FROM collection_log WHERE started_at >= ?
        """, (cutoff,))
        active = [r[0] for r in cur.fetchall() if r[0] != "chain"]

        log(f"--- 活跃源 {len(active)} 个，近24h有成功记录的 {len(fresh)} 个 ---")
        for src in sorted(active):
            last = fresh.get(src)
            ts = parse_ts(last)
            is_market_src = src in MARKET_SOURCES
            if ts is None:
                # 近 24h 内无成功记录
                skip_alert = (not market_open) and is_market_src
                log(f"  [{'WARN(非交易日-忽略)' if skip_alert else 'WARN'}] 源 {src}: 近24h无成功记录")
                if src in CORE_SOURCES and not skip_alert:
                    problems.append(f"核心源 {src} 近 24 小时无任何成功采集")
                continue
            age_min = (now - ts).total_seconds() / 60
            # ⚠️ 2026-10-26 修复：行情源的"多久没成功"按**交易日**算，不按墙钟。
            #    周末/节假日停更是正常的 —— 用墙钟会在下一个交易日开盘瞬间误报
            #    （例：sina 周五 14:50 最后成功 → 周一 09:20 被判 STALE 639 分钟）。
            #    规则：漏掉的交易日数 > MARKET_SRC_MAX_MISSED_SESSIONS 才算异常。
            missed = None
            if is_market_src:
                missed = _trading_days_elapsed(last, now.strftime("%Y-%m-%d"))
            if missed is not None:
                # 有交易日历：用交易日判据
                if missed > MARKET_SRC_MAX_MISSED_SESSIONS:
                    mark = "STALE"
                    if src in CORE_SOURCES:
                        problems.append(
                            f"核心源 {src} 已漏掉 {missed} 个交易日未成功采集"
                            f"（最后一次成功 {last}）")
                else:
                    mark = "OK " if age_min <= ACTIVE_SOURCE_MAX_STALE_MIN else "OK(跨交易日-容忍)"
                log(f"  [{mark}] {src}: 最近成功 {last}（{age_min:.0f} 分钟前，"
                    f"漏掉 {missed} 个交易日）")
                continue
            # 非行情源（新闻/社区等，7×24 运行）：按墙钟
            skip_alert = (not market_open) and is_market_src
            mark = "OK "
            if age_min > ACTIVE_SOURCE_MAX_STALE_MIN:
                mark = "STALE(非交易日-忽略)" if skip_alert else "STALE"
                if src in CORE_SOURCES and not skip_alert:
                    problems.append(
                        f"核心源 {src} 最近成功在 {age_min:.0f} 分钟前"
                        f"（超过 {ACTIVE_SOURCE_MAX_STALE_MIN} 分钟阈值）")
            log(f"  [{mark}] {src}: 最近成功 {last}（{age_min:.0f} 分钟前）")

        # ---- 指标 2：近 1 小时失败率（全局，仅参考） ----
        w = (now - timedelta(minutes=FAIL_RATE_WINDOW_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT status, COUNT(*) FROM collection_log
            WHERE started_at >= ? AND source != 'chain'
            GROUP BY status
        """, (w,))
        counts = {r[0]: r[1] for r in cur.fetchall()}
        total = sum(counts.values())
        fails = counts.get("failed", 0)
        rate = (fails / total) if total else 0
        log(f"--- 近{FAIL_RATE_WINDOW_MIN}分钟(全局参考): 总计 {total} 条, 失败 {fails} 条, 失败率 {rate:.1%} ---")
        if total >= 5 and rate > FAIL_RATE_THRESHOLD:
            problems.append(
                f"近 {FAIL_RATE_WINDOW_MIN} 分钟采集失败率 {rate:.1%}"
                f"（{fails}/{total}），超过 {FAIL_RATE_THRESHOLD:.0%} 阈值")

        # ---- 指标 2b：按 source 分组的失败率（评审盲点 1：全局混合会被稀释）----
        # 全局失败率被成功的大头稀释：eastmoney_em 某小时 3 次全失败，
        # 混进 107 条里就变成 0.9%，完全看不见。必须按源单独算。
        # 用 24h 窗口：低频源每小时仅 1~4 条，1h 窗口样本永远不够。
        ws = (now - timedelta(minutes=PER_SOURCE_WINDOW_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT source,
                   SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS f,
                   COUNT(*) AS t
            FROM collection_log
            WHERE started_at >= ? AND source != 'chain'
            GROUP BY source
        """, (ws,))
        log(f"--- 近{PER_SOURCE_WINDOW_MIN}分钟 按源分组 ---")
        for src, f_cnt, t_cnt in cur.fetchall():
            f_cnt = f_cnt or 0
            t_cnt = t_cnt or 0
            if t_cnt < PER_SOURCE_MIN_SAMPLES:
                log(f"  [样本不足] {src}: {f_cnt}/{t_cnt}（样本<{PER_SOURCE_MIN_SAMPLES}，不判）")
                continue
            succ_rate = 1 - (f_cnt / t_cnt)
            # 已知受限源：按类别判断是否豁免（时段受限在交易时段内不豁免）
            exempt, note = exception_applies(src, "per_source_failure_rate", now=now)
            if exempt:
                log(f"  {note} —— 本窗口 {f_cnt}/{t_cnt} 失败（豁免，不计告警）")
                continue
            if src in KNOWN_SOURCE_EXCEPTIONS:
                log(f"  [已知受限源·本次不豁免] {src}: {f_cnt}/{t_cnt} 失败 "
                    f"({KNOWN_SOURCE_EXCEPTIONS[src]['kind']})")
            if succ_rate < PER_SOURCE_SUCCESS_RATE_THRESHOLD:
                log(f"  [FAIL] {src}: 成功率 {succ_rate:.1%}（{f_cnt}/{t_cnt} 失败）")
                problems.append(
                    f"源 {src} 近 {PER_SOURCE_WINDOW_MIN} 分钟成功率 {succ_rate:.1%}"
                    f"（{f_cnt}/{t_cnt} 失败），低于 {PER_SOURCE_SUCCESS_RATE_THRESHOLD:.0%} 阈值")
            else:
                log(f"  [OK] {src}: 成功率 {succ_rate:.1%}（{f_cnt}/{t_cnt} 失败）")

        # ---- 指标 5：重复错误率（评审盲点 3：静默失败最有效的特征签名）----
        # 同一 (source, data_type) 的失败 error_message 高度重复
        # → 接口**稳定返回无效内容**，不是网络抖动。
        # 实测：estimate_nav 504 次一字不差、eastmoney_em 31/31、xueqiu 16/16
        wr = (now - timedelta(minutes=REPEAT_ERROR_WINDOW_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        # ⚠️ 这里**包含 chain**：chain 是降级链，chain/estimate_nav 重复失败
        #    表示"该 data_type 的整条链路全挂"，是最重要的信号之一。
        cur.execute("""
            SELECT source, data_type, error_message, COUNT(*) AS n,
                   MIN(started_at) AS first_at, MAX(started_at) AS last_at
            FROM collection_log
            WHERE started_at >= ? AND status='failed'
              AND error_message IS NOT NULL AND error_message != ''
            GROUP BY source, data_type, error_message
            HAVING n >= ?
            ORDER BY n DESC
        """, (wr, REPEAT_ERROR_MIN_COUNT))
        repeat_rows = cur.fetchall()
        if repeat_rows:
            log(f"--- 重复错误（近{REPEAT_ERROR_WINDOW_MIN}分钟，同一错误出现>={REPEAT_ERROR_MIN_COUNT}次）---")
            for src, dt, err_msg, n, first_at, last_at in repeat_rows:
                cur.execute("""
                    SELECT COUNT(*) FROM collection_log
                    WHERE started_at >= ? AND status='failed' AND source=? AND data_type=?
                """, (wr, src, dt))
                tot_f = cur.fetchone()[0] or 0
                ratio = (n / tot_f) if tot_f else 0
                if ratio < REPEAT_ERROR_MIN_RATIO:
                    continue
                # fixed_watching（已修复）：只统计修复生效之后的失败，
                # 修复前的历史失败不参与判据（随窗口自然消失）
                ex = KNOWN_SOURCE_EXCEPTIONS.get((src, dt)) or KNOWN_SOURCE_EXCEPTIONS.get(src)
                if ex and ex["kind"] == "fixed_watching" and ex.get("since"):
                    since_ts = ex["since"]
                    cur.execute("""
                        SELECT COUNT(*) FROM collection_log
                        WHERE started_at >= ? AND status='failed'
                          AND source=? AND data_type=?
                          AND error_message IS NOT NULL AND error_message != ''
                    """, (since_ts, src, dt))
                    tot_after = cur.fetchone()[0] or 0
                    cur.execute("""
                        SELECT COUNT(*) FROM collection_log
                        WHERE started_at >= ? AND status='failed'
                          AND source=? AND data_type=?
                          AND error_message IS NOT NULL AND error_message != ''
                    """, (since_ts, src, dt))
                    n_after = cur.fetchone()[0] or 0
                    if n_after < REPEAT_ERROR_MIN_COUNT:
                        log(f"  [已修复·观察中] {src}/{dt}: 换源后同类错误 {n_after} 次"
                            f"（<{REPEAT_ERROR_MIN_COUNT}，历史 {n} 次已随窗口滚出）→ 不告警")
                        continue
                    n, tot_f = n_after, tot_after
                    ratio = (n / tot_f) if tot_f else 0
                    if ratio < REPEAT_ERROR_MIN_RATIO:
                        continue
                # 已知受限源：按类别判断（已修复只豁免换源前的事件）
                exempt, note = exception_applies(src, "repeat_error_rate",
                                                event_ts=wr, now=now, data_type=dt)
                if exempt:
                    log(f"  {note} —— {src}/{dt} 重复 {n}/{tot_f} 次（豁免，不计告警）")
                    continue
                if src in KNOWN_SOURCE_EXCEPTIONS:
                    log(f"  [已知受限源·本次不豁免] {src}/{dt}: 重复 {n}/{tot_f} 次 "
                        f"({KNOWN_SOURCE_EXCEPTIONS[src]['kind']})")
                short = (err_msg or "")[:80]
                log(f"  [REPEAT] {src}/{dt}: 同一错误 {n}/{tot_f} 次（{ratio:.0%} 重复）")
                problems.append(
                    f"源 {src}/{dt} 近 {REPEAT_ERROR_WINDOW_MIN} 分钟出现**高度重复的同一错误** "
                    f"{n}/{tot_f} 次（{ratio:.0%}）→ 接口稳定返回无效内容，非网络抖动。"
                    f"错误摘要: {short}")
        else:
            log(f"--- 重复错误: 无（近{REPEAT_ERROR_WINDOW_MIN}分钟无同一错误出现>={REPEAT_ERROR_MIN_COUNT}次）---")

        # ---- 指标 3：核心表行数增量 ----
        # ⚠️ news_data 不用行数判存活（UNIQUE 去重 → 采集成功但行数不涨是常态），
        #    改由指标 4（新闻采集器成功次数）负责。行数仅作参考打印。
        for tbl in CORE_TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                now_n = cur.fetchone()[0]
            except sqlite3.Error as e:
                log(f"  [WARN] 表 {tbl} 不可读: {e}")
                continue
            # ⚠️ 时间基准：表的 updated_at 用 SQLite DEFAULT CURRENT_TIMESTAMP → **UTC**；
            #    而 collection_log.started_at 是应用写入的**本地时间**。两者差 8 小时，
            #    混用会导致永远查不到增量。表一律用 SQLite 内部函数算窗口，保证同基准。
            cur.execute(f"""
                SELECT COUNT(*) FROM {tbl}
                WHERE updated_at >= datetime('now', ?)
            """, (f"-{FAIL_RATE_WINDOW_MIN} minutes",))
            grew = cur.fetchone()[0]
            if tbl in NEWS_TABLES:
                # 新闻表：只打印，不告警
                log(f"  [INFO] {tbl}: 总行数 {now_n}, 近{FAIL_RATE_WINDOW_MIN}分钟新增 {grew}"
                    f"（不作为判据，见指标 4）")
                continue
            # 非交易日的行情表不增长是正常的（周末无新行情）
            skip_alert = (not market_open) and (tbl in MARKET_TABLES)
            log(f"  [{'OK ' if grew >= MIN_ROW_GROWTH else ('NO-GROW(非交易日-忽略)' if skip_alert else 'NO-GROW')}] {tbl}: "
                f"总行数 {now_n}, 近{FAIL_RATE_WINDOW_MIN}分钟新增 {grew}")
            if grew < MIN_ROW_GROWTH and not skip_alert:
                problems.append(
                    f"表 {tbl} 近 {FAIL_RATE_WINDOW_MIN} 分钟新增 {grew} 行（低于 {MIN_ROW_GROWTH}），数据可能已停止写入")

        # ---- 指标 4：新闻类采集器成功次数（news_data 的真实存活判据）----
        nw = (now - timedelta(minutes=NEWS_COLLECTOR_WINDOW_MIN)).strftime("%Y-%m-%d %H:%M:%S")
        marks = ",".join("?" * len(NEWS_SOURCES))
        cur.execute(f"""
            SELECT source, COUNT(*), MAX(started_at) FROM collection_log
            WHERE started_at >= ? AND status='success' AND source IN ({marks})
            GROUP BY source
        """, (nw, *sorted(NEWS_SOURCES)))
        news_stats = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        news_total = sum(v[0] for v in news_stats.values())
        log(f"--- 近{NEWS_COLLECTOR_WINDOW_MIN}分钟新闻类采集成功 {news_total} 次 ---")
        for src in sorted(NEWS_SOURCES):
            cnt, last = news_stats.get(src, (0, None))
            log(f"  [{'OK ' if cnt >= NEWS_COLLECTOR_MIN_SUCCESS else 'NO-SUCCESS'}] {src}: "
                f"成功 {cnt} 次, 最近 {last or '无'}")
        if news_total < NEWS_COLLECTOR_MIN_SUCCESS:
            problems.append(
                f"新闻类采集器近 {NEWS_COLLECTOR_WINDOW_MIN} 分钟成功 0 次"
                f"（rss/zhihu/news_aggregator/sina 全静默），新闻采集链路可能已断")
        elif news_total < 3:
            # 有成功但极少：只是提示，不进 problems
            log(f"  [INFO] 新闻采集成功次数偏低（{news_total}），可能是安静时段")

        con.close()
    except sqlite3.Error as e:
        log(f"FAIL: 数据库查询错误 {e}")
        notify(f"【8084 行情数据新鲜度告警】数据库查询失败：{e}")
        return 1

    if problems:
        log(f"=== 检测到 {len(problems)} 个问题 ===")
        for p in problems:
            log(f"  - {p}")
        notify("【8084 行情数据新鲜度告警】\n" +
               "\n".join(f"· {p}" for p in problems) +
               f"\n采集周期约 5 分钟，非交易日部分源会失败属正常。详见 {LOG_PATH}")
        return 1

    log("=== 全部指标正常 ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
