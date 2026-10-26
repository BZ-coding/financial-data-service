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
HERMES_BIN = "/home/zsd/.local/bin/hermes"
FEISHU_TARGET = "feishu:oc_90857aaa9c3a4533fdc12c008ca14d00"

# —— 阈值（偏宽松，优先不误报）——
ACTIVE_SOURCE_MAX_STALE_MIN = 180   # 活跃源最近成功超过 3 小时 → 异常
FAIL_RATE_WINDOW_MIN = 60            # 失败率统计窗口 1 小时
FAIL_RATE_THRESHOLD = 0.85          # 失败率 > 85% → 异常
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


def notify(msg):
    """飞书告警（降权到 zsd：hermes 会检查自身目录属主）"""
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
    trading_day = is_trading_day(now)
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

        # ---- 指标 2：近 1 小时失败率 ----
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
        log(f"--- 近{FAIL_RATE_WINDOW_MIN}分钟: 总计 {total} 条, 失败 {fails} 条, 失败率 {rate:.1%} ---")
        if total >= 5 and rate > FAIL_RATE_THRESHOLD:
            problems.append(
                f"近 {FAIL_RATE_WINDOW_MIN} 分钟采集失败率 {rate:.1%}"
                f"（{fails}/{total}），超过 {FAIL_RATE_THRESHOLD:.0%} 阈值")

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
