#!/usr/bin/env python3
"""持仓/订阅对账巡检（2026-11-03，011612 静默缺口事故的巡检卡）。

口径（Chief 已确认）：
  持仓页展示的每个标的（与 /api/v1/catalog/holdings 同源）必须
  ① 有 enabled 订阅覆盖它需要的 data_type；
  ② 该订阅 last_collected 在 48 小时内。
违反 → 逐条列出；退出码 1 = 有缺口（供 timer 告警接续）。

覆盖规则（data_type 需求按 kind）：
  stock → price
  fund  → nav + estimate_nav
板块/榜单订阅（main_fund_rank 等）不在持仓口径内，另由
market8084_watchdog.sh 的 _collect_alert 连败告警覆盖。

用法：
  python3 check_holdings_subs.py             # 正常输出
  python3 check_holdings_subs.py --quiet     # 只在有缺口时输出（timer 用）
退出码：0 = 全对齐；1 = 有缺口；2 = 读取失败。
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from datetime import datetime, timedelta

DB = "file:/home/zsd/financial-data-service/market_service/data/market.db?mode=ro"
FUND_RETENTION_DAYS = 30      # 与 api.py 同口径：陈年残留不算持仓
STALE_HOURS = 48              # 订阅采集新鲜度阈值（Chief 确认）
# 市场数据新鲜度（Chief 2026-11-03 批的第二维度，hk09988"假断更"事故引出）：
# 订阅在跑且采集成功 ≠ 数据新鲜。判市场数据年龄必须用归一化后的 trade_time，
# 裸 MAX(trade_time) 被混合格式骗（字典序 '2026...' > '2026-...'，恒返回 compact
# 旧行 → 看着像断更 4 天，实际 23:16 刚写入；程序员 2026-11-03 实测定位）。
NORM_TRADE_TIME = (
    "CASE WHEN trade_time LIKE '____-__-__%'"
    " THEN replace(substr(trade_time,1,19),'T',' ')"
    " ELSE substr(trade_time,1,4)||'-'||substr(trade_time,5,2)||'-'||substr(trade_time,7,2)"
    "      ||' '||substr(trade_time,10,8) END")
STALE_DAYS_STOCK = 3          # price 数据落后超过 3 个自然日 → WARN（hk 双雄快照路径恒新鲜，A 股收盘后隔日属正常）
NEEDED = {"stock": ["price"], "fund": ["nav", "estimate_nav"]}


def days_since(date_str: str) -> int | None:
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return (datetime.now() - d).days
    except ValueError:
        return None


def holdings(con) -> list[tuple[str, str]]:
    """与 api._holdings_entries 同源：股票=price_data 出现过；基金=nav 近 30 天。"""
    out: list[tuple[str, str]] = []
    for (sym,) in con.execute(
            "SELECT DISTINCT symbol FROM price_data WHERE symbol IS NOT NULL AND symbol != ''"):
        out.append((sym, "stock"))
    for sym, last in con.execute(
            "SELECT symbol, MAX(nav_date) FROM nav_data"
            " WHERE symbol IS NOT NULL AND symbol != '' GROUP BY symbol"):
        d = days_since(last)
        if d is None or d > FUND_RETENTION_DAYS:
            continue
        out.append((sym, "fund"))
    return out


def main() -> int:
    quiet = "--quiet" in sys.argv
    try:
        con = sqlite3.connect(DB, uri=True)
        con.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"[对账巡检] 库连接失败: {e}", file=sys.stderr)
        return 2
    subs = [dict(r) for r in con.execute(
        # ⚠️ 必须带 id：下面的新鲜度判定要用 subscription_id 查 collection_log。
        # 2026-11-03 事故：这里漏了 id → sub.get("id") 恒为 None → 所有订阅被
        # continue 跳过 → fresh 恒空 → 每个标的都报缺口（生产 21 条假警）。
        # 当时误判成 "mode=ro 读不到 WAL"，实测两种连接结果逐字一致，与 WAL 无关。
        "SELECT id, name, symbol, data_types, frequency_min, enabled, last_collected"
        " FROM subscriptions WHERE enabled = 1")]
    gaps: list[str] = []
    for sym, kind in sorted(holdings(con)):
        need = NEEDED[kind]
        for dt in need:
            hits = [s for s in subs
                    if s["symbol"] in (sym, "*")
                    and dt in json.loads(s["data_types"] or "[]")]
            if not hits:
                gaps.append(f"{sym}({kind}) 缺 {dt} 的 enabled 订阅")
                continue
            # 盲区修复（批评家）：scheduler 失败时也更新 last_collected，
            # 所以「订阅在跑但一直失败」检不出来。改查 collection_log 里
            # 该 (symbol, data_type) 最近一次 status='success' 的时间。
            fresh = []
            last_ok_seen = None
            for sub in hits:
                sub_id = sub.get("id")
                ok_val = None
                if sub_id is not None:
                    r = con.execute(
                        "SELECT MAX(started_at) FROM collection_log"
                        " WHERE subscription_id = ? AND status = 'success'",
                        (sub_id,)).fetchone()
                    ok_val = r[0] if r else None
                if ok_val is None:
                    # 兜底（Chief 2026-11-03 批准）：collection_log 里 subscription_id
                    # 为 NULL 的记录（手工触发/历史数据）关联不上订阅 id，只按 id 匹配
                    # 会让该标的永远判"无成功记录" → 假警换个形式回来。
                    # 按 id 查不到时退回按 symbol + data_type 匹配。
                    r = con.execute(
                        "SELECT MAX(started_at) FROM collection_log"
                        " WHERE status = 'success' AND data_type = ?"
                        "   AND (symbol = ? OR symbol = '*')",
                        (dt, sym)).fetchone()
                    ok_val = r[0] if r else None
                if ok_val:
                    last_ok = datetime.fromisoformat(str(ok_val)[:19])
                    if last_ok_seen is None or last_ok > last_ok_seen:
                        last_ok_seen = last_ok
                    if datetime.now() - last_ok < timedelta(hours=STALE_HOURS):
                        fresh.append(sub)
            if not fresh:
                # ⚠️ 文案必须报「最后一次**成功**采集」而不是 last_collected：
                # scheduler 失败时也会更新 last_collected，用它会让告警写着
                # "最后 2026-11-03 02:42"（刚刚）却判"超过 48h 未采集"，
                # 读告警的人会被彻底带偏（2026-11-03 实测踩到）。
                if last_ok_seen is not None:
                    tail = f"最后成功 {last_ok_seen:%Y-%m-%d %H:%M}"
                else:
                    tail = "collection_log 里无任何成功记录"
                lc = max(str(s["last_collected"] or "") for s in hits)[:16]
                gaps.append(f"{sym}({kind}) {dt} 订阅存在但超过 {STALE_HOURS}h 未成功采集"
                            f"（{tail}；last_collected={lc}）")
                continue
            # 第二维度（Chief 2026-11-03 批）：采集在跑，但市场数据本身陈旧。
            # 只查 stock/price：基金净值天然隔日出，不适用盘中新鲜度。
            if dt == "price" and kind == "stock":
                r = con.execute(
                    f"SELECT MAX({NORM_TRADE_TIME}) FROM price_data WHERE symbol = ?",
                    (sym,)).fetchone()
                last_px = r[0] if r else None
                age = days_since(last_px) if last_px else None
                # days_since 只取前 10 位日期，归一化后的 'YYYY-MM-DD HH:MM:SS' 兼容
                if age is not None and age > STALE_DAYS_STOCK:
                    gaps.append(f"{sym}(stock) price 采集成功但数据陈旧：最新行情 {last_px}"
                                f"（{age} 天前，阈值 {STALE_DAYS_STOCK} 天）")
    n_hold = len(set(holdings(con)))
    if gaps and not quiet:
        print(f"[对账巡检] 持仓 {n_hold} 个标的，发现 {len(gaps)} 个缺口：")
        for g in gaps:
            print("  ✗ " + g)
    elif gaps:
        print(f"[对账巡检] {len(gaps)} 个缺口（quiet 模式详见 --help）")
        for g in gaps:
            print("  ✗ " + g)
    else:
        if not quiet:
            print(f"[对账巡检] 持仓 {n_hold} 个标的，订阅全覆盖且 48h 内有采集 ✓")
    return 1 if gaps else 0


if __name__ == "__main__":
    sys.exit(main())
