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
        "SELECT name, symbol, data_types, frequency_min, enabled, last_collected"
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
            for sub in hits:
                sub_id = sub.get("id")
                if sub_id is None:
                    continue
                ok_row = con.execute(
                    "SELECT MAX(started_at) FROM collection_log"
                    " WHERE subscription_id = ? AND status = 'success'",
                    (sub_id,)).fetchone()
                if ok_row and ok_row[0]:
                    last_ok = datetime.fromisoformat(str(ok_row[0])[:19])
                    if datetime.now() - last_ok < timedelta(hours=STALE_HOURS):
                        fresh.append(sub)
            if not fresh:
                last = max(str(s["last_collected"]) for s in hits)
                gaps.append(f"{sym}({kind}) {dt} 订阅存在但超过 {STALE_HOURS}h 未采集"
                            f"（最后 {last[:16]}）")
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
