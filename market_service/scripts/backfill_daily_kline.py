#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""资金流榜个股日线回填（东财 push2his kline → daily_data）。

背景（2026-11-03 用户需求）：
  资金流榜 300 只个股在 price_data 里 0 条日线 → /api/v1/detail 的技术指标区
  一直空着（只有资金流快照）。本脚本把最新一批 main_fund_rank 的 symbol 集合
  拉 1 年日 K（前复权），落 daily_data；`_indicators_of` 的 daily 回退分支
  （api.py）随后即可为这些个股算 MA/MACD/RSI/20日高低。

数据源（2026-11-03 勘查实测）：
  · 东财 push2his kline 直连 + **强制 IPv4**（urllib3 HAS_IPV6=False，运维手
    2026-11-03 定位：push2/push2delay/push2his 的 AAAA 全指向 Azure 边缘，
    IPv6 全断、IPv4 稳定 200）
  · akshare 的 stock_zh_a_hist 不可用——它内部也请求 push2his，同样撞断 IPv6
    （实测 4/4 ConnectionError）
  · 限流实测：15 连发（0.15s 间隔）15/15 成功、平均 0.22s；本脚本再保守
    （0.3s/只 + 连续 3 次失败睡 30s），300 只 ≈ 4-5 分钟

字段映射（实测确认，open/close/high/low 顺序不能凭直觉）：
  klines 行 = "date,open,close,high,low,volume,amount,振幅"（f51..f57）
  → trade_date=date, open=f52, close=f53, high=f54, low=f55, vol=f56, amount=f57

用法：
  python3 backfill_daily_kline.py --limit 3            # 试跑 3 只（写库，验证链路）
  python3 backfill_daily_kline.py                      # 全量 300 只，只拉不写
  python3 backfill_daily_kline.py --apply              # 全量回填（正式）
  python3 backfill_daily_kline.py --all-history        # 用历史全部 symbol（而非最新批）

幂等：daily_data UNIQUE(source, symbol, trade_date) + INSERT OR REPLACE，
重跑只会刷新同名行，不产生重复。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TQDM_DISABLE", "1")

# ⚠️ 必须在 import requests 之前关 IPv6（push2his AAAA 全断，见模块头）
import urllib3.util.connection as _u3conn
_u3conn.HAS_IPV6 = False
import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill.daily_kline")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "market.db")
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
SLEEP_SECONDS = 0.3          # 每只间隔（实测 0.22s/请求，留余量）
FAILURE_BACKOFF = 30         # 连续失败退避
FAILURE_BACKOFF_AFTER = 3
SOURCE = "eastmoney"
HIST_DAYS = 400              # 拉 ~1 年（交易日 243，留节假日余量）


def fetch_symbols(db_path: str, all_history: bool = False) -> list[str]:
    """资金流榜的 symbol 集合：默认最新批（与 /api/v1/sectors 同一 5 分钟窗口），
    --all-history 用历史出现过的全部 symbol。"""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if all_history:
            rows = con.execute(
                "SELECT DISTINCT symbol FROM fund_flow_data"
                " WHERE flow_type='main_fund_rank' AND symbol != ''").fetchall()
        else:
            latest = con.execute(
                "SELECT max(collected_at) FROM fund_flow_data"
                " WHERE flow_type='main_fund_rank'").fetchone()[0]
            if not latest:
                return []
            win = (datetime.strptime(latest[:19], "%Y-%m-%d %H:%M:%S")
                   - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
            rows = con.execute(
                "SELECT DISTINCT symbol FROM fund_flow_data"
                " WHERE flow_type='main_fund_rank' AND collected_at >= ?"
                " AND symbol != ''", (win,)).fetchall()
        return sorted({r[0].strip() for r in rows if r[0] and r[0].strip()})
    finally:
        con.close()


def fetch_kline(symbol: str) -> list[dict]:
    """拉一只股票的日 K（前复权）。返回 daily_data 行参列表；失败返回 []。"""
    # 东财 secid：沪 1.xxxx，深/创业板/科创板/北交所 0.xxxx
    secid = ("1." if symbol.startswith("6") else "0.") + symbol
    beg = (datetime.now() - timedelta(days=HIST_DAYS)).strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")
    params = {
        "secid": secid,
        "fields1": "f1",
        "fields2": "f51,f52,f53,f54,f55,f56,f57",
        "klt": "101",        # 日线
        "fqt": "1",          # 前复权（跨除权连续，指标需要）
        "beg": beg, "end": end,
    }
    r = requests.get(KLINE_URL, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = (r.json() or {}).get("data") or {}
    out = []
    for ln in data.get("klines") or []:
        p = ln.split(",")
        if len(p) < 7:
            continue
        try:
            out.append({
                "trade_date": p[0],
                "open": float(p[1]), "close": float(p[2]),
                "high": float(p[3]), "low": float(p[4]),
                "vol": float(p[5] or 0), "amount": float(p[6] or 0),
            })
        except (ValueError, IndexError):
            continue
    return out


def write_rows(con: sqlite3.Connection, symbol: str, rows: list[dict]) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con.executemany(
        "INSERT OR REPLACE INTO daily_data"
        " (source, symbol, trade_date, open, high, low, close, vol, amount,"
        "  ingested_at, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(SOURCE, symbol, r["trade_date"], r["open"], r["high"], r["low"],
          r["close"], r["vol"], r["amount"], now, now) for r in rows])
    con.commit()
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="实际写库（默认只拉不写）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（测试用）")
    ap.add_argument("--all-history", action="store_true",
                    help="用历史全部 symbol（默认只最新一批）")
    ap.add_argument("--db", default=DB_PATH, help="目标库路径（测试打副本用）")
    args = ap.parse_args()

    db_path = args.db
    is_prod_db = os.path.abspath(db_path) == os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, "data", "market.db"))
    if is_prod_db and not args.apply:
        log.info("干跑模式：拉取但不写库（--apply 写入）")

    symbols = fetch_symbols(db_path, args.all_history)
    if args.limit:
        symbols = symbols[:args.limit]
    log.info("待回填 symbol 数: %d", len(symbols))
    if not symbols:
        log.error("没有取到 symbol——检查 main_fund_rank 最新批是否在跑")
        return 1

    # ⚠️ 用 args.db（2026-11-03 自踩：全局 DB_PATH 忘了替换 → --db 指向副本、
    #    实际连的是生产库，"打副本测试"变成打生产。参数必须贯穿到底。）
    con = sqlite3.connect(db_path, timeout=30)
    ok = fail = inserted = 0
    consec_fail = 0
    failed: list[str] = []
    t0 = time.time()
    try:
        for i, sym in enumerate(symbols, 1):
            try:
                rows = fetch_kline(sym)
            except Exception as e:
                fail += 1
                consec_fail += 1
                failed.append(sym)
                log.warning("[%d/%d] %s 拉取失败 %s: %s",
                            i, len(symbols), sym, type(e).__name__, e)
                if consec_fail >= FAILURE_BACKOFF_AFTER:
                    log.warning("连续失败 %d 次 → 退避 %ds（限流/断网保护）",
                                consec_fail, FAILURE_BACKOFF)
                    time.sleep(FAILURE_BACKOFF)
                    consec_fail = 0
            else:
                consec_fail = 0
                ok += 1
                if rows and args.apply:
                    inserted += write_rows(con, sym, rows)
                if i % 25 == 0 or i == len(symbols):
                    log.info("[%d/%d] 进度 ok=%d fail=%d rows=%d (%.0fs)",
                             i, len(symbols), ok, fail, inserted,
                             time.time() - t0)
            time.sleep(SLEEP_SECONDS)
    finally:
        con.close()

    log.info("完成：ok=%d fail=%d 写入行数=%d 耗时=%.0fs 失败清单=%s",
             ok, fail, inserted, time.time() - t0, failed[:20])
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
