#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""stock_basic_data 回填：补北交所（920 段）与可得的股转代码（批评家 A1）。

背景（2026-11-03）：
  · /api/v1/sectors 的领涨股反查此前用 fund_flow_data，命中仅 143/495=28%；
    切到 stock_basic_data 后历史命中 1431/1472=97%，剩余 41 个未命中里
    绝大多数是**北交所股票**（锦波生物/三元基因/芭薇股份…）——因为表里
    只有东财回填的 5563 只沪深 A 股（60/00/30/68），没有任何北交所代码。
  · 用 akshare 补：stock_info_a_code_name()（沪深 + 北交所 920 段，5578 行）
    + stock_info_bj_name_code()（北交所全量 354 行，920 段）。

代码段说明（不要想当然）：
  · 北交所 2024-2025 起陆续切换到 **920 统一代码段**；akshare 1.18.51 的两个
    列表函数返回的北交所代码**全部是 920xxx**，没有 83/87/43 老代码。
  · 老三板/新三板（43xxxx）在 akshare 1.18.51 里**没有可靠的列表函数**，
    东财行业/概念板块资金流里也不含新三板标的 → 本脚本不虚构数据，
    43 段的覆盖留待上游有列表来源时再补（已在交付说明里注明）。

schema 变更（Chief 已拍板，本脚本幂等执行）：
  · DROP INDEX IF EXISTS ux_stock_basic_symbol
    —— 它是 symbol 单列唯一索引，会让 UNIQUE(symbol, source) 形同虚设：
       将来任何第二个 source 的同名 symbol 都插不进去（报 UNIQUE 失败）。
       保留表级 UNIQUE(symbol, source) 才是正确语义。
  · CREATE INDEX IF NOT EXISTS idx_stock_basic_name —— 领涨股名反查按 name 查，
    回填后 ~5900 行，无索引每次请求全表扫。

用法：
    python3 backfill_stock_basic.py            # 干跑：只打印将发生的变更
    python3 backfill_stock_basic.py --apply    # 实际写入（含 schema 变更）
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TQDM_DISABLE", "1")   # akshare 进度条会污染日志（旧坑）

import akshare as ak

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill.stock_basic")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "market.db")
SOURCE = "akshare"


def fetch_lists() -> list[tuple[str, str]]:
    """拉取 (symbol, name) 合集。只收 code+name，不虚构其它字段。"""
    out: dict[str, str] = {}

    df = ak.stock_info_a_code_name()          # 沪深 A + 北交所 920 段
    for _, r in df.iterrows():
        sym, name = str(r["code"]).strip(), str(r["name"]).strip()
        if sym and name:
            out.setdefault(sym, name)
    log.info("stock_info_a_code_name: %d 行", len(df))

    bj = ak.stock_info_bj_name_code()         # 北交所全量（920 段）
    for _, r in bj.iterrows():
        sym, name = str(r["证券代码"]).strip(), str(r["证券简称"]).strip()
        if sym and name:
            out.setdefault(sym, name)
    log.info("stock_info_bj_name_code: %d 行", len(bj))

    return sorted(out.items())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="实际写入（默认干跑）")
    args = ap.parse_args()

    rows = fetch_lists()
    log.info("上游合计 %d 只（沪深 %d + 北交所 920 段 %d，含重复合并）",
             len(rows),
             sum(1 for s, _ in rows if s.startswith(("60", "68", "00", "30"))),
             sum(1 for s, _ in rows if s.startswith("92")))

    import sqlite3
    con = sqlite3.connect(DB_PATH, timeout=30)
    try:
        existing = {s: (n, src) for s, n, src in con.execute(
            "SELECT symbol, name, source FROM stock_basic_data")}
        new = [(s, n) for s, n in rows if s not in existing]
        # 已存在但名字不同（改名/简称差异）也更新本 source 的行；不动别家 source
        renamed = [(s, n) for s, n in rows
                   if s in existing and existing[s][1] != n and existing[s][1] != SOURCE]
        log.info("对比库内 %d 行：将新增 %d，与本 source 不同名的存量行 %d（不覆盖他 source）",
                 len(existing), len(new), len(renamed))
        for s, n in new[:12]:
            log.info("  新增示例: %s %s", s, n)
        if len(new) > 12:
            log.info("  …… 其余 %d 条略", len(new) - 12)

        if not args.apply:
            log.info("干跑结束（--apply 写入）")
            return 0

        cur = con.cursor()
        # ① schema 变更（幂等；DROP INDEX 必须在写入前，否则同 symbol 第二个 source 插不进）
        cur.execute("DROP INDEX IF EXISTS ux_stock_basic_symbol")
        log.info("已 DROP INDEX ux_stock_basic_symbol（symbol 单列唯一，与 UNIQUE(symbol,source) 冲突）")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_basic_name ON stock_basic_data(name)")
        log.info("已 CREATE INDEX idx_stock_basic_name（领涨股名反查走索引）")

        # ② upsert（只增改，不删任何行）
        # 库内 CURRENT_TIMESTAMP 默认是 UTC，显式写入也用 UTC（时区口径约定）
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        cur.executemany(
            "INSERT INTO stock_basic_data (source, symbol, name, ingested_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(symbol, source) DO UPDATE SET"
            " name = excluded.name, updated_at = excluded.updated_at",
            [(SOURCE, s, n, now, now) for s, n in rows])
        con.commit()

        after = con.execute(
            "SELECT source, COUNT(*) FROM stock_basic_data GROUP BY source").fetchall()
        log.info("回填完成。按 source 分布: %s", after)
        return 0
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
