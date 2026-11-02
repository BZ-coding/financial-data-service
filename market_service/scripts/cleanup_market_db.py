#!/usr/bin/env python3
"""market.db 数据清理工具（默认干跑）

⚠️ 2026-10-26 新增。此前 `Database.cleanup_old_data()` 有定义但**零调用方**，
   而且它第一条 DELETE 就抛异常（news_data 没有 timestamp 列），就算调用也必然失败
   → collection_log 435MB 只增不减。

用法：
    # 1) 干跑：只看要删什么、删多少（不改任何数据）
    ./venv/bin/python3.14 scripts/cleanup_market_db.py

    # 2) 真删（会先自动备份 collection_log 与各清理表）
    ./venv/bin/python3.14 scripts/cleanup_market_db.py --apply

    # 3) 自定义保留期
    ./venv/bin/python3.14 scripts/cleanup_market_db.py --set log_days=30 --set price_days=90

⚠️ 真删前会自动：
   1) 备份 collection_log 到 data/backups/（JSONL 导出，保留全部行）
   2) 打印将要删除的清单，等确认
   3) 删完提示需要 VACUUM 才能真正回收磁盘空间（VACUUM 需停服务）
"""
import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 项目根 = market_service/ 的上一级
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
# market_service/ 目录本身也要在 sys.path 上：
#   2026-10-30 根治后，`market_service/database.py` 与 `market_service/api.py`
#   已改成**包内相对导入优先**（`from .safe_ops import safe_delete`，
#   平铺导入只作回退）→ 主路径不再依赖这一行。
#   但**回退分支仍需要它**：平铺导入（`from safe_ops import ...`）只有在
#   market_service/ 本身在 sys.path 上时才解析得到（safe_ops 是兄弟模块）。
#   → 两条路径都留着：包内相对导入走主路，平铺回退走保底。
#   无论从哪个目录、用哪种方式直接跑本脚本，导入都能解析 → 不再依赖
#   systemd unit 的 Environment=PYTHONPATH（那只是止血期的过渡写法）。
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from market_service.database import Database, TZ  # noqa: E402

DEFAULT_RETENTION = {
    "log_days": 30, "price_days": 90, "news_days": 30, "minute_days": 60,
    "estimate_nav_days": 30, "aggregator_days": 14, "mmx_search_days": 90,
    # 2026-11-03（项 ⑥）：fund_flow_data 原来不在保留列表——top_n 扩容后
    # 300 个股 + 992 板块 × ~6 批/日 ≈ 7750 行/天 ≈ 550MB/年，无限增长。
    # 90 天与前值分析窗口匹配；更早的批次只对历史回测有用。
    "fund_flow_days": 90,
}


def db_size_mb(path):
    return os.path.getsize(path) / 1024 / 1024


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="真正执行删除（默认只干跑）")
    ap.add_argument("--set", action="append", default=[], metavar="KEY=DAYS")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    db_path = Path(args.db) if args.db else _PROJECT_ROOT / "market_service" / "data" / "market.db"
    ret = dict(DEFAULT_RETENTION)
    for s in args.set:
        k, _, v = s.partition("=")
        ret[k] = int(v)

    db = Database(str(db_path))
    before = db_size_mb(db_path)

    print(f"=== market.db 清理工具 {'（真删）' if args.apply else '（干跑 DRY-RUN，不改数据）'} ===")
    print(f"库路径: {db_path}")
    print(f"当前大小: {before:.1f} MB")
    print(f"保留期配置: {ret}\n")

    result = db.cleanup_old_data(ret, dry_run=True)  # 【只统计，不删除】
    print(f"{'表':24s} {'保留':>5s} {'截止':20s} {'可删':>12s}")
    print("-" * 68)
    total = 0
    for t, info in result["cutoffs"].items():
        n = result["deleted"].get(t, 0)
        if isinstance(n, int):
            total += n
        print(f"{t:24s} {info['retention_days']:>4d}d {info['cutoff']:20s} {n if isinstance(n,int) else n:>12}")
    print("-" * 68)
    print(f"{'合计':24s} {'':5s} {'':20s} {total:>12,}\n")

    if not args.apply:
        print("→ 这是干跑。确认清单无误后加 --apply 执行真实删除。")
        print("→ 注意：DELETE 只会让 SQLite 标记页可复用，**磁盘空间要 VACUUM 才真正回收**")
        print("   （VACUUM 需停 8084 服务，建议低峰期：systemctl --user stop market-data-service.service")
        db.close()
        return 0

    # ⚠️ 2026-10-31 修正（批评家）：下面这两段原来**先真删一遍再打印表格、
    #    备份完又真删第二遍**（cleanup_old_data 在 dry_run=not args.apply 时
    #    已经真删了）。后果：
    #      · 表格打印的是**第一次真删的行数**（看起来像"可删清单"）
    #      · 备份发生在第一次删除之后 → 备份的是"删完剩下的"（旧数据没备到！）
    #      · 最后打印的"删除完成"是**第二次调用** → 几乎恒为全 0，
    #        被误读成"删除全部失败 / expect_rows 回滚"（2026-10-30 排查即栽在这）
    #    现在改为：开头 dry_run=True 只统计 → 打印 → 确认真删模式时
    #    【先备份、再一次性真删】，表格里的数字只是统计，与删除动作分离。
    if args.apply:
        # 备份 collection_log（最大的表）—— 必须在**删除之前**
        bak_dir = db_path.parent / "backups"
        bak_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        bak = bak_dir / f"collection_log_{stamp}.jsonl"
        print(f"→ 备份 collection_log → {bak}")
        rows = db.conn.execute(
            "SELECT id, source, data_type, status, error_message, started_at FROM collection_log").fetchall()
        with open(bak, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(dict(r), ensure_ascii=False) + "\n")
        print(f"  已备份 {len(rows):,} 行")

        # 一次性真删（内部对每张表仍是 COUNT → expect_rows 精确护栏 → DELETE）
        result = db.cleanup_old_data(ret, dry_run=False)
        print(f"\n→ 删除完成: {result['deleted']}")
    else:
        print("→ （未进入真删模式）")

    after = db_size_mb(db_path)
    print(f"→ 当前大小: {before:.1f} MB → {after:.1f} MB（磁盘回收需 VACUUM）")
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
