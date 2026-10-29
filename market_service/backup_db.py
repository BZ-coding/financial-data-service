#!/usr/bin/env python3
"""backup_db —— 行情库在线备份（补上"恢复能力空档"）

背景（2026-10-29）
------------------
生产事故后复盘发现：**备份只有一份 Oct 26 的全库副本**（1.8GB，当时库还没瘦身），
之后只有清理工具自动产生的 ``collection_log_*.jsonl``（**只覆盖一张表**）。
也就是说：**10-26 之后的任何变更都没有可回滚的快照** ——
若今天再发生一次同类事故，能拿到的恢复源就是 3 天前的状态。

本脚本提供
----------
- **在线一致性全库备份**：用 SQLite 的 ``Connection.backup()`` API
  （不是 ``cp``！cp 在写入进行中会拿到**撕裂的副本**），**不停服**
- **gzip 压缩**（行情库里大量重复文本，压缩比很高）
- **配置表单独导出 JSON**：``subscriptions`` / ``tickets`` / ``source_metrics`` /
  ``failed_*`` —— 这几张表才是"业务配置"，体积小、人可读、**恢复最快**
  （全库 300MB 的备份不适合当日常恢复源）
- **MANIFEST**：记录 sha256、各表行数、源库路径与大小 → 备份可校验、可审计
- **保留期清理**：默认保留 7 天（且**至少保留最近 3 份**），自动清理旧备份

⚠️ 重要：**备份不是"当前状态快照"**（2026-10-29 验收员复核发现）
   Oct 26 那份原始备份里，某条已决定停用的记录（id=38 雪球）仍是 enabled=1
   —— 那是"停用决定"之前拍的。**备份能保证的是结构与历史数据的完整，
   不是业务决策的正确。**
   → 恢复时必须**以"当前期望状态"为准逐条核对**关键配置行，
     不能盲目相信备份里的业务状态字段。详见运维手册坑 45。

用法
----
    python3 backup_db.py                      # 备份 + 清理旧备份
    python3 backup_db.py --verify-only        # 只校验最近一份备份的完整性
    python3 backup_db.py --keep-days 14
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

BASE = Path(__file__).resolve().parent
DEFAULT_DB = BASE / "data" / "market.db"
DEFAULT_OUT = BASE / "data" / "backups"

#: 配置类表（小、人可读、恢复最快）——单独导出 JSON
CONFIG_TABLES = [
    "subscriptions",
    "tickets",
    "source_metrics",
    "failed_collections",
    "failed_transforms",
]

#: 全库备份里额外记录行数的表（用于 MANIFEST 校验，不导出内容）
COUNT_TABLES = [
    "subscriptions", "tickets", "source_metrics", "failed_collections",
    "failed_transforms", "price_data", "collection_log", "news_aggregator_data",
    "daily_data", "minute_data", "estimate_nav_data",
]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def table_counts(conn: sqlite3.Connection, tables: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
            out[t] = int(row[0]) if row else 0
        except Exception:
            out[t] = -1  # 表不存在
    return out


def export_config(conn: sqlite3.Connection) -> Dict[str, Any]:
    """导出配置类表为 JSON（人可读、快速恢复源）。"""
    data: Dict[str, Any] = {}
    for t in CONFIG_TABLES:
        try:
            cur = conn.execute(f"SELECT * FROM {t}")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            data[t] = {"columns": cols, "rows": rows}
        except Exception as e:
            data[t] = {"error": f"{type(e).__name__}: {e}"}
    return data


def do_backup(db_path: Path, outdir: Path, keep_days: int, min_keep: int = 3) -> Dict[str, Any]:
    outdir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    raw_path = outdir / f"market-{ts}.db"
    gz_path = outdir / f"market-{ts}.db.gz"
    cfg_path = outdir / f"config-{ts}.json"
    manifest_path = outdir / f"MANIFEST-{ts}.json"

    src_size = db_path.stat().st_size if db_path.exists() else -1
    src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    dst = sqlite3.connect(raw_path)
    try:
        # ★ 在线一致性备份 API（不停服、不撕裂）
        src.backup(dst)
    finally:
        dst.close()
        src.close()

    # 压缩
    with open(raw_path, "rb") as fin, gzip.open(gz_path, "wb", compresslevel=6) as fout:
        shutil.copyfileobj(fin, fout)
    raw_path.unlink(missing_ok=True)

    # 配置表导出（从备份库里读，保证与快照一致）
    bconn = sqlite3.connect(f"file:{gz_path}?mode=ro", uri=True) if False else None
    # 注：gzip 不能直接给 sqlite 打开 → 用临时解压读取
    tmp_raw = outdir / f".tmp-{ts}.db"
    with gzip.open(gz_path, "rb") as fin, open(tmp_raw, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    rconn = sqlite3.connect(f"file:{tmp_raw}?mode=ro", uri=True)
    try:
        counts = table_counts(rconn, COUNT_TABLES)
        config = export_config(rconn)
    finally:
        rconn.close()
        tmp_raw.unlink(missing_ok=True)
    cfg_path.write_text(json.dumps(config, ensure_ascii=False, indent=1, default=str), encoding="utf-8")

    manifest = {
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_db": str(db_path),
        "source_size": src_size,
        "backup_file": gz_path.name,
        "backup_size": gz_path.stat().st_size,
        "backup_sha256": sha256_of(gz_path),
        "config_file": cfg_path.name,
        "counts": counts,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")

    # 清理旧备份（保留期 + 至少保留最近 min_keep 份）
    cutoff = time.time() - keep_days * 86400
    all_backups = sorted(outdir.glob("market-*.db.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
    removed = []
    for i, p in enumerate(all_backups):
        if i < min_keep:
            continue
        if p.stat().st_mtime < cutoff:
            for suffix in (".db.gz",):
                try:
                    p.unlink()
                    removed.append(p.name)
                except Exception:
                    pass
            for meta in outdir.glob(p.name.replace(".db.gz", "*.json")):
                try:
                    meta.unlink()
                except Exception:
                    pass
    # 顺带清理旧的 collection_log jsonl（保留期同）
    for p in outdir.glob("collection_log_*.jsonl"):
        if p.stat().st_mtime < cutoff:
            try:
                p.unlink()
                removed.append(p.name)
            except Exception:
                pass

    manifest["removed_old"] = removed
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return manifest


def verify_latest(outdir: Path) -> bool:
    """校验最近一份备份：sha256 一致 + 能解压 + 表可读。"""
    backups = sorted(outdir.glob("market-*.db.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not backups:
        print("[verify] ❌ 没有任何备份文件")
        return False
    latest = backups[0]
    manifest_file = outdir / (latest.name.replace(".db.gz", "") .replace("market-", "MANIFEST-") + ".json")
    if not manifest_file.exists():
        print(f"[verify] ❌ 找不到 MANIFEST: {manifest_file.name}")
        return False
    m = json.loads(manifest_file.read_text(encoding="utf-8"))
    actual = sha256_of(latest)
    if actual != m.get("backup_sha256"):
        print(f"[verify] ❌ sha256 不匹配！\n  期望 {m.get('backup_sha256')}\n  实际 {actual}")
        return False
    try:
        with gzip.open(latest, "rb") as f:
            head = f.read(16)
        if not head.startswith(b"SQLite format 3"):
            print("[verify] ❌ 解压后不是合法 SQLite 文件")
            return False
    except Exception as e:
        print(f"[verify] ❌ 解压失败: {e}")
        return False
    print(f"[verify] ✅ {latest.name} 校验通过（sha256 一致、可解压、SQLite 头正确）")
    print(f"[verify]    源库大小 {m.get('source_size')} → 备份 {m.get('backup_size')} bytes")
    print(f"[verify]    表行数: {json.dumps(m.get('counts'), ensure_ascii=False)}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="行情库在线备份")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--outdir", default=str(DEFAULT_OUT))
    ap.add_argument("--keep-days", type=int, default=7)
    ap.add_argument("--verify-only", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    if args.verify_only:
        return 0 if verify_latest(outdir) else 1

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[backup] ❌ 源库不存在: {db_path}")
        return 1
    t0 = time.time()
    m = do_backup(db_path, outdir, args.keep_days)
    print(f"[backup] ✅ 完成，用时 {time.time() - t0:.1f}s")
    print(f"[backup]    备份文件: {m['backup_file']} ({m['backup_size']} bytes)")
    print(f"[backup]    配置导出: {m['config_file']}")
    print(f"[backup]    sha256: {m['backup_sha256'][:32]}…")
    print(f"[backup]    源库 {m['source_size']} bytes → 压缩后 {m['backup_size']} bytes")
    print(f"[backup]    清理旧备份 {len(m.get('removed_old', []))} 个")
    print(f"[backup]    关键表行数: {json.dumps(m['counts'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
