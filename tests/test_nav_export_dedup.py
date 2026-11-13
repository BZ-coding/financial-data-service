"""Regression tests for nav_history export dedup (2026-11-13, docs/decisions/0005).

体检发现：nav_data 约束 UNIQUE(**source**, symbol, nav_date) 含 source，
跨源同日会各存一行（实测 008114 有 11 组 akshare + eastmoney_web 同日双行）。
业务上同一基金同一天只应有一个净值；出口不去重 → /api/v1/history 与
bars_from_nav 拿到同日多行 → 日期序列错位。现在出口按同日取 id 最大
（最新采集）的一行。
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from market_service.database import Database  # noqa: E402


def _mk(db_path):
    """建带跨源同日双行的 nav_data。"""
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE nav_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, symbol TEXT NOT NULL,
        nav_date DATE NOT NULL, nav REAL NOT NULL, acc_nav REAL, change_pct REAL,
        raw_data TEXT, ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, symbol, nav_date))""")
    rows = [
        # (source, nav_date, nav, ingested_at)——akshare 先入库（id 小），eastmoney_web 后入库
        ("akshare", "2026-04-01", 1.757, "2026-04-07 11:15:17"),
        ("eastmoney_web", "2026-04-01", 1.757, "2026-04-07 20:07:41"),
        ("akshare", "2026-04-02", 1.7618, "2026-04-07 20:16:46"),
        ("eastmoney_web", "2026-04-02", 1.7618, "2026-04-07 20:07:41"),
        ("akshare", "2026-04-03", 1.736, "2026-04-07 11:15:17"),
    ]
    for src, d, nav, ing in rows:
        con.execute(
            "INSERT INTO nav_data (source, symbol, nav_date, nav, ingested_at) "
            "VALUES (?,?,?,?,?)", (src, "008114", d, nav, ing))
    con.commit()
    con.close()


def test_cross_source_same_date_deduped_at_export(tmp_path):
    """库里同日双源两行 → 出口每天只一行。"""
    p = str(tmp_path / "t.db")
    _mk(p)
    db = Database(p)
    rows = db.get_nav_history("008114", days=250)
    dates = [str(r["nav_date"])[:10] for r in rows]
    assert len(dates) == len(set(dates)), dates
    assert sorted(dates) == dates  # 升序


def test_export_keeps_latest_ingest_per_date(tmp_path):
    """同日多行取 id 最大（最新采集）的一行。"""
    p = str(tmp_path / "t.db")
    _mk(p)
    db = Database(p)
    rows = {str(r["nav_date"])[:10]: r["nav"] for r in db.get_nav_history("008114", days=250)}
    assert rows["2026-04-03"] == 1.736


def test_raw_db_untouched(tmp_path):
    """出口去重不改动库（修复在读取侧，不迁移数据）。"""
    p = str(tmp_path / "t.db")
    _mk(p)
    Database(p)
    con = sqlite3.connect(p)
    n = con.execute(
        "SELECT COUNT(*) FROM nav_data WHERE symbol='008114'"
        " AND nav_date='2026-04-01'").fetchone()[0]
    assert n == 2, n
