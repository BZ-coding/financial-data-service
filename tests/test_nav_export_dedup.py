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


def test_export_dedup_holds_even_if_duplicates_reappear(tmp_path):
    """即使库里又出现同日双行（迁移前的老库/外部写入），出口仍只出一行。

    这是"读取侧兜底"的价值：迁移只解决存量，防御要落在出口。
    """
    p = str(tmp_path / "d.db")
    con = sqlite3.connect(p)
    con.execute("""CREATE TABLE nav_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, symbol TEXT NOT NULL,
        nav_date DATE NOT NULL, nav REAL NOT NULL, acc_nav REAL, change_pct REAL,
        raw_data TEXT, ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, symbol, nav_date))""")
    # 绕过迁移直接塞同日双行（模拟外部写入/迁移前老库）
    for src, ing in (("akshare", "2026-04-07 11:15:17"), ("eastmoney_web", "2026-04-07 20:07:41")):
        con.execute(
            "INSERT INTO nav_data (source, symbol, nav_date, nav, ingested_at) VALUES (?,?,?,?,?)",
            (src, "008114", "2026-04-01", 1.757, ing))
    con.commit()
    con.close()

    db = Database(p)
    rows = db.get_nav_history("008114", days=250)
    dates = [str(r["nav_date"])[:10] for r in rows]
    assert len(dates) == len(set(dates)), dates


def test_migration_merges_cross_source_rows(tmp_path):
    """老库跨源同日重复 → 迁移合并成一行 + 建业务唯一索引（幂等）。"""
    p = str(tmp_path / "m.db")
    _mk(p)
    db = Database(p)
    n = db.conn.execute("SELECT COUNT(*) FROM nav_data").fetchone()[0]
    assert n == 3, n  # 5 行 → 同日合并后 3 个日期
    dup = db.conn.execute(
        "SELECT COUNT(*) FROM (SELECT symbol, nav_date, COUNT(*) n FROM nav_data"
        " GROUP BY 1,2 HAVING COUNT(*)>1)").fetchone()[0]
    assert dup == 0
    idx = [r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE tbl_name='nav_data' AND type='index'")]
    assert "idx_nav_symbol_date" in idx
    # 幂等
    db2 = Database(p)
    assert db2.conn.execute("SELECT COUNT(*) FROM nav_data").fetchone()[0] == 3


def test_cross_source_recollect_replaces_not_duplicates(tmp_path):
    """换源再采同一基金同一天 → 替换而非并存（业务唯一索引 + INSERT OR REPLACE）。"""
    p = str(tmp_path / "r.db")
    _mk(p)
    db = Database(p)
    before = db.conn.execute(
        "SELECT COUNT(*) FROM nav_data WHERE symbol='008114' AND nav_date='2026-04-01'"
    ).fetchone()[0]
    db.insert_nav({"source": "newsource", "symbol": "008114",
                   "nav_date": "2026-04-01", "nav": 1.757})
    after = db.conn.execute(
        "SELECT COUNT(*) FROM nav_data WHERE symbol='008114' AND nav_date='2026-04-01'"
    ).fetchone()[0]
    assert before == 1 and after == 1, (before, after)
    src = db.conn.execute(
        "SELECT source FROM nav_data WHERE symbol='008114' AND nav_date='2026-04-01'"
    ).fetchone()[0]
    assert src == "newsource", src
