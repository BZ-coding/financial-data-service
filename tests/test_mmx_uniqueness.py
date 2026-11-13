"""Regression tests for mmx_search_data uniqueness (2026-11-13, docs/decisions/0005).

体检发现：表无唯一约束，insert_mmx_search 的 INSERT OR IGNORE 在无约束时是 no-op
→ 采集器每次全量重插同样结果（实测 77,887 行 / 4,274 distinct link，18 倍膨胀；
近 3 天新增 3,393 行仅 335 个不同 link）。

修复：link 业务唯一（部分唯一索引，排除 NULL/空 link），老库由
_migrate_mmx_uniqueness() 先去重再建索引（幂等）。
"""
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from market_service.database import Database  # noqa: E402


def test_migrate_dedups_and_creates_index(tmp_path):
    """老库（无约束、有重复）init 后：重复被清掉、唯一索引存在。"""
    db_path = str(tmp_path / "t.db")
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE mmx_search_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT NOT NULL, title TEXT,
        link TEXT, snippet TEXT, date TEXT, collected_at TEXT NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    for i in range(3):
        con.execute(
            "INSERT INTO mmx_search_data (query, link, collected_at) VALUES (?,?,?)",
            ("q", "http://same/link", f"2026-11-1{i} 00:00:00"))
    con.execute(
        "INSERT INTO mmx_search_data (query, link, collected_at) VALUES (?,?,?)",
        ("q", "http://other/link", "2026-11-12 00:00:00"))
    con.execute(
        "INSERT INTO mmx_search_data (query, link, collected_at) VALUES (?,?,?)",
        ("q", None, "2026-11-12 00:00:00"))  # NULL link 不参与唯一性
    con.commit()
    assert con.execute("SELECT COUNT(*) FROM mmx_search_data").fetchone()[0] == 5
    con.close()

    db = Database(db_path)  # init_tables → 迁移自动跑
    n = db.conn.execute("SELECT COUNT(*) FROM mmx_search_data").fetchone()[0]
    # 3 行同 link 保留 1 行 + 1 行 other + 1 行 NULL link = 3 行
    assert n == 3, n
    dup = db.conn.execute(
        "SELECT COALESCE(SUM(n-1),0) FROM (SELECT link, COUNT(*) n FROM mmx_search_data"
        " WHERE link IS NOT NULL AND link != '' GROUP BY link HAVING COUNT(*)>1)"
    ).fetchone()[0]
    assert dup == 0
    idx = [r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE tbl_name='mmx_search_data' AND type='index'")]
    assert "idx_mmx_link" in idx


def test_migrate_idempotent(tmp_path):
    """迁移重跑：行数不变、不报错。"""
    db_path = str(tmp_path / "t.db")
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE mmx_search_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT NOT NULL, title TEXT,
        link TEXT, snippet TEXT, date TEXT, collected_at TEXT NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    for i in range(2):
        con.execute("INSERT INTO mmx_search_data (query, link, collected_at) VALUES (?,?,?)",
                    ("q", "http://same/link", f"2026-11-1{i} 00:00:00"))
    con.commit()
    con.close()

    db1 = Database(db_path)
    n1 = db1.conn.execute("SELECT COUNT(*) FROM mmx_search_data").fetchone()[0]
    db2 = Database(db_path)
    n2 = db2.conn.execute("SELECT COUNT(*) FROM mmx_search_data").fetchone()[0]
    assert n1 == n2 == 1


def test_insert_ignores_duplicate_link(tmp_path):
    """同一 link 重复插入 → 只存一行（INSERT OR IGNORE 现在有索引兜底）。"""
    db = Database(str(tmp_path / "t.db"))
    db.insert_mmx_search({"query": "q", "title": "t", "link": "http://x/1",
                           "snippet": "s", "date": ""})
    db.insert_mmx_search({"query": "q", "title": "t2", "link": "http://x/1",
                           "snippet": "s2", "date": ""})
    n = db.conn.execute(
        "SELECT COUNT(*) FROM mmx_search_data WHERE link='http://x/1'").fetchone()[0]
    assert n == 1, n


def test_null_and_empty_link_not_deduped(tmp_path):
    """NULL / 空 link 不参与唯一性（搜索源缺 link 时不能因唯一索引整条丢）。"""
    db = Database(str(tmp_path / "t.db"))
    db.insert_mmx_search({"query": "q", "title": "t", "link": "",
                           "snippet": "s", "date": ""})
    db.insert_mmx_search({"query": "q2", "title": "t2", "link": None,
                           "snippet": "s", "date": ""})
    n = db.conn.execute("SELECT COUNT(*) FROM mmx_search_data").fetchone()[0]
    assert n == 2, n


def test_schema_and_migration_do_not_conflict(tmp_path):
    """schema.sql 建索引前必须先完成去重：老库含重复时 init_tables 不得报
    UNIQUE constraint failed（2026-11-12 公告 announcement_id 的顺序冲突同类坑）。"""
    db_path = str(tmp_path / "t.db")
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE mmx_search_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT NOT NULL, title TEXT,
        link TEXT, snippet TEXT, date TEXT, collected_at TEXT NOT NULL,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    for i in range(5):
        con.execute("INSERT INTO mmx_search_data (query, link, collected_at) VALUES (?,?,?)",
                    ("q", "http://dup/link", f"2026-11-1{i} 00:00:00"))
    con.commit()
    con.close()
    # 不抛异常即通过
    Database(db_path)
