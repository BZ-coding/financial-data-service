"""Regression tests for announcement identity / dedup / freshness-cadence (2026-11-12 phase6).

Three defects these lock down:

1. **公告身份依赖完整 URL** — cninfo 的同一个 announcementId 会返回两种时间格式
   (`2026-11-03 00:00:00` 与 `2026-11-03`)，且 link 内嵌 `announcementTime=` 参数随之
   变化。原库 `UNIQUE(link)` 因此失效 → 同一公告重复入库（002202 实测 135 行 / 89 标题）。
   现在稳定身份 = `announcement_id`，唯一索引 `(source, symbol, announcement_id)`。

2. **get_announcements 硬编码 LIMIT 50** — 90 天窗口被静默砍到 50 条，
   phase6 报告把 "50(封顶)" 当成真实条数。现在 `limit=None` 表示不截断。

3. **daily/weekly 源被按准实时源判 STALE** — `hkex_enhanced`(1440min) /
   `news_aggregator`(1440min) / `csindex`(1440|10080min) 被 180 分钟阈值卡成
   假故障。现在阈值按订阅 cadence 推导：`max(180, frequency_min × 2)`。
"""
import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from market_service.database import Database  # noqa: E402
from market_service.market_data_freshness import (  # noqa: E402
    ACTIVE_SOURCE_MAX_STALE_MIN,
    SOURCE_CADENCE_TOLERANCE,
    _load_source_cadence,
    _source_stale_threshold,
)


# ---------------------------------------------------------------- announcement identity

def _db(tmp_path) -> Database:
    db = Database(str(tmp_path / "t.db"))
    return db


def test_extract_announcement_id_prefers_cninfo_id():
    from market_service.database import _extract_announcement_id
    link = ("http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
            "&announcementId=1225661217&announcementTime=2026-11-07 00:00:00")
    assert _extract_announcement_id(link) == "cninfo:1225661217"


def test_extract_announcement_id_falls_back_to_em_pdf_id():
    from market_service.database import _extract_announcement_id
    link = "https://pdf.dfcfw.com/pdf/H2_AN202611101831571607_1.pdf"
    assert _extract_announcement_id(link) == "em:AN202611101831571607"


def test_canonical_link_strips_dynamic_time_parameter():
    from market_service.database import _canonical_announcement_link
    a = ("http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
        "&announcementId=1225661217&announcementTime=2026-11-07 00:00:00")
    b = ("http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
        "&announcementId=1225661217&announcementTime=2026-11-07")
    assert _canonical_announcement_link(a) == _canonical_announcement_link(b)


def test_canonical_time_is_date_granularity():
    from market_service.database import _canonical_announcement_time
    assert _canonical_announcement_time("2026-11-03 00:00:00") == "2026-11-03"
    assert _canonical_announcement_time("2026-11-03") == "2026-11-03"


def test_same_announcement_different_time_format_dedupes_to_one_row(tmp_path):
    """同一 announcementId 的两种时间格式入库后只应保留一行，且日期归一。"""
    db = _db(tmp_path)
    base = ("http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
            "&announcementId=1225661217")
    db.insert_announcement({
        "source": "cninfo", "symbol": "002202",
        "title": "关于为全资子公司提供担保的公告",
        "announcement_time": "2026-11-03 00:00:00",
        "link": base + "&announcementTime=2026-11-03 00:00:00",
    })
    db.insert_announcement({
        "source": "cninfo", "symbol": "002202",
        "title": "关于为全资子公司提供担保的公告",
        "announcement_time": "2026-11-03",
        "link": base + "&announcementTime=2026-11-03",
    })
    rows = db.conn.execute(
        "SELECT announcement_time, link, announcement_id FROM announcement_data").fetchall()
    assert len(rows) == 1, f"同一 announcementId 应只保留一行，实际 {len(rows)} 行"
    assert rows[0][0] == "2026-11-03"
    assert rows[0][2] == "cninfo:1225661217"


def test_different_announcements_are_not_collapsed(tmp_path):
    db = _db(tmp_path)
    for aid in ("1225661216", "1225661217"):
        db.insert_announcement({
            "source": "cninfo", "symbol": "002202",
            "title": f"公告 {aid}",
            "announcement_time": "2026-11-07",
            "link": (f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
                     f"&announcementId={aid}&announcementTime=2026-11-07"),
        })
    n = db.conn.execute("SELECT COUNT(*) FROM announcement_data").fetchone()[0]
    assert n == 2, "不同 announcementId 不得被合并"


def test_identity_unique_index_exists(tmp_path):
    db = _db(tmp_path)
    idx = {r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='announcement_data'")}
    assert "idx_ann_identity" in idx, "必须建 (source,symbol,announcement_id) 唯一索引"


def test_migration_is_idempotent(tmp_path):
    db = _db(tmp_path)
    db.insert_announcement({
        "source": "cninfo", "symbol": "002202", "title": "x",
        "announcement_time": "2026-11-03 00:00:00",
        "link": ("http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
                 "&announcementId=999&announcementTime=2026-11-03 00:00:00"),
    })
    db._migrate_announcement_identity()
    db._migrate_announcement_identity()
    n = db.conn.execute("SELECT COUNT(*) FROM announcement_data").fetchone()[0]
    assert n == 1


# ---------------------------------------------------------------- no LIMIT 50

def test_get_announcements_has_no_hardcoded_limit(tmp_path):
    """造 120 条公告，必须全部返回（原来硬编码 LIMIT 50 只返回 50 条）。"""
    db = _db(tmp_path)
    for i in range(120):
        db.insert_announcement({
            "source": "cninfo", "symbol": "002202", "title": f"公告{i}",
            "announcement_time": "2026-11-07",
            "link": (f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
                     f"&announcementId={1000+i}&announcementTime=2026-11-07"),
        })
    rows = db.get_announcements("002202", days=90)
    assert len(rows) == 120, f"90 天窗口应返回全部 120 条，实际 {len(rows)} 条"
    assert db.get_announcements("002202", days=90, limit=50) is not None
    assert len(db.get_announcements("002202", days=90, limit=50)) == 50


def test_get_announcements_sorted_newest_first(tmp_path):
    db = _db(tmp_path)
    for day, aid in (("2026-11-01", "1"), ("2026-11-12", "2"), ("2026-11-07", "3")):
        db.insert_announcement({
            "source": "cninfo", "symbol": "002202", "title": f"公告{aid}",
            "announcement_time": day,
            "link": (f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002202"
                     f"&announcementId={aid}&announcementTime={day}"),
        })
    rows = db.get_announcements("002202", days=90)
    assert [r["announcement_time"] for r in rows] == ["2026-11-12", "2026-11-07", "2026-11-01"]


# ---------------------------------------------------------------- freshness cadence

def test_source_threshold_follows_subscription_cadence(tmp_path):
    """daily 源阈值应为 1440×2=2880 分钟，而不是准实时的 180 分钟。"""
    db = _db(tmp_path)
    db.conn.execute(
        "INSERT INTO subscriptions (id,name,type,config,symbol,data_types,frequency_min,"
        "enabled,trading_hours_only,backup_sources) "
        "VALUES (1,'hkex','daily','{}','hk03690','announcement',1440,1,0,NULL)")
    db.conn.execute(
        "INSERT INTO collection_log (subscription_id,source,data_type,status,started_at) "
        "VALUES (1,'hkex_enhanced','announcement','success','2026-11-12 10:00:00')")
    cur = db.conn.cursor()
    cmap = _load_source_cadence(cur, "2026-11-01 00:00:00")
    thr = _source_stale_threshold("hkex_enhanced", cmap)
    assert thr == max(ACTIVE_SOURCE_MAX_STALE_MIN, 1440 * SOURCE_CADENCE_TOLERANCE)
    assert thr == 2880


def test_realtime_source_keeps_180_min_floor(tmp_path):
    """准实时源 (akshare, frequency_min=5) 仍按 180 分钟下限判定。"""
    db = _db(tmp_path)
    db.conn.execute(
        "INSERT INTO subscriptions (id,name,type,config,symbol,data_types,frequency_min,"
        "enabled,trading_hours_only,backup_sources) "
        "VALUES (1,'ak','realtime','{}','002202','price',5,1,1,NULL)")
    db.conn.execute(
        "INSERT INTO collection_log (subscription_id,source,data_type,status,started_at) "
        "VALUES (1,'akshare','price','success','2026-11-12 22:00:00')")
    cur = db.conn.cursor()
    cmap = _load_source_cadence(cur, "2026-11-01 00:00:00")
    assert _source_stale_threshold("akshare", cmap) == ACTIVE_SOURCE_MAX_STALE_MIN


def test_disabled_subscription_does_not_set_cadence(tmp_path):
    """停用订阅不参与判据。"""
    db = _db(tmp_path)
    db.conn.execute(
        "INSERT INTO subscriptions (id,name,type,config,symbol,data_types,frequency_min,"
        "enabled,trading_hours_only,backup_sources) "
        "VALUES (1,'off','daily','{}','002202','announcement',10080,0,0,NULL)")
    db.conn.execute(
        "INSERT INTO collection_log (subscription_id,source,data_type,status,started_at) "
        "VALUES (1,'csindex','index','success','2026-11-12 05:00:00')")
    cur = db.conn.cursor()
    cmap = _load_source_cadence(cur, "2026-11-01 00:00:00")
    assert "csindex" not in cmap
    assert _source_stale_threshold("csindex", cmap) == ACTIVE_SOURCE_MAX_STALE_MIN


def test_unknown_source_falls_back_to_180(tmp_path):
    db = _db(tmp_path)
    cur = db.conn.cursor()
    assert _source_stale_threshold("no_such_source", {}) == ACTIVE_SOURCE_MAX_STALE_MIN


def test_backup_sources_are_read_from_subscriptions(tmp_path):
    """备源关系来自 subscriptions.backup_sources（真实配置）。"""
    db = _db(tmp_path)
    db.conn.execute(
        "INSERT INTO subscriptions (id,name,type,config,symbol,data_types,frequency_min,"
        "enabled,trading_hours_only,backup_sources) "
        "VALUES (1,'x','daily','{}','002202','announcement',1440,1,0,'akshare_hist,tushare')")
    db.conn.execute(
        "INSERT INTO collection_log (subscription_id,source,data_type,status,started_at) "
        "VALUES (1,'hkex_enhanced','announcement','success','2026-11-12 10:00:00')")
    cur = db.conn.cursor()
    cmap = _load_source_cadence(cur, "2026-11-01 00:00:00")
    assert cmap["hkex_enhanced"]["backups"] == {"akshare_hist", "tushare"}
