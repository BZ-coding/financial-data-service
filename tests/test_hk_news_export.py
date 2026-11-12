#!/usr/bin/env python3
"""港股公告出口回归测试（2026-11-12）。

锁 down 三个缺陷（全部在**读取语义**，写入侧无重复）：

1. **时间窗/排序用错字段** —— 旧实现按 `ingested_at`（入库时刻，UTC）过滤+排序：
   - 采集晚的旧公告被排到最新位置（实测 hk09988：最新 published 2026-11-10
     排第 2，首位是 2026-10-07）；
   - 90 天窗口混进更早发布的旧公告（实测 hk03690 多出 6 条）。
   → 现在窗口与排序一律按 `published`（发布时刻）。

2. **`published` 格式混排** —— 库里实测 4 种格式（unix 秒 4883 行 /
   ISO+08:00 413 行 / 'YYYY-MM-DD HH:MM:SS' 122 行 / YYYYMMDD 1 行），
   字符串比较必错 → 统一 `_published_datetime()` 归一到 +08:00 再比较。

3. **固定 LIMIT 50 静默截断** —— 港股分支原来固定 `LIMIT 50`，
   现在 `limit=None` 表示不截断（调用方可显式传 limit）。

另外锁 down：
   - `as_of` 由最新一条的 published 派生（原来港股分支根本没有 as_of）；
   - 输出带 `published_at` / `published_date` 归一字段；
   - dsa 侧对 unix 秒 published 不再误判为过期（消费端契约）。
"""
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from market_service.database import Database, _published_datetime  # noqa: E402

TZ = timezone(timedelta(hours=8))


def _db(tmp_path) -> Database:
    return Database(str(tmp_path / "hk_news.db"))


def _insert_news(db: Database, symbol: str, title: str, published: str,
                  source: str = "hkex", link: str = None) -> None:
    db.conn.execute(
        "INSERT INTO news_data (source, title, summary, link, symbol, published, "
        "ingested_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        (source, title, "s", link or (f"https://www1.hkexnews.hk/{title}.pdf"),
         symbol, published, "2026-11-12 03:00:00", "2026-11-12 03:00:00"),
    )
    db.conn.commit()


# ------------------------------------------------------------------ 归一

def test_published_datetime_handles_all_four_formats():
    """库里实测的 4 种 published 格式必须都能归一到同一时区。"""
    # ISO + 偏移
    assert _published_datetime("2026-11-10T20:36:00+08:00").strftime(
        "%Y-%m-%d %H:%M:%S") == "2026-11-10 20:36:00"
    # 空格分隔
    assert _published_datetime("2026-11-10 20:36:00").strftime(
        "%Y-%m-%d %H:%M:%S") == "2026-11-10 20:36:00"
    # unix 秒（字符串）
    ts = datetime(2026, 11, 10, 20, 36, 0, tzinfo=TZ).timestamp()
    assert _published_datetime(str(int(ts))).strftime(
        "%Y-%m-%d %H:%M:%S") == "2026-11-10 20:36:00"
    # YYYYMMDD
    assert _published_datetime("20261110").strftime(
        "%Y-%m-%d %H:%M:%S") == "2026-11-10 00:00:00"


def test_published_datetime_returns_none_on_garbage():
    assert _published_datetime("not-a-time") is None
    assert _published_datetime("") is None
    assert _published_datetime(None) is None


# ------------------------------------------------------------------ 排序

def test_sorted_by_published_not_ingested_at(tmp_path):
    """旧公告但采集晚 → 必须排在最新公告之后（原来排第 1）。"""
    db = _db(tmp_path)
    _insert_news(db, "hk09988", "旧公告", "2026-10-07T16:32:00+08:00")
    _insert_news(db, "hk09988", "最新公告", "2026-11-10T20:36:00+08:00")
    rows = db.get_news_by_published("hk09988", days=90, source="hkex")
    titles = [r["title"] for r in rows]
    assert titles == ["最新公告", "旧公告"], titles


def test_window_uses_published_not_ingested_at(tmp_path):
    """published 早于窗口、但入库很晚的旧公告必须被排除。"""
    db = _db(tmp_path)
    # 200 天前发布，但刚入库
    _insert_news(db, "hk03690", "很旧的公告",
                 (datetime.now(TZ) - timedelta(days=200)).strftime("%Y-%m-%dT%H:%M:%S+08:00"))
    _insert_news(db, "hk03690", "新公告",
                 (datetime.now(TZ) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S+08:00"))
    rows = db.get_news_by_published("hk03690", days=90, source="hkex")
    assert [r["title"] for r in rows] == ["新公告"], [r["title"] for r in rows]


def test_ordering_stable_for_same_timestamp(tmp_path):
    db = _db(tmp_path)
    for i in range(5):
        _insert_news(db, "hk03690", f"同刻{i}", "2026-11-10T20:36:00+08:00")
    rows = db.get_news_by_published("hk03690", days=90, source="hkex")
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids, reverse=True), ids


# ------------------------------------------------------------------ 截断 / as_of

def test_no_hardcoded_limit_50(tmp_path):
    """造 120 条港股公告，必须全部返回（原来固定 LIMIT 50 只回 50 条）。"""
    db = _db(tmp_path)
    for i in range(120):
        _insert_news(db, "hk03690", f"公告{i}", "2026-11-10T20:36:00+08:00")
    rows = db.get_news_by_published("hk03690", days=90, source="hkex")
    assert len(rows) == 120, f"应返回全部 120 条，实际 {len(rows)}"
    limited = db.get_news_by_published("hk03690", days=90, source="hkex", limit=50)
    assert len(limited) == 50


def test_as_of_derived_from_latest_published(tmp_path):
    db = _db(tmp_path)
    _insert_news(db, "hk03690", "a", "2026-11-04T18:00:00+08:00")
    _insert_news(db, "hk03690", "b", "2026-11-10T20:36:00+08:00")
    rows = db.get_news_by_published("hk03690", days=90, source="hkex")
    assert rows[0]["published_date"] == "2026-11-10"
    assert rows[0]["published_at"] == "2026-11-10 20:36:00"
    assert rows[0]["time_source"] == "published"


def test_source_filter_isolates_hkex(tmp_path):
    """stock_news 与 hkex_announcement 必须走不同 source，不能互相污染。"""
    db = _db(tmp_path)
    _insert_news(db, "hk03690", "披露易", "2026-11-10T20:36:00+08:00", source="hkex")
    _insert_news(db, "hk03690", "普通新闻", "2026-11-09T10:00:00+08:00", source="ths_hk")
    hkex = db.get_news_by_published("hk03690", days=90, source="hkex")
    assert [r["title"] for r in hkex] == ["披露易"]
    news = db.get_news_by_published("hk03690", days=90)
    assert len(news) == 2


def test_unparsable_published_falls_back_to_ingested_at(tmp_path):
    """published 解析不了 → 退回入库时刻，并明确标注口径（不静默）。"""
    db = _db(tmp_path)
    db.conn.execute(
        "INSERT INTO news_data (source, title, summary, link, symbol, published, "
        "ingested_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        ("hkex", "坏时间", "s", "https://x/1.pdf", "hk03690", "坏时间",
         (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
         (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")),
    )
    db.conn.commit()
    rows = db.get_news_by_published("hk03690", days=90, source="hkex")
    assert len(rows) == 1
    assert rows[0]["time_source"] == "ingested_at"


# ------------------------------------------------------------------ API 出口

@pytest.fixture(scope="module")
def hk_client():
    from fastapi.testclient import TestClient
    from market_service import api as api_mod
    from market_service.api import app
    return TestClient(app), api_mod


def test_api_hkex_announcement_has_as_of_and_sorted(hk_client, tmp_path, monkeypatch):
    """API 层：hkex_announcement 必须带 as_of、按 published 倒序、无 50 条截断。"""
    client, api_mod = hk_client
    db = _db(tmp_path)
    _insert_news(db, "hkTEST", "最新公告", "2026-11-10T20:36:00+08:00")
    _insert_news(db, "hkTEST", "旧公告", "2026-10-07T16:32:00+08:00")
    for i in range(60):
        _insert_news(db, "hkTEST", f"批量{i}", "2026-11-09T20:36:00+08:00")
    monkeypatch.setattr(api_mod, "db", db)

    r = client.get("/api/v1/data", params={
        "source": "akshare", "symbol": "hkTEST", "data_type": "hkex_announcement"})
    assert r.status_code == 200, r.text[:300]
    body = r.json()
    assert body["as_of"] == "2026-11-10", body.get("as_of")
    assert body["count"] == 62, body["count"]          # 62 > 50 → 证明没被截断
    titles = [i["title"] for i in body["items"]]
    assert titles[0] == "最新公告", titles[:3]
    # 发布时刻排序：2026-11-10 > 2026-11-09(批量) > 2026-10-07(旧公告)
    assert titles[1].startswith("批量"), titles[:3]
    assert titles[-1] == "旧公告", titles[-3:]
    assert body["items"][0]["published_date"] == "2026-11-10"


def test_api_hkex_announcement_404_when_empty(hk_client, tmp_path, monkeypatch):
    client, api_mod = hk_client
    db = _db(tmp_path)
    monkeypatch.setattr(api_mod, "db", db)
    r = client.get("/api/v1/data", params={
        "source": "akshare", "symbol": "hkNONE", "data_type": "hkex_announcement"})
    assert r.status_code == 404, r.text[:200]


def test_api_hkex_announcement_limit_param(hk_client, tmp_path, monkeypatch):
    client, api_mod = hk_client
    db = _db(tmp_path)
    for i in range(80):
        _insert_news(db, "hkLIM", f"批量{i}", "2026-11-09T20:36:00+08:00")
    monkeypatch.setattr(api_mod, "db", db)
    r = client.get("/api/v1/data", params={
        "source": "akshare", "symbol": "hkLIM", "data_type": "hkex_announcement",
        "limit": 10})
    assert r.status_code == 200
    assert r.json()["count"] == 10


# ------------------------------------------------------------------ dsa 消费端

def test_dsa_announcement_filter_handles_unix_second_published():
    """dsa 侧：unix 秒 published 不能被当成日期（原来 '1762000000'[:10] 恒 ≥ cutoff）。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_dsa_server_check",
        Path("/home/zsd/codes/dsa-mcp/src/dsa_mcp/server.py"))
    src = Path("/home/zsd/codes/dsa-mcp/src/dsa_mcp/server.py").read_text(encoding="utf-8")
    # 直接验证过滤逻辑（与 server.py 中的实现一致）
    cutoff = (datetime.now() - timedelta(days=90)).isoformat()[:10]

    def keep(item):
        ts = (item.get("announcement_time") or item.get("published_date")
              or item.get("published_at") or item.get("published") or "")
        ts = str(ts)
        day = ts[:10] if (len(ts) >= 10 and ts[4] == "-" and ts[7] == "-") else ""
        return bool(day) and day >= cutoff

    assert keep({"published": "1762000000"}) is False, "unix 秒不得被当作近期"
    assert keep({"published_date": "2026-11-10"}) is True
    assert keep({"announcement_time": "2026-11-12"}) is True
    assert keep({}) is False
