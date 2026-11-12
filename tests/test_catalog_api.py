#!/usr/bin/env python3
"""三层导航后端单测：/api/v1/catalog、/api/v1/catalog/<cat>、/api/v1/detail

覆盖（2026-11-01 阶段 2 改版）：
  · L1 六个类别齐全、计数与库实查一致、stale 判定（龙虎榜/涨停池应 stale）
  · L2 各类列表：indices 14 / holdings 12 / fundflow 截断但带 total /
    dragon、limitup 停更标记 / news 4 源含 caijing empty
  · L3 详情：**三种品种自动识别**（指数/股票/基金）+ 指标可算 + 名字解析
  · 名字解析：港股名字必须能从 subscriptions.name 解析出来（库里其他表没有）
  · 边界：未知类别 404、非法 symbol 422、不存在 404
  · 数据质量：指数必须走**去重后**的 bars（周末/复用快照已剔除）
"""
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from market_service import api as api_mod  # noqa: E402
from market_service.api import app  # noqa: E402

client = TestClient(app)

CATS = ("indices", "holdings", "fundflow", "dragon", "limitup", "news")


@pytest.fixture(scope="module")
def catalog():
    api_mod._CAT_CACHE.update({"stamp": None, "at": 0.0, "payload": None})
    r = client.get("/api/v1/catalog")
    assert r.status_code == 200, r.text[:300]
    return r.json()


# ── L1 ─────────────────────────────────────────────────────────────────────
def test_catalog_six_categories(catalog):
    keys = [c["key"] for c in catalog["categories"]]
    assert keys == list(CATS), keys
    for c in catalog["categories"]:
        for k in ("key", "name", "count", "unit", "latest", "stale", "stale_days"):
            assert k in c, f"{c['key']} 缺 {k}"


def test_catalog_counts_match_db(catalog):
    """L1 的 count 必须与数据库实查一致（不许硬编码）。"""
    db = api_mod.db
    by = {c["key"]: c for c in catalog["categories"]}

    n_idx = db.conn.execute("SELECT count(DISTINCT code) FROM index_data").fetchone()[0]
    assert by["indices"]["count"] == n_idx, (by["indices"]["count"], n_idx)

    n_dragon = db.conn.execute("SELECT count(DISTINCT code) FROM dragon_tiger_data").fetchone()[0]
    assert by["dragon"]["count"] == n_dragon

    n_limit = db.conn.execute("SELECT count(DISTINCT code) FROM limit_up_pool_data").fetchone()[0]
    assert by["limitup"]["count"] == n_limit

    # fundflow 的 count 口径在 2026-11-02「指标看板定稿」时**故意**改了：
    # L1 count = **本批**条数（最新一次采集的榜单长度），不是历史累计 DISTINCT symbol。
    # 理由（api.py 原注释）：前端点进去只有 50 条，count 却报 2006 是欺骗性 UI；
    # 历史累计另放 L2 的 total_history。→ 这里按**同一口径**从库实查比对。
    n_flow_batch = db.conn.execute(
        "SELECT COUNT(*) FROM fund_flow_data WHERE flow_type='main_fund_rank'"
        " AND collected_at >= (SELECT datetime(max(collected_at),'-5 minutes')"
        "   FROM fund_flow_data WHERE flow_type='main_fund_rank')"
    ).fetchone()[0]
    assert by["fundflow"]["count"] == n_flow_batch, (by["fundflow"]["count"], n_flow_batch)

    # holdings = price_data 标的 + 近 30 天有净值的基金
    n_price = db.conn.execute("SELECT count(DISTINCT symbol) FROM price_data").fetchone()[0]
    assert by["holdings"]["count"] >= n_price


def test_catalog_stale_flags(catalog):
    """stale 判定正确性（2026-11-01 更新）：

    龙虎榜/涨停池的采集链路已修复（bcbc405 只建了半截管道：有 _fetch_ 函数和读
    端点，但 collect() 无分发分支、scheduler 无入库分支、无订阅 → 只停更）。
    数据恢复更新后**不能再断言"必须 stale"**——那是把 bug 写成预期。
    现在断言判定逻辑本身：stale ⟺ stale_days 超阈值（10 天），两者自洽。
    """
    by = {c["key"]: c for c in catalog["categories"]}
    for cat in ("dragon", "limitup"):
        stale = by[cat]["stale"]
        days = by[cat]["stale_days"] or 0
        assert stale == (days > 10), (cat, stale, days)
        if not stale:
            assert days <= 10, (cat, days)
    # 指数/快讯是近期的
    assert by["news"]["stale"] is False, by["news"]
    # count 口径不同要显式标单位：快讯是"条"（条目数），其余是"个"（标的数）
    assert by["news"]["unit"] == "条", by["news"]
    assert by["indices"]["unit"] == "个", by["indices"]
    # 快讯 count 是 4 源条目合计，**不等于** news_aggregator_data 的行数（16 次采集快照）
    n_items = sum((api_mod.db.get_latest_news_aggregator() or {}).get(f"{k}_count") or 0
                  for k in ("caijing", "eeo", "sina_live", "ths_hk"))
    assert by["news"]["count"] == n_items, (by["news"]["count"], n_items)


# ── L2 ─────────────────────────────────────────────────────────────────────
def test_l2_all_categories():
    for cat in CATS:
        r = client.get(f"/api/v1/catalog/{cat}")
        assert r.status_code == 200, (cat, r.text[:200])
        d = r.json()
        for k in ("key", "name", "items", "total", "stale"):
            assert k in d, f"{cat} 缺 {k}"
        assert isinstance(d["items"], list)
        assert d["total"] >= len(d["items"]), f"{cat}: total={d['total']} items={len(d['items'])}"


def test_l2_indices():
    d = client.get("/api/v1/catalog/indices").json()
    assert d["total"] == 14, d["total"]
    assert len(d["items"]) == 14
    for it in d["items"]:
        assert it["symbol"].startswith(("sh", "sz")), it
        assert it["name"] and it["name"] != it["symbol"], it   # 名字来自 index_data.name
        assert it["last"] is not None
        # ⚠️ 指数必须走去重后的 bars：data_as_of 应是**真实交易日**，
        #    不应是周末（上游在周末也写行）
        import datetime
        wd = datetime.date.fromisoformat(it["data_as_of"]).weekday()
        assert wd < 5, f"{it['symbol']} data_as_of 落在周末: {it['data_as_of']}"


def test_l2_lists_are_capped_but_report_total():
    d = client.get("/api/v1/catalog/fundflow").json()
    assert len(d["items"]) <= 50, len(d["items"])
    assert d["total"] > len(d["items"]), "资金流应有截断（total > items）"
    for it in d["items"]:
        assert it["rank"] is not None and it["name"], it


def test_l2_fundflow_total_history_is_distinct_symbol_count():
    """「数字诚实」契约：L2 必须同时给出历史累计 total_history = DISTINCT symbol。

    L1 的 count 只报本批条数（前端只展示 50 条，报 2006 是欺骗性 UI）；
    历史累计不能被悄悄丢掉，否则用户无法知道库里一共有多少标的。
    """
    db = api_mod.db
    d = client.get("/api/v1/catalog/fundflow").json()
    n_hist = db.conn.execute(
        "SELECT count(DISTINCT symbol) FROM fund_flow_data WHERE flow_type='main_fund_rank'"
    ).fetchone()[0]
    assert d.get("total_history") == n_hist, (d.get("total_history"), n_hist)
    # 本批条数 ≤ 历史累计（不能反超）
    assert d["total"] <= n_hist, (d["total"], n_hist)
    # L1 count 与 L2 total 同口径（都是本批条数）
    c = client.get("/api/v1/catalog").json()
    by = {x["key"]: x for x in c["categories"]}
    assert by["fundflow"]["count"] == d["total"], (by["fundflow"]["count"], d["total"])


def test_l2_news_sources():
    d = client.get("/api/v1/catalog/news").json()
    assert set(d["sources"]) == {"caijing", "eeo", "sina_live", "ths_hk"}, d["sources"]
    assert d["sources"]["caijing"]["status"] == "empty"      # 必须如实上报
    assert len(d["items"]) <= 20
    ts = [i["ts"] for i in d["items"]]
    assert ts == sorted(ts, reverse=True), ts[:3]


# ── L3 ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("sym,kind", [
    ("sh000300", "index"),
    ("002202", "stock"),
    ("008114", "fund"),
])
def test_detail_auto_kind(sym, kind):
    r = client.get("/api/v1/detail", params={"symbol": sym})
    assert r.status_code == 200, r.text[:300]
    d = r.json()
    assert d["kind"] == kind, (sym, d["kind"])
    assert d["name"] and d["name"] != sym, d          # 名字解析成功
    assert d["quote"] and d["quote"]["last"] is not None
    ind = d["indicators"]
    assert ind["bars"] >= 20, ind["bars"]
    assert ind["ma20"] is not None
    assert 0 <= ind["rsi6"] <= 100
    assert "news" in d and "announcements" in d


def test_detail_index_uses_deduped_bars():
    """指数详情的 bars 必须是**去重后**的数量（远少于表内行数）。"""
    d = client.get("/api/v1/detail", params={"symbol": "sh000300"}).json()
    n_rows = api_mod.db.conn.execute(
        "SELECT count(*) FROM index_data WHERE code='sh000300'").fetchone()[0]
    assert d["indicators"]["bars"] < n_rows, (d["indicators"]["bars"], n_rows)
    assert d["indicators"]["bars"] >= 24          # 够算 RSI24


def test_detail_stock_has_announcements():
    d = client.get("/api/v1/detail", params={"symbol": "002202"}).json()
    assert len(d["announcements"]) > 0, "002202 应有公告数据"


def test_detail_errors():
    assert client.get("/api/v1/detail", params={"symbol": "../etc/passwd"}).status_code == 422
    assert client.get("/api/v1/detail", params={"symbol": "999999ZZZZ"}).status_code == 404
    assert client.get("/api/v1/catalog/nosuch").status_code == 404


# ── 名字解析 ───────────────────────────────────────────────────────────────
def test_hk_names_parsed_from_subscriptions():
    """港股名字在库里只有 subscriptions.name 这一个来源，必须能解析出来。"""
    for sym, expect in (("hk03690", "美团"), ("hk09988", "阿里")):
        name = api_mod._resolve_name(sym, "stock")
        assert expect in name, f"{sym} 解析出 {name!r}，期望含 {expect!r}"


def test_parse_subscription_name_variants():
    """格式很脏，逐个钉住（2026-11-01 实测样本）。"""
    cases = [
        ("002202", "002202金风科技(综合)", "金风科技"),
        ("002202", "002202金风科技基本面", "金风科技"),
        ("002202", "002202金风-announcement", "金风"),
        ("hk03690", "hk03690_美团_行情", "美团"),
        ("hk09988", "hk09988_阿里_行情", "阿里"),
        ("hk03690", "hk03690-hkex-announcement", None),   # 剥完为空 → None（回退代码）
        ("008114", "008114天弘红利低波动", "天弘红利低波动"),
    ]
    for sym, raw, expect in cases:
        got = api_mod._parse_subscription_name(sym, raw)
        assert got == expect, f"{raw!r} → {got!r}，期望 {expect!r}"


def test_name_overrides_used_as_fallback():
    """name_overrides.json 里的名字应能覆盖（011612 库里无 fund_name）。"""
    name = api_mod._resolve_name("011612", "fund")
    assert name == "华夏科创50", name
