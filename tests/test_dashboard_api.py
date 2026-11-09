#!/usr/bin/env python3
"""监控页聚合端点单测（/api/v1/dashboard，阶段 1）

覆盖：
  · 结构完整性（quotes / indicators / news / health / as_of / watchlist_size）
  · 清单来自 watchlist.json（8 只：3 股票 + 5 基金）
  · 股票与基金语义不同：基金涨跌幅标 estimated=True；基金无 high/low（新高新低不适用）
  · 数据新鲜度：stale / stale_days 存在；011612 数据过期 → stale=True
  · 新闻：只 4 个中文源、caijing empty 要如实上报、时间倒序、默认 ≤20 条
  · 健康：成功率 / 24h 失败数 / 降级源
  · 缓存：第二次请求 cached=True 且更快
  · **数值一致性**：股票指标与 /api/v1/indicator 逐项相等（同口径，防止两处算法漂移）

用法：./market_service/venv/bin/python3.14 -m pytest tests/test_dashboard_api.py -q
"""
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from market_service import api as api_mod  # noqa: E402
from market_service.api import app  # noqa: E402

client = TestClient(app)

STOCK_SYMBOLS = {"002202", "hk03690", "hk09988"}
# 2026-11-01 起：清单改为**数据库真实数据**（不再读 watchlist.json）
# 基金 = nav_data 里最新净值在 30 天内（排除陈年残留）
FUND_SYMBOLS = {"008114", "011612", "014086", "015716", "018824",
                "023895", "024450", "163807", "270023"}
# 陈年残留（最新净值远超 30 天）—— 必须被排除
JUNK_FUNDS = {"000001", "270002", "110011", "161725"}


@pytest.fixture(scope="module")
def dash():
    api_mod._DASH_CACHE.update({"stamp": None, "at": 0.0, "payload": None})
    r = client.get("/api/v1/dashboard")
    assert r.status_code == 200, r.text[:400]
    return r.json()


def test_top_level_structure(dash):
    for k in ("as_of", "watchlist_size", "quotes", "indicators", "news", "health",
              "cached", "cache_age_sec"):
        assert k in dash, f"缺字段 {k}"
    # 数量按库实查（3 股票 + 9 基金 = 12），不再硬编码 8
    assert dash["watchlist_size"] == len(dash["quotes"]) == len(dash["indicators"])
    assert dash["watchlist_size"] >= 12, dash["watchlist_size"]


def test_holdings_symbols(dash):
    """清单来自**数据库真实数据**：3 股票 + 9 基金；4 只陈年残留必须被排除。"""
    syms = {q["symbol"] for q in dash["quotes"]}
    assert STOCK_SYMBOLS <= syms, f"缺股票: {STOCK_SYMBOLS - syms}"
    assert FUND_SYMBOLS <= syms, f"缺基金: {FUND_SYMBOLS - syms}"
    missing = JUNK_FUNDS & syms
    assert not missing, f"陈年残留基金未被排除: {missing}"
    assert "SGXZ99103178" not in syms          # 库里零数据
    # 名字必须解析出来（库里多级回退；港股靠解析 subscriptions.name）
    for q in dash["quotes"]:
        assert q["name"] and q["name"] != q["symbol"], q


def test_quote_fields_and_semantics(dash):
    for q in dash["quotes"]:
        for k in ("symbol", "name", "kind", "last", "change_pct",
                  "estimated", "data_as_of", "stale", "stale_days"):
            assert k in q, f"{q['symbol']} 缺 {k}"
        assert q["kind"] in ("stock", "fund")
        if q["kind"] == "fund":
            # 基金"最新价"= 盘中估值或 T+1 净值 → 必须与股票区分
            assert isinstance(q["estimated"], bool)
        else:
            assert q["estimated"] is False
        # 数据时间必须是 ISO 破折号形式（不能出现 2026/10/30 这种斜杠）
        if q["data_as_of"]:
            assert "/" not in q["data_as_of"], q["data_as_of"]


def test_stale_flag_for_stale_symbol(dash):
    """011612 只有 20 条 nav 且 2026-10-27 后未更新 → 必须标 stale。"""
    q = {x["symbol"]: x for x in dash["quotes"]}
    if "011612" in q and q["011612"]["data_as_of"]:
        assert q["011612"]["stale"] is True, q["011612"]
        assert (q["011612"]["stale_days"] or 0) >= 4, q["011612"]


def test_indicators_stock_vs_fund(dash):
    by = {i["symbol"]: i for i in dash["indicators"]}
    for sym in STOCK_SYMBOLS:
        a = by[sym]
        assert a["bars"] >= 20, (sym, a["bars"])
        assert a["ma5"] is not None and a["ma20"] is not None
        assert a["rsi6"] is not None and 0 <= a["rsi6"] <= 100
        # 股票有 high/low
        assert a["high_20d"] is not None and a["low_20d"] is not None
        assert isinstance(a["at_20d_high"], bool)
    for sym in FUND_SYMBOLS:
        a = by[sym]
        # 基金：MA/MACD/RSI 可算（nav 适配），但**新高/新低不适用** → 必须是 None
        assert a["high_20d"] is None and a["low_20d"] is None, (sym, a)
        assert a["at_20d_high"] is None and a["at_20d_low"] is None
        if a["bars"] >= 20:
            assert a["ma20"] is not None


def test_news_block(dash):
    n = dash["news"]
    for k in ("items", "total", "limit", "sources"):
        assert k in n, f"news 缺 {k}"
    assert set(n["sources"]) == {"caijing", "eeo", "sina_live", "ths_hk"}, n["sources"]
    # caijing 当前 empty —— 必须如实上报，不能静默消失
    assert n["sources"]["caijing"]["status"] == "empty", n["sources"]["caijing"]
    assert len(n["items"]) <= n["limit"] == 20
    # 时间倒序
    ts = [it["ts"] for it in n["items"]]
    assert ts == sorted(ts, reverse=True), ts[:5]
    # 每条都有源与标题、时间可解析
    for it in n["items"]:
        assert it["source"] in n["sources"]
        assert it["title"]
        assert len(it["ts"]) == 19 and it["ts"][4] == "-"


def test_health_block(dash):
    h = dash["health"]
    for k in ("success_rate_24h", "failed_24h", "success_24h", "degraded_sources"):
        assert k in h, f"health 缺 {k}"
    assert isinstance(h["degraded_sources"], list)


def test_cache_hit_second_call():
    api_mod._DASH_CACHE.update({"stamp": None, "at": 0.0, "payload": None})
    t0 = time.time()
    r1 = client.get("/api/v1/dashboard")
    cold = time.time() - t0
    assert r1.status_code == 200
    assert r1.json()["cached"] is False

    t0 = time.time()
    r2 = client.get("/api/v1/dashboard")
    hot = time.time() - t0
    d2 = r2.json()
    assert d2["cached"] is True, d2.get("cached")
    assert d2["cache_age_sec"] >= 0
    # 缓存命中必须显著更快（放宽到 1/5，避免 CI 抖动）
    assert hot < max(cold / 5, 0.01), f"cold={cold:.3f}s hot={hot:.3f}s"
    # 缓存命中时内容一致（除缓存元字段）
    for k in ("watchlist_size", "quotes", "indicators", "news", "health"):
        assert d2[k] == r1.json()[k], f"缓存命中后 {k} 变了"


def test_consistency_with_single_symbol_endpoint(dash):
    """股票指标必须与 /api/v1/indicator **逐项相等**（同口径，防两处算法漂移）。"""
    by = {i["symbol"]: i for i in dash["indicators"]}
    for sym in sorted(STOCK_SYMBOLS):
        single = client.get("/api/v1/indicator", params={"symbol": sym}).json()
        a = by[sym]
        pairs = [
            ("ma5", a["ma5"], single["ma"]["ma5"]),
            ("ma20", a["ma20"], single["ma"]["ma20"]),
            ("bias", a["bias_ma5_pct"], single["ma"]["bias_ma5_pct"]),
            ("dif", a["macd"]["dif"], single["macd"]["dif"]),
            ("dea", a["macd"]["dea"], single["macd"]["dea"]),
            ("bar", a["macd"]["bar"], single["macd"]["bar"]),
            ("death_cross", a["macd"]["death_cross"], single["macd"]["death_cross"]),
            ("golden_cross", a["macd"]["golden_cross"], single["macd"]["golden_cross"]),
            ("rsi6", a["rsi6"], single["rsi"]["rsi6"]),
            ("rsi12", a["rsi12"], single["rsi"]["rsi12"]),
            ("rsi24", a["rsi24"], single["rsi"]["rsi24"]),
            ("high_20d", a["high_20d"], single["levels"]["high_20d"]),
            ("low_20d", a["low_20d"], single["levels"]["low_20d"]),
        ]
        bad = [(n, x, y) for n, x, y in pairs if x != y]
        assert not bad, f"{sym} 与单只端点不一致: {bad}"
