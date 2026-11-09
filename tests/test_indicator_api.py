#!/usr/bin/env python3
"""指标端点单测（/api/v1/indicator）

覆盖：
  · 正常返回的结构与字段完整性
  · 数值与共享库 golden 一致（ma5/ma20/macd）
  · symbol 校验（后端自己的防线）：非法格式 → 422
  · 无数据 → 404
  · rsi_method 字段存在且为 wilder（口径标注，前端要显示）
  · data_as_of / bars 存在（新鲜度与样本量，前端要显示）
  · EMA 预热依赖：不同 days 会得到不同 MACD —— 记录该特性，防止被当成 bug

用法：./market_service/venv/bin/python3.14 -m pytest tests/test_indicator_api.py -q
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from market_service.api import app  # noqa: E402

client = TestClient(app)


def test_indicator_ok():
    r = client.get("/api/v1/indicator", params={"symbol": "002202"})
    assert r.status_code == 200, r.text
    d = r.json()
    for k in ("symbol", "data_as_of", "bars", "close",
              "ma", "macd", "rsi", "rsi_method", "levels"):
        assert k in d, f"缺字段 {k}"
    assert d["symbol"] == "002202"
    assert d["bars"] >= 20
    assert d["data_as_of"]
    assert d["rsi_method"] == "wilder", "RSI 口径必须显式标注（看板要显示）"
    # N1（2026-10-31）：不返回 name —— 库里无可靠名字源，null 是噪音；
    # MCP 工具 get_indicators 也不带该键，两处必须一致。
    assert "name" not in d, "不应返回 name 键（与 MCP 输出保持一致）"
    for k in ("ma5", "ma20", "bias_ma5_pct"):
        assert k in d["ma"]
    for k in ("dif", "dea", "bar", "death_cross", "golden_cross"):
        assert k in d["macd"]
    for k in ("rsi6", "rsi12", "rsi24"):
        assert k in d["rsi"]
    for k in ("high_20d", "low_20d", "at_20d_high", "at_20d_low"):
        assert k in d["levels"]


def test_indicator_matches_library_on_same_live_data():
    """端点输出必须等于「用同一份 N 库数据现算」的结果。

    ⚠️ **不要拿实时端点去比冻结的 golden fixture**（2026-11-01 修正）：
       端点取的是**滑动 60 天窗口**，随着时间推移最老的一天会滑出窗口
       （实测 002202 从 56 根变成 55 根），而 MACD/RSI 这类 EMA 指标
       依赖预热长度 → 数值会跟着变 0.005 左右 → 冻结 fixture 比对标必然假失败。
       冻结 golden 的正确位置是 fin-indicators 的纯库测试
       （tests/test_golden_snapshot.py，喂固定 fixture 数据）。

    本测试改为「同源数据现算」：端点 vs 库函数，喂**同一份** price_history，
    这样只校验"端点实现没写错"，不受数据漂移影响。
    """
    import pandas as pd
    sys.path.insert(0, "/home/zsd/codes/fin-indicators/src")
    import fin_indicators as fi
    from market_service import api as api_mod

    d = client.get("/api/v1/indicator", params={"symbol": "002202"}).json()

    # 用端点**同一个窗口**（60 天）取同一份数据，现算一遍
    items = api_mod.db.get_price_history("002202", 60)
    bars = fi.bars_from_intraday(items)
    assert len(bars) == d["bars"], f"bars 不一致: 库 {len(bars)} vs 端点 {d['bars']}"

    df = pd.DataFrame(bars)
    ma = fi.ma_series(df, (5, 20))
    macd = fi.macd_series(df)
    assert abs(d["ma"]["ma5"] - round(float(ma["MA5"].iloc[-1]), 3)) < 1e-9
    assert abs(d["ma"]["ma20"] - round(float(ma["MA20"].iloc[-1]), 3)) < 1e-9
    assert abs(d["macd"]["dif"] - round(float(macd["MACD_DIF"].iloc[-1]), 4)) < 1e-9
    assert abs(d["macd"]["dea"] - round(float(macd["MACD_DEA"].iloc[-1]), 4)) < 1e-9
    assert abs(d["macd"]["bar"] - round(float(macd["MACD_BAR"].iloc[-1]), 4)) < 1e-9
    assert abs(d["rsi"]["rsi6"] - round(float(fi.rsi_wilder(df, 6).iloc[-1]), 2)) < 1e-9
    assert abs(d["rsi"]["rsi12"] - round(float(fi.rsi_wilder(df, 12).iloc[-1]), 2)) < 1e-9
    assert abs(d["rsi"]["rsi24"] - round(float(fi.rsi_wilder(df, 24).iloc[-1]), 2)) < 1e-9
    hl = fi.high_low_n(df, df, n=20)
    assert d["levels"]["high_20d"] == round(hl["high"], 3)
    assert d["levels"]["low_20d"] == round(hl["low"], 3)


@pytest.mark.parametrize("bad", ["", "002202;DROP", "../../etc/passwd",
                                 "a" * 17, "002202/../x", "中文代码", "00 2202"])
def test_symbol_validation_422(bad):
    r = client.get("/api/v1/indicator", params={"symbol": bad})
    assert r.status_code == 422, f"symbol={bad!r} 应 422，实际 {r.status_code}"


def test_no_data_404():
    r = client.get("/api/v1/indicator", params={"symbol": "999999ZZZ"})
    assert r.status_code == 404, r.text


def test_days_bounds():
    assert client.get("/api/v1/indicator",
                      params={"symbol": "002202", "days": 19}).status_code == 422
    assert client.get("/api/v1/indicator",
                      params={"symbol": "002202", "days": 251}).status_code == 422


def test_ema_warmup_dependency_documented():
    """EMA 类指标依赖预热长度：不同 days → MACD 不同。

    这不是 bug，但**看板必须固定 days**，否则用户会看到"同一个标的数字变了"。
    本测试把该特性钉住，防止有人以为可以随便调 days。
    """
    a = client.get("/api/v1/indicator", params={"symbol": "002202", "days": 60}).json()
    b = client.get("/api/v1/indicator", params={"symbol": "002202", "days": 30}).json()
    assert a["macd"]["dif"] != b["macd"]["dif"], "若相等，说明该测试的前提失效，请复核"
    # MA 类不依赖预热（只要够周期），两者应一致
    assert abs(a["ma"]["ma5"] - b["ma"]["ma5"]) < 5e-4
    assert abs(a["ma"]["ma20"] - b["ma"]["ma20"]) < 1e-2
