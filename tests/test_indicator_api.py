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
    for k in ("symbol", "name", "data_as_of", "bars", "close",
              "ma", "macd", "rsi", "rsi_method", "levels"):
        assert k in d, f"缺字段 {k}"
    assert d["symbol"] == "002202"
    assert d["bars"] >= 20
    assert d["data_as_of"]
    assert d["rsi_method"] == "wilder", "RSI 口径必须显式标注（看板要显示）"
    for k in ("ma5", "ma20", "bias_ma5_pct"):
        assert k in d["ma"]
    for k in ("dif", "dea", "bar", "death_cross", "golden_cross"):
        assert k in d["macd"]
    for k in ("rsi6", "rsi12", "rsi24"):
        assert k in d["rsi"]
    for k in ("high_20d", "low_20d", "at_20d_high", "at_20d_low"):
        assert k in d["levels"]


def test_indicator_matches_shared_lib_golden():
    """与 golden oracle 对齐（golden 是迁移前 dsa-mcp 在真实日线上的输出）。"""
    golden_path = Path("/home/zsd/codes/fin-indicators/tests/fixtures/golden_oracle.json")
    if not golden_path.exists():
        pytest.skip("golden fixture 不存在")
    g = json.load(open(golden_path, encoding="utf-8"))["oracle"]["checker"]
    d = client.get("/api/v1/indicator", params={"symbol": "002202"}).json()
    # 端点做了 round(...,3)/(...,4)，用 golden 的完整值四舍五入后比对
    assert abs(d["ma"]["ma5"] - float(g["ma5"])) < 5e-4
    assert abs(d["ma"]["ma20"] - float(g["ma20"])) < 5e-4
    assert abs(d["macd"]["dif"] - float(g["dif_last"])) < 5e-5
    assert abs(d["macd"]["dea"] - float(g["dea_last"])) < 5e-5
    assert abs(d["macd"]["bar"] - float(g["bar_last"])) < 5e-5
    assert d["macd"]["death_cross"] == g["macd_death_cross"]


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
