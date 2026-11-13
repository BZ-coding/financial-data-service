"""板块资金流同花顺兜底（2026-11-13）。

背景：东财 push2delay 对本机出口 IP 的封禁逐日恶化（实测成功率
11-10 100% → 11-11 82% → 11-12 10% → 11-13 3%，独立进程复测 12 次只成功 3 次），
板块资金流因此长期采不到数据（fund_flow_data 最近一条是 11-12 08:50）。

兜底：akshare 的同花顺口径 stock_fund_flow_industry / stock_fund_flow_concept
**不受东财封禁影响**（实测行业 90 条 0.5s、概念 387 条 1.0s）。

本测试不联网：用假的 akshare 注入，验证
  1) 字段映射正确（净额 亿元 → 元；涨跌幅百分数原样）
  2) 同花顺异常时返回 None（不抛异常）
  3) collect() 在东财失败时走兜底并标 success
"""
import asyncio
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from market_service.collectors.community_enhanced import CommunityEnhancedCollector  # noqa: E402


def _df():
    return pd.DataFrame([
        {"序号": 1, "行业": "影视院线", "行业指数": 1760.62, "行业-涨跌幅": 4.61,
         "流入资金": 49.60, "流出资金": 45.74, "净额": 3.86, "公司家数": 20,
         "领涨股": "幸福蓝海", "领涨股-涨跌幅": 16.61, "当前价": 17.48},
    ])


def test_ths_mapping_converts_yi_yuan_to_yuan(monkeypatch):
    import akshare as ak
    monkeypatch.setattr(ak, "stock_fund_flow_industry", lambda symbol="即时": _df())
    items = CommunityEnhancedCollector()._fetch_sector_fund_flow_ths("industry", top_n=10)
    assert items, "兜底应返回数据"
    it = items[0]
    assert it["name"] == "影视院线"
    assert it["change_pct"] == pytest.approx(4.61)
    # 净额 3.86 亿元 → 3.86e8 元（与东财 f62 同量纲）
    assert it["main_net_inflow"] == pytest.approx(3.86e8)
    assert it["leading_stock"] == "幸福蓝海"
    assert it["provider"] == "ths"
    # 同花顺不提供主力净流入占比 → 留空而不是编造
    assert it["main_net_pct"] is None


def test_ths_failure_returns_none_not_exception(monkeypatch):
    import akshare as ak

    def _boom(symbol="即时"):
        raise AttributeError("'NoneType' object has no attribute 'text'")
    monkeypatch.setattr(ak, "stock_fund_flow_industry", _boom)
    # 兜底失败必须返回 None（由调用方判失败），不能抛
    assert CommunityEnhancedCollector()._fetch_sector_fund_flow_ths("industry") is None


def test_collect_falls_back_to_ths_when_eastmoney_fails(monkeypatch):
    """东财失败 → 同花顺兜底 → collect() 仍成功。"""
    import akshare as ak
    monkeypatch.setattr(ak, "stock_fund_flow_industry", lambda symbol="即时": _df())
    c = CommunityEnhancedCollector()
    # 让东财那条路直接失败
    monkeypatch.setattr(c, "_fetch_sector_fund_flow", lambda kind, top_n: None)
    res = asyncio.run(c.collect(data_type="sector_fund_flow", symbol="__sector__",
                                kind="industry", top_n=10))
    assert res.success, "有同花顺兜底时不应判采集失败"
    assert res.data and len(res.data) == 1
    assert res.data[0]["flow_type"] == "sector_fund_flow"


def test_collect_fails_when_both_sources_fail(monkeypatch):
    """两个源都失败 → 判失败（诚实上报，不假装有数据）。"""
    import akshare as ak

    def _boom(symbol="即时"):
        raise AttributeError("boom")
    monkeypatch.setattr(ak, "stock_fund_flow_industry", _boom)
    c = CommunityEnhancedCollector()
    monkeypatch.setattr(c, "_fetch_sector_fund_flow", lambda kind, top_n: None)
    res = asyncio.run(c.collect(data_type="sector_fund_flow", symbol="__sector__",
                                kind="industry", top_n=10))
    assert not res.success
    assert "兜底" in (res.error or "")
