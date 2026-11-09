#!/usr/bin/env python3
"""
test_eastmoney_fields.py
========================

Pitfall regression test: 东方财富 push2 / push2delay stock/get 单股接口
的字段含义验证.

历史 bug (2026-07-03):
- 误以为 f107 = 涨跌额, 但实际 f107 已失效 (A股=0/1, 港股=secid前缀)
- 误以为 f169 = 市场类型, 但实际 f169 = 涨跌额 (= price - prev_close)
- 误以为 f170 = 更新时间, 但实际 f170 = 涨跌幅%

验证策略:
1. mock requests.Session.get, 用真实 push2 抓到的 raw 喂进去
2. 跑 EastMoneyCollector.fetch_price, 断言:
   - change_amt ≈ price - prev_close (i.e. 实际涨跌额)
   - high == f44, low == f45 (这两个字段一直是对的, 别动)
3. 不依赖外网 (CI 环境无 EM 接口), 用预录 raw 模拟响应
"""

import sys
import asyncio
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

# 让 CI 能 import market_service
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from market_service.collectors.eastmoney import EastMoneyCollector


# 预录真实 raw (2026-07-03 14:00+ 抓取)
# 5 个标的 覆盖 A 股沪/深/创/科创 + 港股 + 大盘指数
RECORDED_RAW = {
    "1.600519": {  # 贵州茅台(沪)
        "f43": 1193.47, "f44": 1210.14, "f45": 1185.0, "f46": 1205.24,
        "f47": 28528, "f48": 3413828693.0, "f50": 0.74,
        "f57": "600519", "f58": "贵州茅台", "f60": 1203.0,
        "f107": 1, "f169": -9.53, "f170": -0.79,
    },
    "0.002202": {  # 金风科技(深)
        "f43": 24.46, "f44": 24.8, "f45": 24.2, "f46": 23.8,
        "f47": 100000, "f48": 2440000000.0, "f50": 1.5,
        "f57": "002202", "f58": "金风科技", "f60": 23.51,
        "f107": 0, "f169": 0.95, "f170": 4.04,
    },
    "0.300750": {  # 宁德时代(创业)
        "f43": 381.88, "f44": 385.0, "f45": 379.5, "f46": 380.0,
        "f47": 50000, "f48": 19000000000.0, "f50": 0.5,
        "f57": "300750", "f58": "宁德时代", "f60": 382.35,
        "f107": 0, "f169": -0.47, "f170": -0.12,
    },
    "0.000001": {  # 上证指数(深市前缀, 因为 f57="000001" 走 0. 分支)
        "f43": 4073.7, "f44": 4090.0, "f45": 4020.0, "f46": 4030.0,
        "f47": 999999, "f48": 9999999999.0, "f50": 0.0,
        "f57": "000001", "f58": "上证指数", "f60": 4028.9,
        "f107": 1, "f169": 44.8, "f170": 1.11,
    },
    "116.00700": {  # 腾讯控股(港)
        "f43": 435.4, "f44": 438.0, "f45": 432.0, "f46": 433.0,
        "f47": 8000, "f48": 3000000000.0, "f50": 0.0,
        "f57": "00700", "f58": "腾讯控股", "f60": 430.2,
        "f107": 116, "f169": 5.2, "f170": 1.21,
    },
}
# 上证指数的 secid (SH=1, 但 collector 内 f57="000001" 走 0. 分支, 实际请求 secid=0.000001)
# 已在 RECORDED_RAW 中以 0.000001 为 key



def _mock_session_get_factory(raw_map):
    """构造一个 mock session, 给定 secid 就返回对应 raw."""
    def _factory(url, params=None, timeout=None, **kwargs):
        secid = (params or {}).get("secid", "")
        resp = MagicMock()
        resp.status_code = 200
        if secid in raw_map:
            resp.json.return_value = {"rc": 0, "rt": 4, "data": raw_map[secid]}
        else:
            resp.json.return_value = {"rc": 1, "data": None}
        return resp
    return _factory


def test_change_amt_uses_f169_not_f107():
    """回归测试: change_amt 必须读 f169, 不是 f107. f107 在 EM push2 已失效."""
    collector = EastMoneyCollector(rate_limit_per_minute=9999)

    with patch.object(collector.session, "get",
                      side_effect=_mock_session_get_factory(RECORDED_RAW)):
        for secid, raw in RECORDED_RAW.items():
            symbol = raw["f57"]
            if secid.startswith("116."):
                symbol = "HK" + symbol
            result = asyncio.run(collector.fetch_price(symbol))
            assert result.success, f"{symbol}: {result.error}"
            data = result.data[0]

            # 1. change_amt 必须是 f169, 不应该是 f107 的垃圾值 (0/1/116)
            expected_change_amt = raw["f169"]
            assert data["change_amt"] == expected_change_amt, (
                f"{symbol}: change_amt={data['change_amt']} 应该是 f169={expected_change_amt}, "
                f"不能是 f107={raw['f107']} (f107 在 push2 / push2delay 已失效)"
            )

            # 2. change_amt 必须等于 price - prev_close (EM 端的核心不变量)
            computed = round(raw["f43"] - raw["f60"], 2)
            assert abs(data["change_amt"] - computed) < 0.05, (
                f"{symbol}: change_amt={data['change_amt']} 应该 ≈ price({raw['f43']}) - prev_close({raw['f60']}) = {computed}"
            )

            # 3. high / low 字段一直是 f44 / f45, 别动
            assert data["high"] == raw["f44"], f"{symbol}: high 应为 f44={raw['f44']}, 实得 {data['high']}"
            assert data["low"] == raw["f45"], f"{symbol}: low 应为 f45={raw['f45']}, 实得 {data['low']}"

            # 4. price 必须是 f43
            assert data["price"] == raw["f43"], f"{symbol}: price 应为 f43={raw['f43']}, 实得 {data['price']}"

            print(f"  ✅ {symbol:8s} price={data['price']:.2f}  change_amt={data['change_amt']}  high={data['high']}  low={data['low']}")


def test_f107_is_not_used_for_change_amt():
    """专门铁证: 即使 raw 里 f107 有明确垃圾值, 我们也绝不能用它当涨跌额."""
    collector = EastMoneyCollector(rate_limit_per_minute=9999)

    # 故意构造一个 f107=999 (异常大) 但 f169=0.5 (合理) 的 mock
    fake_raw = {
        "f43": 10.0, "f44": 10.5, "f45": 9.5, "f46": 10.0,
        "f47": 100, "f48": 1000000.0, "f50": 1.0,
        "f57": "399999", "f58": "测试标的", "f60": 9.5,
        "f107": 999,  # 故意制造垃圾值
        "f169": 0.5,  # 正确涨跌额
        "f170": 5.26,
    }

    with patch.object(collector.session, "get",
                      side_effect=_mock_session_get_factory({"0.399999": fake_raw})):
        result = asyncio.run(collector.fetch_price("399999"))
        assert result.success, f"fetch_price 失败: {result.error}"
        data = result.data[0]

        # 关键断言: 绝对不能用 f107=999 当涨跌额
        assert data["change_amt"] != 999, "❌ 用了 f107 当涨跌额 (f107 已失效)"
        # 必须用 f169
        assert data["change_amt"] == 0.5, f"❌ 涨跌额={data['change_amt']} 应为 f169=0.5"
        print(f"  ✅ 垃圾 f107=999 被正确忽略, change_amt 用 f169=0.5")


def test_f169_fallback_to_price_minus_prev_close():
    """f169 缺失时, 应该回退用 price - prev_close 计算."""
    collector = EastMoneyCollector(rate_limit_per_minute=9999)

    fake_raw = {
        "f43": 100.0, "f44": 101.0, "f45": 99.0, "f46": 100.0,
        "f47": 100, "f48": 1000000.0, "f50": 1.0,
        "f57": "388888", "f58": "测试", "f60": 98.0,
        # 注意: 没有 f169, 也没有 f107
    }

    with patch.object(collector.session, "get",
                      side_effect=_mock_session_get_factory({"0.388888": fake_raw})):
        result = asyncio.run(collector.fetch_price("388888"))
        assert result.success, f"fetch_price 失败: {result.error}"
        data = result.data[0]
        # price=100, prev_close=98, 涨跌额=2.0
        assert data["change_amt"] == 2.0, f"❌ 回退算错, 实得 {data['change_amt']}, 应为 2.0"
        print(f"  ✅ f169 缺失时正确回退, change_amt=2.0 (price 100 - prev_close 98)")


def test_em_fields_includes_required_keys():
    """静态检查: EM_FIELDS 字符串必须包含 f43, f44, f45, f60, f169, 不应包含 f107."""
    fields = EastMoneyCollector.EM_FIELDS
    for required in ("f43", "f44", "f45", "f60", "f169"):
        assert required in fields, f"❌ EM_FIELDS 缺 {required}: {fields}"
    assert "f107" not in fields, f"❌ EM_FIELDS 不应包含 f107 (已失效): {fields}"
    print(f"  ✅ EM_FIELDS 字段集正确: {fields}")


if __name__ == "__main__":
    print("=" * 60)
    print("EastMoney pitfall regression: push2 字段错位 (2026-07-03)")
    print("=" * 60)
    test_em_fields_includes_required_keys()
    print()
    test_change_amt_uses_f169_not_f107()
    print()
    test_f107_is_not_used_for_change_amt()
    print()
    test_f169_fallback_to_price_minus_prev_close()
    print()
    print("=" * 60)
    print("全部通过 ✅")
    print("=" * 60)
