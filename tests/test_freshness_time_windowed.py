"""Regression tests for the time_windowed exemption (2026-11-13).

背景：Hermes 报「eastmoney_em 成功率 12.3%」+「main_fund_rank 19/19 同一错误」，
用户转来要求确认。查下来是两件事叠在一起：

1) **口径错**（探针的锅）：
   eastmoney_em 登记为 time_windowed（东财收盘后对本机出口 IP 主动断连，
   资金流是日内数据，收盘后不采集是正确的）。但豁免逻辑用
   `in_trading_hours(now)` —— 判的是**探针跑的那一刻**，而指标统计的是
   **最近 24 小时**窗口。窗口里绝大多数失败发生在收盘后（本该豁免），
   只要探针恰好在盘中运行 → 整窗不豁免 → 把正常的盘后失败算成故障。
   实测：混入盘后失败后成功率被算成 12.3%，只看盘中原值是 38.9%。
   修法：**时段受限源只统计交易时段内的样本**，按**事件发生时刻**判定，
   而不是按探针运行时刻。

2) **真故障**（不能被豁免掩盖）：
   盘中成功率只有 38.9% → 东财确实在把我们往外推（11-10 还是 100%，
   11-11 82%、11-12 10%、11-13 3%，逐日恶化）。这条必须继续告警。

另外：重复错误指标在**受限之后**必须重新过最小次数门槛 —— 原来的 HAVING
是对全窗口计数做的，受限后可能只剩 1 次，若不重判就会把「1 次失败」
写成「100% 重复」。
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from market_service import market_data_freshness as f  # noqa: E402

TZ = timezone(timedelta(hours=8))


def _dt(s):
    return datetime.fromisoformat(s).replace(tzinfo=TZ)


def test_offhours_event_is_exempt_even_when_probe_runs_in_trading_hours():
    """窗口里盘后发生的失败 → 豁免，不管探针此刻在不在盘中。"""
    f._CALENDAR_CACHE["days"] = {"2026-11-12"}
    f._CALENDAR_CACHE["loaded_at"] = 1e18  # 防止真的去加载日历
    # 探针"现在"在交易时段（10:35）
    now = _dt("2026-11-12 10:35:00")
    # 事件发生在盘后（03:00）→ 属预期失败 → 豁免
    exempt, note = f.exception_applies(
        "eastmoney_em", "repeat_error_rate",
        event_ts="2026-11-12 03:00:00", now=now)
    assert exempt, "盘后失败应豁免（东财收盘后主动断连是预期行为）"
    assert "非交易时段" in note


def test_intraday_event_is_not_exempt():
    """盘中发生的失败 → 真故障，不豁免。"""
    f._CALENDAR_CACHE["days"] = {"2026-11-12"}
    f._CALENDAR_CACHE["loaded_at"] = 1e18
    now = _dt("2026-11-12 03:10:00")   # 探针此刻在盘后
    exempt, _ = f.exception_applies(
        "eastmoney_em", "repeat_error_rate",
        event_ts="2026-11-12 10:20:00", now=now)
    assert not exempt, "盘中失败是真故障，即使探针此刻在盘后也要告警"


def test_non_trading_day_event_is_exempt():
    """非交易日的失败 → 豁免（当天不开市）。"""
    f._CALENDAR_CACHE["days"] = {"2026-11-12"}   # 11-14 不在日历里
    f._CALENDAR_CACHE["loaded_at"] = 1e18
    exempt, _ = f.exception_applies(
        "eastmoney_em", "repeat_error_rate",
        event_ts="2026-11-14 10:00:00", now=_dt("2026-11-14 10:05:00"))
    assert exempt, "非交易日的失败应豁免"


def test_other_sources_unaffected():
    """非 time_windowed 的源，事件时刻不影响豁免结论。

    注：xueqiu 是 paused_by_decision（2026-10-26 人工暂停订阅）→ 始终豁免，
    是既有语义，不在本次改动范围。
    """
    f._CALENDAR_CACHE["days"] = {"2026-11-12"}
    f._CALENDAR_CACHE["loaded_at"] = 1e18
    for src in ("sina", "akshare", "rss", "zhihu"):
        exempt, _ = f.exception_applies(
            src, "per_source_failure_rate",
            event_ts="2026-11-12 03:00:00", now=_dt("2026-11-12 10:00:00"))
        assert not exempt, f"{src} 不该被豁免"


def test_paused_by_decision_still_always_exempt():
    """paused_by_decision（xueqiu 人工暂停）→ 始终豁免，语义不变。"""
    f._CALENDAR_CACHE["days"] = {"2026-11-12"}
    f._CALENDAR_CACHE["loaded_at"] = 1e18
    exempt, note = f.exception_applies(
        "xueqiu", "per_source_failure_rate",
        event_ts="2026-11-12 10:00:00", now=_dt("2026-11-12 10:00:00"))
    assert exempt, "人工暂停的源应始终豁免"
    assert "已知暂停" in note


def test_event_in_trading_hours_helper():
    """_event_in_trading_hours：按事件时刻判，且尊重交易日历。"""
    f._CALENDAR_CACHE["days"] = {"2026-11-12"}
    f._CALENDAR_CACHE["loaded_at"] = 1e18
    assert f._event_in_trading_hours("2026-11-12 10:00:00") is True
    assert f._event_in_trading_hours("2026-11-12 09:00:00") is False   # 早于 09:15
    assert f._event_in_trading_hours("2026-11-12 15:30:00") is False   # 晚于 15:05
    assert f._event_in_trading_hours("2026-11-12 11:30:00") is True    # 午休也算（探针口径 09:15-15:05）
    assert f._event_in_trading_hours("2026-11-14 10:00:00") is False   # 非交易日
    assert f._event_in_trading_hours(None) is False


def test_time_windowed_sample_threshold_is_lower():
    """时段受限源只统计盘中样本 → 样本天然更少 → 最小样本门槛单独放宽。"""
    assert f.PER_SOURCE_MIN_SAMPLES_TIME_WINDOWED < f.PER_SOURCE_MIN_SAMPLES
