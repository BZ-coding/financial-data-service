#!/usr/bin/env python3
"""探针干跑观察期复核 —— 判断是否满足"可以开飞书推送"的条件

【为什么需要】评审纪律：**误报告警比没告警更糟**。
探针上线时如果直接开推送，一个没调好的判据就会往用户飞书里灌噪音，
久而久之用户就不看了 —— 探针等于不存在。

【观察期必须覆盖】（评审要求）
  1. 一个**完整交易日**（覆盖 09:15-15:05 行情时段）
  2. 一个**非交易日**（周末/节假日 —— 此时行情不更新是正常的）
  3. 一次**凌晨非交易时段**（00:00-09:15 收盘后时段）

【判据】
  - 观察期内探针**零误报**（没有任何一次把"正常"判成"异常"）
  - 覆盖了上述三类时段
  - 满足后提示可以切到推送模式（人工确认再切，不自动切）

【用法】
  python3 probe_dryrun_review.py            # 只看状态
  python3 probe_dryrun_review.py --apply    # 满足条件时自动切到推送模式
"""
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone

LOG = "/home/zsd/financial-data-service/logs/market-data-freshness.log"
DRY_FLAG_FILE = "/home/zsd/.config/systemd/user/market-freshness-probe.service.d/dry-run.conf"
UNIT = "/home/zsd/.config/systemd/user/market-freshness-probe.service"
CN_TZ = timezone(timedelta(hours=8))
NOW = datetime.now(CN_TZ)

# 观察期起点（判据最后一次变更时刻 —— 判据变了，之前的观察不再作数）
# 2026-10-26 22:27：加入按源分组失败率 + 重复错误率 + 已知暂停源识别
START = datetime(2026, 10, 26, 22, 27, 0, tzinfo=CN_TZ)


def in_trading_hours(dt):
    if dt.weekday() >= 5:
        return False
    m = dt.hour * 60 + dt.minute
    return (9 * 60 + 15) <= m <= (15 * 60 + 5)


# 交易日判定缓存：同一日期只查一次（否则每条日志都启动一次子进程 → 极慢）
_TRADING_DAY_CACHE = {}


def is_non_trading_day(dt):
    """复用 check_trading_day.py 判定（避免自己写一套），带缓存"""
    key = dt.strftime("%Y-%m-%d")
    if key in _TRADING_DAY_CACHE:
        return _TRADING_DAY_CACHE[key]
    s = "/home/zsd/.hermes/scripts/check_trading_day.py"
    try:
        out = subprocess.run([sys.executable, s, "--date", key],
                             capture_output=True, text=True, timeout=60).stdout.strip()
        res = out.startswith("HOLIDAY")
    except Exception:
        res = None
    _TRADING_DAY_CACHE[key] = res
    return res


def read_log():
    try:
        with open(LOG, encoding="utf-8", errors="ignore") as f:
            return f.readlines()
    except Exception:
        return []


def main():
    lines = read_log()
    # 只看观察期内的运行记录
    runs = []
    for ln in lines:
        m = re.match(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", ln)
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)
        except ValueError:
            continue
        if ts >= START:
            runs.append((ts, ln.rstrip()))

    print(f"观察期起点: {START:%Y-%m-%d %H:%M:%S}")
    print(f"当前时间:   {NOW:%Y-%m-%d %H:%M:%S}")
    print(f"观察期时长: {(NOW - START).total_seconds() / 3600:.1f} 小时")

    # 统计（只认"运行结束"的判定标记行，明细行不算运行次数）
    verdict_norm = [r for r in runs if "=== 全部指标正常 ===" in r[1]]
    verdict_abn = [r for r in runs if "=== 检测到" in r[1] and "个问题 ===" in r[1]]
    dry = [r for r in runs if "DRY-RUN" in r[1]]
    total_runs = len(verdict_norm) + len(verdict_abn)
    false_alarms = verdict_abn
    normal = verdict_norm
    print(f"\n运行次数: {total_runs}  |  判定正常: {len(normal)}  |  "
          f"判定异常: {len(false_alarms)}  | 干跑模式告警: {len(dry)}")
    print(f"  (日志明细行共 {len(runs)} 行，不计入运行次数)")

    # 覆盖的三类时段
    cover = {
        "完整交易日(09:15-15:05)": False,
        "非交易日": False,
        "凌晨非交易时段(00:00-09:15)": False,
    }
    for ts, _ in runs:
        if in_trading_hours(ts):
            cover["完整交易日(09:15-15:05)"] = True
        elif ts.hour < 9:
            cover["凌晨非交易时段(00:00-09:15)"] = True
        else:
            nd = is_non_trading_day(ts)
            if nd is True:
                cover["非交易日"] = True
            elif nd is False and ts.hour >= 15:
                cover["完整交易日(09:15-15:05)"] = True

    print("\n时段覆盖:")
    for k, v in cover.items():
        print(f"  {'✅' if v else '⏳'} {k}")

    ready = all(cover.values()) and not false_alarms and len(normal) >= 3
    print(f"\n结论: {'✅ 满足开推送条件' if ready else '⏳ 观察期未完成'}")

    if not ready:
        missing = [k for k, v in cover.items() if not v]
        if missing:
            print(f"  待覆盖: {', '.join(missing)}")
        if false_alarms:
            print(f"  ⚠️ 有 {len(false_alarms)} 次异常判定，需人工确认是否误报")
        return 1

    if "--apply" in sys.argv:
        os.makedirs(os.path.dirname(DRY_FLAG_FILE), exist_ok=True)
        with open(DRY_FLAG_FILE, "w") as f:
            f.write("[Service]\nEnvironment=PROBE_DRY_RUN=0\n")
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
        print("  → 已切到推送模式（PROBE_DRY_RUN=0），下次运行生效")
    else:
        print("  → 加 --apply 可自动切换到推送模式")
    return 0


if __name__ == "__main__":
    sys.exit(main())
