# 计划：搬 dSA 分析模块 + 双 MCP 仓 (v2)

**作者**: Hermes (zsd 协作)
**日期**: 2026-07-02
**状态**: 待 zsd 审阅
**前置文档**: `dSA-AUDIT.md` (已读完 dSA 全部相关源码)
**涉及**: `financial-data-service` (现有仓) + `dsa-mcp` (新仓)

---

## ⚠️ 全局铁律（zsd 明确，2026-07-02）

1. **数据全走 8084，不许依赖 dSA DB**
   - dsa-mcp 的所有 tool 需要数据时，**必须通过 8084 MCP 拿**（或 8084 REST）
   - dSA 的 `history_loader` / SQLite / DataFetcherManager **一律不依赖**
   - 改造原则: 把 dSA 函数签名 `func(stock_code)` 改成 `func(df: pd.DataFrame, ...)`，df 由调用方从 8084 拿
2. **预警 = 纯信号，不绑推送**
   - dsa-mcp 的 alert tool 只返回 `triggered / signals[]`，**不调任何 sender**
   - agent 拿到信号自己决定下一步（写报告 / 调别的 tool / 调用 Hermes 推）
   - 调度、冷却、推送、持久化全归调用方

**违反这两条任何一条的设计 = 立刻打回**。

## 0. 背景 & 审计结论摘要

完整审计见 `dSA-AUDIT.md`。TL;DR:

**能搬 (黄金)**:
- `StockTrendAnalyzer` (849 行综合算法，零外部状态)
- `registry.py` (80 行 dataclass + decorator)
- 3 个纯函数 tool (`calculate_ma` / `get_volume_analysis` / `analyze_pattern`)
- 6 个 agent 的 `system_prompt` (字符串，存 .md)
- 15 个 YAML 策略 (纯文本配置)

**不搬**:
- `data_tools.py` / `market_tools.py` (依赖 dSA fetcher)
- `backtest_tools.py` (读 dSA SQLite，无数据源)
- `search_tools.py` (依赖外部 API key)
- `notification_sender/*` (我们用 Hermes + feishu)
- dSA 的 fetcher / llm / orchestrator / agent class

**自己设计**:
- 预警 MCP (dSA 没有独立模块，全耦合在推送)

---

## 1. 仓库拓扑

```
financial-data-service/        (现有仓, 不改 ownership)
├─ market_service/             (现有 REST 服务, 保留)
│   ├─ api.py                  (新增 5 个接口)
│   └─ data_sources/
│       └─ akshare_market_structure.py  (新增: 板块/概念/统计/涨停池/龙虎榜)
├─ mcp_server/                 (新增: 镜像 REST 查询接口为 MCP)
│   ├─ server.py               (MCP 入口, 端口 8086)
│   ├─ tools_quote.py          (get_quote / get_kline / get_realtime_indices)
│   └─ tools_market.py         (新增的 5 个 REST 接口镜像)
└─ docs/
    ├─ PLAN-zsd-fin-modules.md (本文件)
    └─ dSA-AUDIT.md            (审计报告)

dsa-mcp/                       (新仓, 待建)
├─ README.md
├─ pyproject.toml              (依赖: mcp, pandas, numpy, pyyaml)
├─ UPSTREAM.md                 (记录与 dSA 同步点)
├─ src/
│   └─ dsa_mcp/
│       ├─ __init__.py
│       ├─ server.py           (MCP 入口, 端口 8087)
│       ├─ registry.py         (从 dSA 抄, 80 行)
│       ├─ analysis/
│       │   ├─ __init__.py
│       │   ├─ trend.py        (从 dSA src/stock_analyzer.py 抄, 改 1 行 import)
│       │   ├─ ma.py           (从 dSA analysis_tools 抄 calculate_ma)
│       │   ├─ volume.py       (从 dSA analysis_tools 抄 get_volume_analysis)
│       │   └─ pattern.py      (从 dSA analysis_tools 抄 analyze_pattern)
│       ├─ strategies/         (15 个 YAML 直接复制)
│       │   └─ *.yaml
│       ├─ prompts/            (6 个 agent prompt, .md 格式)
│       │   ├─ technical.md
│       │   ├─ intel.md
│       │   ├─ risk.md
│       │   ├─ portfolio.md
│       │   ├─ decision.md
│       │   └─ decision_chat.md
│       └─ alerts/             (自设计, dSA 没有)
│           ├─ __init__.py
│           ├─ checker.py      (规则引擎)
│           └─ rules.yaml      (10-15 条预警规则)
├─ tests/
│   ├─ test_trend.py           (StockTrendAnalyzer 单测)
│   ├─ test_ma.py
│   ├─ test_volume.py
│   ├─ test_pattern.py
│   ├─ test_strategies.py      (YAML 解析)
│   ├─ test_alerts.py
│   └─ test_mcp_server.py      (启动 + tools/list)
└─ docs/
    └─ PLAN-zsd-fin-modules.md (副本或链接)
```

---

## 2. Phase 拆解 (按审计结论调整)

### Phase 1: ✅ 源码审计 (已完成)

详见 `dSA-AUDIT.md`。

---

### Phase 2: financial-data-service 加 MCP 层 + REST 接口补全

**目标**: 现有 8084 REST 服务不动, 新增 MCP server 镜像查询接口 + **8 个新 REST 端点**（3 真新 + 5 包装）。

**预估工作量**: **2 天** (包装工作量类似 v2)

#### 2.1 8 个新 REST 端点（基于 8084-AUDIT 修正）

**A. 3 个真正需要新建的** (底层 collector / db 都没有):

```python
# akshare 数据源: stock_market_activity_legu / stock_zt_pool_em / stock_lhb_detail_em
GET /api/v1/data?data_type=market_stats          # 涨跌统计 (涨/跌/平/停家数)
GET /api/v1/data?data_type=limit_up_pool&n=20   # 涨停池 + 连板梯队
GET /api/v1/data?data_type=dragon_tiger&symbol=  # 龙虎榜 (单只)
```

**B. 5 个包装层 (db 方法已有, 只需暴露 REST)**:

```python
GET /api/v1/data?data_type=announcement&symbol=600519  # 公告 (180天)
GET /api/v1/data?data_type=community&symbol=600519     # 股吧 (30天)
GET /api/v1/data?data_type=fund_flow&flow_type=...     # 主力资金榜/板块资金流
GET /api/v1/news?symbol=600519&days=7                 # 按 symbol 查个股新闻
GET /api/v1/data?data_type=fundamental&symbol=600519   # PE/PB/市值
```

**实现细节**:
- 在 `market_service/api.py` 加 8 个 `if data_type == "..."` 分支
- 调对应 `db.get_*` 方法
- 3 个真新增的需要先加 akshare collector 函数 + db 存储

#### 2.2 新增 collector 函数（仅 3 个）

`market_service/collectors/akshare.py` 加:
```python
def _fetch_market_stats_sync() -> Optional[Dict]:
    """ak.stock_market_activity_legu() → 涨跌统计"""
    # 返回: {up_count, down_count, flat_count, limit_up_count, limit_down_count, total_amount}

def _fetch_limit_up_pool_sync(date: str = None, n: int = 20) -> Optional[List[Dict]]:
    """ak.stock_zt_pool_em(date=...) → 涨停池"""
    # 返回: [{code, name, price, change_pct, limit_times, first_limit_time, last_limit_time}, ...]

def _fetch_dragon_tiger_sync(symbol: str) -> Optional[List[Dict]]:
    """ak.stock_lhb_detail_em(symbol=...) → 龙虎榜"""
    # 返回: [{date, buyer_name, buyer_amount, seller_name, seller_amount, net_amount}, ...]
```

**新增 db 表** (schema.sql 升级):
```sql
CREATE TABLE IF NOT EXISTS market_stats_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE UNIQUE NOT NULL,
    up_count INTEGER, down_count INTEGER, flat_count INTEGER,
    limit_up_count INTEGER, limit_down_count INTEGER,
    total_amount REAL, raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS limit_up_pool_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    code TEXT NOT NULL, name TEXT, price REAL, change_pct REAL,
    limit_times INTEGER, first_limit_time TEXT, last_limit_time TEXT,
    raw_data TEXT, ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, code)
);

CREATE TABLE IF NOT EXISTS dragon_tiger_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    code TEXT NOT NULL, name TEXT,
    buyer_name TEXT, buyer_amount REAL,
    seller_name TEXT, seller_amount REAL,
    net_amount REAL, raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

`market_service/database.py` 加 3 个对应 db 方法:
- `save_market_stats(data)` / `get_latest_market_stats()`
- `save_limit_up_pool(data_list)` / `get_limit_up_pool(date)`
- `save_dragon_tiger(symbol, data)` / `get_dragon_tiger(symbol, days)`

#### 2.3 新增 MCP server

`mcp_server/server.py` - 入口, 端口 8086

`mcp_server/tools_quote.py` (4 个 tool):
```python
@app.tool()
def get_quote(symbol: str) -> dict:
    """实时行情 - 走 8084 REST /api/v1/data?data_type=price"""

@app.tool()
def get_kline(symbol: str, period: str = "daily", days: int = 60) -> list:
    """日K线 - 走 8084 db get_daily"""

@app.tool()
def get_realtime_indices() -> list:
    """主要指数实时 - 走 8084 REST /api/v1/data?data_type=index"""

@app.tool()
def get_fundamental(symbol: str) -> dict:
    """基本面 (PE/PB/市值) - 走 8084 REST /api/v1/data?data_type=fundamental (新增)"""
```

`mcp_server/tools_market.py` (8 个 tool):
```python
@app.tool()
def get_sector_rankings(n: int = 5) -> list:
    """板块涨跌榜 - 8084 db get_fund_flow(flow_type='sector_fund_flow', sector_kind='industry')"""

@app.tool()
def get_concept_rankings(n: int = 5) -> list:
    """概念涨跌榜 - 8084 db get_fund_flow(flow_type='sector_fund_flow', sector_kind='concept')"""

@app.tool()
def get_main_fund_rank(n: int = 20) -> list:
    """主力资金榜 - 8084 db get_fund_flow(flow_type='main_fund_rank')"""

@app.tool()
def get_market_stats() -> dict:
    """涨跌统计 - 8084 REST /api/v1/data?data_type=market_stats (新增)"""

@app.tool()
def get_limit_up_pool(n: int = 20) -> list:
    """涨停池 - 8084 REST /api/v1/data?data_type=limit_up_pool (新增)"""

@app.tool()
def get_dragon_tiger(symbol: str, days: int = 30) -> list:
    """龙虎榜 - 8084 REST /api/v1/data?data_type=dragon_tiger (新增)"""

@app.tool()
def get_news(symbol: str, days: int = 7, source: str = None) -> list:
    """按 symbol 查个股新闻 - 8084 REST /api/v1/news?symbol=&days= (新增包装)"""

@app.tool()
def get_announcements(symbol: str, days: int = 90) -> list:
    """公告查询 - 8084 db get_announcements (新增包装)"""
```

**合计 12 个 tool** (4 query + 8 market/news)

#### 2.4 验收

- [ ] 8 个新 REST 端点 curl 测试全过
- [ ] MCP server 启动, `tools/list` 返回 12 个 tool
- [ ] 每个 tool 至少 1 个 happy path 测试
- [ ] 8084 现有功能不破

#### 2.5 commit

拆 2 个 commit (新功能分两组):
1. `feat(api): 新增 announcement/community/fund_flow/news/fundamental 包装接口`
2. `feat(api): 新增 market_stats/limit_up_pool/dragon_tiger + collector + db 表`

---

### Phase 3: 新仓 dsa-mcp

**目标**: 把 dSA 的"分析算法 + 策略 + prompt + 自设计预警"做成独立 MCP server。

**预估工作量**: **2-3 天** (审计砍掉了 backtest/search, 工作量比 v1 少 2-3 天)

#### 3.1 仓库初始化 (0.5 天)

- [ ] GitHub 建仓 `zhangsheng377/dsa-mcp` (**公开**, 见 Q2)
- [ ] 克隆到 `/home/zsd/dsa-mcp/`
- [ ] `git remote add upstream https://github.com/ZhuLinsen/daily_stock_analysis.git`
- [ ] `pyproject.toml` 依赖: `mcp`, `pandas`, `numpy`, `pyyaml`
- [ ] `README.md` 注明与 dSA 的关系 (license = MIT, fork 性质)
- [ ] `UPSTREAM.md` 记录:
  ```
  # 同步记录
  - 2026-07-02: 基于 dSA @ 48b9e18a 初始化
  - 后续每月 diff upstream, 手动同步
  ```

#### 3.2 搬算法 (0.5 天)

- [ ] `registry.py` 整文件搬 → `src/dsa_mcp/registry.py`
  - 改: 文件路径 import
- [ ] `src/stock_analyzer.py` 整文件搬 → `src/dsa_mcp/analysis/trend.py`
  - 改: `from src.config import get_config` → 本地常量 `BIAS_THRESHOLD = 5.0`
  - 改: `from src.services.xxx` 删除 (如有)
- [ ] `analysis_tools.py` 拆:
  - `calculate_ma` (整函数搬) → `src/dsa_mcp/analysis/ma.py`
  - `get_volume_analysis` (整函数搬) → `src/dsa_mcp/analysis/volume.py`
  - `analyze_pattern` (整函数搬) → `src/dsa_mcp/analysis/pattern.py`
  - **`analyze_trend` 不搬** (依赖 history_loader, 见 3.3)
- [ ] **不搬**: backtest_tools / data_tools / market_tools / search_tools

#### 3.3 改造 `analyze_trend` tool (0.5 天)

**审计结论**: dSA 的 `analyze_trend` 调 `load_history_df(stock_code, days)` 拉数据再 `analyzer.analyze(df, code)`。

**铁律**: 不许依赖 dSA DB，df 全部由调用方从 8084 拿。

**改造方案**: 把 `load_history_df` 调用换成参数注入。

```python
# 新版 src/dsa_mcp/analysis/trend_tool.py
def analyze_trend(symbol: str, df: pd.DataFrame) -> dict:
    """
    symbol: 股票代码
    df: 历史 K 线 DataFrame (由调用方从 8084 MCP 拿)
    """
    if df is None or len(df) < 20:
        return {"error": f"Insufficient data for {symbol}"}
    analyzer = StockTrendAnalyzer()
    result = analyzer.analyze(df, symbol)
    return result_to_dict(result)
```

**dsa-mcp 调用方（agent）流程**:
```python
# 1. 从 8084 MCP 拿 K 线 (强制走 8084, 不走 dSA)
df = await client_8084.call_tool("get_kline", symbol, days=60)
# 2. 调 dsa-mcp 分析
trend = await client_dsa.call_tool("analyze_trend", symbol, df)
```

**其他纯函数 tool 也按此原则改造** (`calculate_ma` / `get_volume_analysis` / `analyze_pattern` 也都改成接 df 参数)。

#### 3.4 搬策略 + prompt (0.5 天)

- [ ] `strategies/*.yaml` 15 个 → `src/dsa_mcp/strategies/` (整目录复制)
  - 编码: 显式 UTF-8
  - 不修改任何 YAML 内容
- [ ] 6 个 agent `system_prompt` 字符串 → `src/dsa_mcp/prompts/*.md`
  - technical_agent.py → technical.md (从 `system_prompt()` 函数体抄)
  - intel_agent.py → intel.md
  - risk_agent.py → risk.md
  - portfolio_agent.py → portfolio.md
  - decision_agent.py → decision.md + decision_chat.md (chat mode)
  - **不抄代码, 只抄字符串**

#### 3.5 自设计 alert (1 天)

**铁律**: 预警 = 纯信号，不绑推送（详见顶部）。**数据全走 8084，不依赖 dSA DB**。

**dSA 没有可搬的预警**, 自己设计:

`src/dsa_mcp/alerts/rules.yaml` (10-15 条规则):
```yaml
- id: price_break_20d_high
  name: 突破20日新高
  description: 收盘价创近20日新高
  severity: high
  conditions:
    - field: close
      op: gte
      value: max_20d_high
  cooldown_minutes: 60

- id: volume_breakout_3x
  name: 放量3倍
  description: 当日成交量 > 5日均量 3 倍
  severity: medium
  conditions:
    - field: volume_ratio
      op: gte
      value: 3.0

- id: bias_ma5_over_5pct
  name: 乖离率超5%
  description: 收盘价相对 MA5 乖离率 > 5%
  severity: low
  conditions:
    - field: bias_ma5
      op: gte
      value: 5.0
```

`src/dsa_mcp/alerts/checker.py`:
```python
def check_alert(symbol: str, rule_id: str = None) -> dict:
    """
    检查预警（纯信号，不推任何东西）
    symbol: 股票代码
    rule_id: 规则 ID (None = 跑全部规则)

    数据来源: 通过 8084 MCP 拿 quote + kline (强制铁律)

    返回: {
      symbol: str,
      triggered: bool,
      signals: [
        {rule_id, severity, name, value, reason, suggested_action}
      ]
    }
    """
    # 1. 调 8084 MCP 拿数据 (铁律: 走 8084)
    quote = await client_8084.call_tool("get_quote", symbol)
    kline = await client_8084.call_tool("get_kline", symbol, days=60)
    df = pd.DataFrame(kline)
    # 2. 加载 rules.yaml
    # 3. 跑每条规则的 conditions
    # 4. 返回纯信号结构（不推任何东西）
```

**tool 接口**:
- `check_alert(symbol, rule_id=None) -> {triggered, signals[]}`
- `list_alert_types() -> [{rule_id, name, severity, description}]`

**绝对不做**:
- ❌ 不调任何 sender（飞书/企微/HTTP webhook 等）
- ❌ 不写数据库
- ❌ 不做调度（cron / scheduler 都不归 dsa-mcp）
- ❌ 不依赖 dSA 的 history_loader / DataFetcherManager

#### 3.6 写 MCP server (0.5 天)

`src/dsa_mcp/server.py` 入口, 端口 8087

**tools 列表** (10 个, **数据全走 8084 / dSA 仓的 df 由调用方传**):

```python
@app.tool()
def calculate_macd(symbol: str, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """算 MACD, 复用 StockTrendAnalyzer._calculate_macd (df 内部从 8084 MCP 拿)"""

@app.tool()
def calculate_rsi(symbol: str, period: int = 14) -> float:
    """算 RSI (df 内部从 8084 MCP 拿)"""

@app.tool()
def calculate_ma(symbol: str, periods: str = "5,10,20,60") -> dict:
    """算 MA + bias (df 内部从 8084 MCP 拿)"""

@app.tool()
def get_volume_analysis(symbol: str, days: int = 30) -> dict:
    """量价分析 (df 内部从 8084 MCP 拿)"""

@app.tool()
def analyze_pattern(symbol: str, days: int = 60) -> dict:
    """K 线形态识别 (df 内部从 8084 MCP 拿)"""

@app.tool()
def analyze_trend(symbol: str, df_data: list = None) -> dict:
    """综合技术分析
    - df_data: 可选, 调用方传 K 线 records (避免重复拉)
    - None: 内部从 8084 MCP 拿
    """

@app.tool()
def list_strategies() -> list:
    """列出 15 个策略 (返回 name + display_name + category)"""

@app.tool()
def get_strategy(strategy_id: str) -> str:
    """返回策略的完整 instructions 文本 (中文)"""

@app.tool()
def list_alert_types() -> list:
    """列出所有预警规则 (不跑规则, 只列定义)"""

@app.tool()
def check_alert(symbol: str, rule_id: str = None) -> dict:
    """检查预警, 返回纯信号 (不推任何东西)"""
```

**关键约束**:
- 所有 tool 内部需要数据时 **必须走 8084 MCP**（不允许 import dSA fetcher/history_loader）
- `analyze_trend` 接受可选 `df_data` 参数, 调用方可以预先从 8084 拿好 K 线传进来（避免重复拉）

#### 3.7 测试 (0.5 天)

- [ ] `test_trend.py`: `StockTrendAnalyzer` 用合成数据测 (dSA 自带 `__main__` 测试可参考)
- [ ] `test_ma.py`: `calculate_ma` 用已知数据比对
- [ ] `test_volume.py`: `get_volume_analysis` happy path
- [ ] `test_pattern.py`: `analyze_pattern` 检测特定形态
- [ ] `test_strategies.py`: 15 YAML 都能正确解析
- [ ] `test_alerts.py`: 触发 1-2 条规则
- [ ] `test_mcp_server.py`: 启动 + `tools/list` + 调 1-2 个 tool

#### 3.8 commit (拆 3 个)

1. `chore: init repo + upstream tracking`
2. `feat(analysis): port StockTrendAnalyzer + 3 analysis tools from dSA`
3. `feat(mcp): implement MCP server with 10 tools + alert checker`

---

### Phase 4: 接入 cron b55832b29fc0

**目标**: 改造现有盘中快报 cron, 让它用两个 MCP 增强 prompt。

**预估工作量**: 0.5 天

#### 4.1 任务

- [ ] cron prompt 更新: 报告生成时调 8084 MCP 拿数据, 调 dsa-mcp 拿策略匹配和预警检查
- [ ] 报告模板增加"策略匹配"段落: 列出当日符合哪些 dSA 策略
- [ ] 增加"预警检查"段落: 调 dsa-mcp `check_alert` 检查持仓标的
- [ ] 跑一次 dry-run 验证

#### 4.2 验收

- [ ] 明早 09:45 cron 触发, 报告结构正确
- [ ] dSA MCP 调用失败时降级 (不阻塞基础报告)
- [ ] 报告内容质量明显提升 (对比旧版)

#### 4.3 commit

单个 commit: `chore(cron): 集成 8084-mcp + dsa-mcp 增强盘中快报`

---

## 3. 时间线 (按审计结论调整)

| Phase | 工作量 | 累计 |
|---|---|---|
| Phase 1 源码审计 | ✅ 完成 | 0 |
| Phase 2 8084 MCP 层 | 2 天 | 2 天 |
| Phase 3 dsa-mcp 新仓 | 2-3 天 | 4-5 天 |
| Phase 4 cron 接入 | 0.5 天 | 5-6 天 |

**总计**: 5-6 天 (v1 是 7-9 天, 砍了 backtest + search 节省 2-3 天)

---

## 4. 风险与回滚

| 风险 | 影响 | 缓解 |
|---|---|---|
| `analyze_trend` 改造引入 bug | 综合分析算错 | 写测试 + 对比 dSA 原版输出 |
| 6 个 prompt 抄写漏字符 | agent 行为漂移 | 字符串完全 copy-paste, 不重写 |
| 15 YAML 编码问题 | 中文乱码 | 复制时显式 utf-8 + git diff 验证 |
| 5 个新 REST 接口 akshare 调用慢 | cron 报告延迟 | 加 timeout + cache + 重试 |
| dSA upstream 更新 | 我们代码漂移 | 写 UPSTREAM.md 记录, 每月手动 sync |
| 自设计 alert 规则不够 | 漏报/误报 | 借鉴 dSA risk_agent 的 7 类检查清单 |
| MCP server 端口冲突 | 启动失败 | 8084 (REST) / 8086 (fds MCP) / 8087 (dsa-mcp) |

**回滚**:
- Phase 2 commit 可独立 revert (不影响 8084 REST)
- Phase 3 是新仓, 删除仓即回滚
- Phase 4 是 prompt 改动, revert cron job 即可

---

## 5. 与 v1 的差异 (审计后修正)

| 项 | v1 估时 | v2 估时 | 节省 |
|---|---|---|---|
| Phase 2 工作量 | 2-3 天 | 2 天 | 0.5-1 天 (确定不做 search MCP) |
| Phase 3 工作量 | 3-5 天 | 2-3 天 | 1-2 天 (砍掉 backtest + search) |
| 总工作量 | 7-9 天 | 5-6 天 | **2-3 天** |
| 复杂度 | 中 | 低 | backtest + search 都涉及外部依赖 |

| 关键决策变化 (vs v2):
1. **不搬 `analyze_trend` 直接版**: 改造为接受 `df` 参数, 调用方自己拉数据
2. **不搬 backtest_tools**: 我们没数据源
3. **不搬 search_tools**: 用 zhihu MCP + 8084 news_aggregator
4. **自设计 alert**: dSA 无独立模块, 数据全走 8084 (announcement/community/news_aggregator)
5. **Phase 2 不加 search MCP**: 用 8084 已有 news_aggregator

| 关键决策变化 (vs v2, 基于 8084-AUDIT):
1. **Phase 2 接口从"5 个新"改成"8 个补全"**: 5 个纯包装已有 db 方法 + 3 个真正新建 (market_stats/limit_up_pool/dragon_tiger)
2. **个股新闻查询**: 已有 `community_enhanced._fetch_em_stock_news(symbol)` 按 symbol 查 (东方财富搜索 API)
3. **公告已存在**: `announcement_data` 表 + `get_announcements(symbol, days)` 已有, 只需 REST 包装
4. **板块/概念/资金流已存在**: `fund_flow_data` 表 + `get_fund_flow(flow_type, sector_kind)` 已有
5. **PE/PB 已存在**: `fundamental_data` 表 + `get_latest_fundamental(symbol)` + 腾讯接口 PE/PB
6. **真缺 3 个**: 涨跌统计 / 涨停池 / 龙虎榜 — 新增 akshare collector 函数

---

## 6. 待你拍板

### Q1: dsa-mcp 端口 ✅ 默认 8087 (8084/8086 已用)

### Q2: 仓的公开性
- **公开**: 和 dSA 同 license MIT, 标注 fork 来源, 社区友好
- **私有**: 防 dSA 作者介意

**我倾向公开**: MIT 协议本就允许, 标注清晰即可。

### Q3: MCP server 进程模型 ✅ **常驻**

**zsd 决定 (2026-07-02)**: systemd 拉起, 端口 8087 持续监听, agent 随时调。

**实施细节**:
- Phase 3 写 `systemd/dsa-mcp.service` (User=zsd, ExecStart=uvicorn ..., Restart=always)
- 加到 `systemctl --user enable dsa-mcp`
- 启动验证: `curl http://localhost:8087/tools/list` 必须返回 10 个 tool
- 日志: `journalctl --user -u dsa-mcp -f`

**为什么常驻更合理**:
- 8084 已常驻, 多一个 8087 不增加管理负担
- cron 启动延迟敏感 (按需启动要 1-2s 启动 + pandas import)
- MCP 协议设计本就假设 server 持续监听

### Q4: alert 规则 YAML 的设计 ✅ **种子规则启动**

**zsd 决定 (2026-07-02)**: 用 dSA risk_agent 的 7 类检查做种子。

**种子来源 (dSA `risk_agent.py` 的 7 类检查)**:
1. **Insider / Major Shareholder Activity** — 减持, 质押
2. **Earnings Warnings** — 业绩预亏, 业绩变脸
3. **Regulatory** — 监管处罚, 立案调查
4. **Industry Policy** — 行业政策风险
5. **Lock-up Expirations** — 解禁 (30 天内)
6. **Valuation Extremes** — PE > 100 或负数, PB > 10
7. **Technical Warning Signs** — 死叉, 跌破关键支撑

**zsd 修正 (2026-07-02)**: "我们应该都配过" API key + 已有 mmx_search / zhihu search 等

**实际数据能力盘点（修正版）**:

financial-data-service 仓已存在的 collector:
- `mmx_search.py` — MMX 搜索
- `news_aggregator.py` — 新闻聚合
- `rss.py` — RSS 订阅
- `community_enhanced.py` — 社区增强 (东财股吧/雪球)
- `zhihu.py` — 知乎
- `akshare.py` — akshare 财经新闻

已暴露的 8084 REST:
- `GET /api/v1/news/aggregator` ✅ (已存在)

已装的 MCP:
- zhihu (搜索/全网/热榜/直答) 4 个 tool

**结论**: 之前我标记 "🔴 骨架+新闻 TODO" 的 4 条规则（减持/业绩/监管/解禁）**全部是🟢 立即可跑**，走 8084 `news_aggregator` + akshare 数据源。

**Phase 3 第一天交付** (`src/dsa_mcp/alerts/rules.yaml` 初版, 11 条全部立即可跑):

```yaml
# === 技术类 (6 条, 数据走 8084 get_kline + dsa-mcp analyze_trend) ===
- id: bias_ma5_over_5pct       # 乖离率超5% 不追高
- id: volume_breakout_3x        # 放量3倍
- id: price_break_20d_high      # 突破20日新高
- id: macd_death_cross          # MACD 死叉
- id: rsi_overbought_70         # RSI > 70 超买
- id: ma5_below_ma20            # MA5 < MA20 空头排列

# === 估值类 (1 条, 数据走 8084 get_stock_info) ===
- id: valuation_pe_anomaly      # PE > 100 或负数 (等 Phase 2 get_stock_info)

# === 新闻/事件类 (4 条, 数据走 8084 news_aggregator) ===
- id: insider_reduction         # 大股东减持
- id: earnings_warning          # 业绩预亏/变脸
- id: regulatory_penalty        # 监管处罚/立案调查
- id: lockup_expiry             # 解禁 (30 天内)
```

**11 条全部🟢立即可跑，无任何 TODO**。

**迭代节奏**: 初版 11 条跑 1 周, 看触发效果再加/减/调阈值。

---

## 7. 下一步

等 zsd 审阅本计划 + 回答 Q2/Q3。
Q1/Q4 已默认结论, 无异议即按此执行。

审阅通过后:
1. Phase 2 开工 (2 天) → 8084 加 5 接口 + MCP server
2. Phase 3 开工 (2-3 天) → 新建 dsa-mcp 仓 + 搬算法 + 自设计 alert
3. Phase 4 开工 (0.5 天) → cron 接入

每完成一个 Phase 在本文末尾追加"完成记录"。

---

## 完成记录

(执行时填写)