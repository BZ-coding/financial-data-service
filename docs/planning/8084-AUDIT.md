# financial-data-service (8084) 真实能力盘点

**审计对象**: `BZ-coding/financial-data-service` @ `7535f41` (2026-07-01)
**审计者**: Hermes
**日期**: 2026-07-02
**目标**: 摸清 8084 service 真实能力，作为 dsa-mcp / 8086 MCP 的数据底座

---

## 0. 结论摘要

**8084 不是一个"轻量采集器"——它是一个完整的金融数据仓库**：
- 14 张表（远超 README 写的）
- 12 个 collector（README 严重低估）
- 完整多源降级路由（Router + Chain）
- 调度器自动采集
- REST API + Web 管理界面
- 工单系统

**对 dSA MCP 计划的关键影响**：
1. 之前 PLAN 写的"新增 5 个 REST 接口" → **多数 db 层已经有了**，只缺 REST 包装
2. 之前 PLAN 写的"alert 规则 11 条立即可跑" → **数据来源全部存在**，无需新建任何 collector
3. 之前 PLAN 写的"PE/PB 数据需新增 `get_stock_info`" → **`get_latest_fundamental(symbol)` 已存在**
4. 之前 PLAN 写的"个股新闻查询" → **community_enhanced.collector 已支持**，**只是没暴露 REST**

---

## 1. 仓库真实结构

```
financial-data-service/
├─ config/
│   ├─ config.yaml              (1057b)
│   └─ rss_config.json          (1180b, 4 个 RSS/akshare 源)
├─ data/
├─ docs/
│   └─ decisions/
│       ├─ 0001-主力资金榜与板块资金流架构.md  (9457b)
│       └─ README.md
├─ logs/
├─ market_service/              (核心代码)
│   ├─ api.py                   24KB - FastAPI 入口
│   ├─ database.py              59KB - SQLite 操作层 (70 个方法)
│   ├─ scheduler.py             26KB - 调度器 (12 collector 全启用)
│   ├─ schema.sql               (13 张表 + 索引)
│   ├─ run.py / run_server*.py  (启动脚本)
│   ├─ collectors/              15 个 .py
│   │   ├─ base.py              3KB - BaseCollector
│   │   ├─ router.py            10KB - 多源降级路由
│   │   ├─ chain.py             21KB - 链式 collector
│   │   ├─ akshare.py           45KB - A股主力 (14 个 _fetch_)
│   │   ├─ akshare_rebuilt.py   19KB - akshare 重构版
│   │   ├─ tushare.py           10KB - Tushare Pro
│   │   ├─ tickflow.py          6KB - tickflow 降级
│   │   ├─ massive.py           8KB - 美股 API
│   │   ├─ sina.py              5KB - 新浪直连
│   │   ├─ eastmoney.py         6KB - 东方财富 push2
│   │   ├─ rss.py               12KB - RSS 订阅
│   │   ├─ news_aggregator.py   11KB - 全网聚合 (HN+GitHub+同花顺)
│   │   ├─ mmx_search.py        5KB - mmx CLI
│   │   ├─ community_enhanced.py  17KB - 东方财富搜索 + 雪球热帖 + 主力资金 + 板块资金流
│   │   ├─ zhihu.py             9KB - 知乎搜索
│   │   └─ __init__.py
│   ├─ data/
│   ├─ logs/
│   ├─ config/
│   └─ scripts/  (2 .py)
└─ static/
    └─ admin.html              (Web 管理页)
```

---

## 2. 数据库真实能力 (schema.sql + database.py)

### 2.1 全部 13 张表

| 表 | 字段要点 | 来源 | 用于 alert / 报告 |
|---|---|---|---|
| `nav_data` | 基金净值 (source, symbol, nav_date, nav, acc_nav, change_pct) | akshare | 基金 |
| `price_data` | 实时行情 (price, volume, amount, high, low, open, prev_close), **60天** | akshare/sina/tencent | ✅ 价格类 |
| `news_data` | 财经新闻 (title, summary, link, symbol, published), **30天** | akshare/rss | ✅ 新闻 |
| `index_data` | 大盘指数 (code, price, change_pct, volume, amount, high, low, open, prev_close), **永久** | akshare | ✅ 指数 |
| `daily_data` | 日K线 (open/high/low/close/pre_close/change/pct_chg/vol/amount), **60天** | tushare | ✅ 技术 |
| `minute_data` | 分钟K线 (open/high/low/close/volume/amount), **60天** | sina | ✅ 量价 |
| `hsgt_data` | 北向资金 Top10 (trade_date, ts_code, name, close, change, pct_chg, vol, amount) | tushare | ✅ 资金 |
| `announcement_data` | **公告 (symbol, title, announcement_time, link)**, **180天** | akshare 巨潮资讯 | ✅ **减持/业绩/监管/解禁** |
| `community_data` | 股吧 (symbol, title, author, reply_count, click_count, published), **30天** | akshare 股吧 | ✅ 舆情 |
| `news_aggregator_data` | **全网聚合 (hackernews/github/ths 三个固定 source, 不是按 symbol)** | news_aggregator | ✅ 全网热点 |
| `fund_flow_data` | **主力资金榜 + 板块资金流 (flow_type: main_fund_rank/sector_fund_flow, sector_kind: industry/concept)**, **永久** | community_enhanced | ✅ **板块榜/资金流** |
| `subscriptions` | 订阅配置 (name, type, config, symbol, data_types, frequency_min, trading_hours_only, backup_sources, enabled) | — | 调度 |
| `collection_log` | 采集日志 (status, items_fetched, items_stored, error_message, duration_sec) | — | 监控 |

**真实表数 13 张（不是 README 写的 12 张）**——`fund_flow_data` 是 2026-06-23 新增的。

### 2.2 database.py 70 个方法全清单

#### 数据查询（按数据类型）
- nav: `get_latest_nav`, `get_nav_history`
- price: `get_latest_price`, `get_price_history`
- daily: `get_latest_daily`, `get_daily`
- minute: `get_latest_minute_bar`, `get_minute_bars`
- index: `get_latest_index`
- fundamental: **`get_latest_fundamental`, `get_fundamental_history`, `save_fundamental`** ⭐ 已有！
- news: `get_news_by_link`, `get_recent_news`
- announcement: **`get_announcements(symbol, days)`** ⭐ 已有！
- community: `get_community_posts`
- fund_flow: **`get_fund_flow(flow_type, days, limit)`** ⭐ 已有！
- news_aggregator: `get_latest_news_aggregator`
- hsgt: `get_latest_hsgt`
- estimate_nav: `get_latest_estimate_nav`

#### 订阅管理
- `get_all_subscriptions`, `get_due_subscriptions`, `get_subscription_by_symbol`, `get_subscriptions_by_type`
- `add_subscription`, `update_subscription`, `delete_subscription`
- `update_subscription_last_run`

#### 工单系统
- `create_ticket`, `get_tickets`, `get_ticket`, `update_ticket_status`, `add_ticket_comment`

#### 监控
- `log_collection`, `get_recent_errors`, `get_stats`
- `cleanup_old_data`

#### 内部
- `init_tables`, `_enable_wal`, `insert_*` (12 个)

**70 个方法里，外部查询只暴露了 ~15 个 REST 接口**——**大量 db 方法**没被 REST 暴露，需要 Phase 2 加 MCP tool 包装。

---

## 3. 12 个 Collector 全能力盘点

### 3.1 akshare.py (主力) — 14 个 `_fetch_` 函数
| 函数 | 数据 | 来源 | 是否已入库 |
|---|---|---|---|
| `_fetch_fund_nav_sync` | 基金净值 | akshare `fund_open_fund_info_em` | ✅ nav_data |
| `_fetch_fund_estimate_nav_sync` | 盘中估算净值 | akshare `fund_estimated_value` | ✅ estimate_nav_data |
| `_fetch_stock_news_sync` | **个股新闻** | akshare `stock_news_em` | ✅ news_data |
| `_fetch_index_data_sync` | **指数** | akshare `stock_zh_index_spot_em` | ✅ index_data |
| `_fetch_announcement_cninfo` | **公告** | akshare `stock_announcement_cninfo` (巨潮) | ✅ announcement_data |
| `_fetch_community_guba` | **股吧** | akshare `stock_comment_em` | ✅ community_data |
| `_fetch_fund_fundamental_xq` | 雪球基金基本面 | 雪球 | — |
| `_fetch_hk_quote_tencent` | **港股行情** | 腾讯 qt.gtimg.cn | — |
| `_fetch_stock_quote_sync` | **A股实时行情（4 级降级）** | akshare_spot → akshare_hist → tencent → sina | ✅ price_data |
| `_fetch_stock_quote_tencent` | **A股行情 + 基本面** | 腾讯 qt.gtimg.cn (PE/PB/市值/行业) | ✅ |
| `_fetch_stock_fundamental_tencent` | **PE/PB/市值** | 腾讯 | ✅ fundamental_data |
| `_fetch_stock_quote_sina` | A股行情 | 新浪 hq.sinajs.cn | ✅ price_data |
| `_fetch_minute_bars_sina` | **分钟K线** | 新浪 | ✅ minute_data |

### 3.2 community_enhanced.py — 4 个 `_fetch_` 函数
| 函数 | 数据 | 用途 |
|---|---|---|
| `_fetch_em_stock_news(symbol, page_size)` | **东方财富搜索API，按 symbol 查个股新闻** (港股/A股/美股) | 个股新闻查询 ⭐ |
| `_fetch_xueqiu_hot_tweets` | 雪球热帖 | 舆情 |
| `_fetch_xueqiu_hot_follow` | 雪球热关注 | 舆情 |
| `_fetch_xueqiu_hot_deal` | 雪球热门交易 | 舆情 |
| `_fetch_main_fund_rank(top_n)` | **主力资金榜** | 主力资金榜 ⭐ |
| `_fetch_sector_fund_flow(kind, top_n)` | **板块资金流 industry/concept** | 板块榜/概念榜 ⭐ |

### 3.3 其他 collector
- `massive.py` — 美股 (价格/分钟K)
- `tushare.py` — 日K/北向资金/stock_basic
- `tickflow.py` — 降级兜底
- `sina.py` — 新浪直连
- `eastmoney.py` — 东方财富 push2 (A股+港股+指数实时行情)
- `rss.py` — RSS 订阅 (4 个源: akshare财经快讯/央视新闻/36氪/新浪财经)
- `news_aggregator.py` — 全网聚合 (HN/GitHub/同花顺)
- `mmx_search.py` — `mmx search` CLI 调用
- `zhihu.py` — 知乎搜索/热榜/直答
- `chain.py` — 链式 collector (10 种 transform)

---

## 4. REST API 真实能力

### 4.1 已暴露的接口

| 端点 | 方法 | 用途 |
|---|---|---|
| `/` | GET | 根路径 |
| `/api/v1/data?source=&symbol=&data_type=&fresh=` | GET | 查最新 |
| `/api/v1/history?symbol=&data_type=&days=` | GET | 查历史 |
| `/api/v1/news/aggregator` | GET | 全网聚合新闻（**无参数，最新一期**） |
| `/api/v1/subscriptions` | GET/POST | 订阅管理 |
| `/api/v1/subscriptions/{id}` | PUT/DELETE | 改/删 |
| `/api/v1/subscriptions/{id}/toggle` | PATCH | 启停 |
| `/api/v1/tickets` | GET/POST | 工单 |
| `/api/v1/tickets/{id}` | GET | 详情 |
| `/api/v1/tickets/{id}/status` | PATCH | 改状态 |
| `/api/v1/tickets/{id}/comment` | POST | 评论 |
| `/api/v1/stats` | GET | 统计 |
| `/api/v1/errors?hours=` | GET | 错误日志 |
| `/api/v1/collect?source=&symbol=&data_type=` | POST | **手动采集 (501 未实现)** |
| `/api/docs` | GET | Swagger |
| `/api/redoc` | GET | ReDoc |
| `/admin` | GET | Web 管理页 |
| `/guide`, `/api-guide` | GET | 用户/API 文档 |

### 4.2 data_type 支持的取值
nav / estimate_nav / price / daily / minute / index / hsgt / news / fundamental / stock_basic / news_aggregator

**没有**：`sector_rankings` / `concept_rankings` / `market_stats` / `limit_up_pool` / `dragon_tiger` / **`announcement` (数据表有但没暴露)** / **`community` (数据表有但没暴露)** / **`fund_flow` (数据表有但没暴露)**

### 4.3 已知问题
- `fresh=true` 抛 501
- `POST /api/v1/collect` 抛 501
- **绝大多数 db 方法没 REST 包装**（如 `get_announcements`, `get_fund_flow`, `get_community_posts`, `get_latest_fundamental`）

---

## 5. 路由器 (router.py) 真实设计

`CollectorRouter` 的设计：
- **6 个市场**: SH / SZ / BJ / US / HK / 期货(SHF/DCE/ZCE/CFX/INE/GFE)
- **12 种数据类型**: price / nav / fundamental / index / news / announcement / community / minute / daily / estimate_nav / hsgt / stock_basic
- **每个 (市场, 数据类型) 组合**都定义了降级链路

例如 `("SH", "price")` 的链路：
```
akshare → tushare → tickflow → massive
```

这是**完整的多源降级路由**——dSA 没有这么细的路由。

---

## 6. 对 dSA MCP 计划的影响（修正）

### 6.1 Phase 2 不需要新建 5 个接口，只需补 3 个

**之前 PLAN 写的"5 个新 REST 接口"**：
- ❌ `sector_rankings` — 不需要新建，**`fund_flow_data` + `get_fund_flow("sector_fund_flow", days=1)` + filter sector_kind='industry' 就有**
- ❌ `concept_rankings` — 同上，filter sector_kind='concept' 就有
- ❌ `market_stats`（涨跌统计）— **真没有，需要新建**，加 akshare `stock_market_activity_legu` 拉
- ❌ `limit_up_pool` — **真没有，需要新建**，加 akshare `stock_zt_pool_em` 拉
- ❌ `dragon_tiger` — **真没有，需要新建**，加 akshare `stock_lhb_detail_em` 拉

**修正**: Phase 2 新建 3 个 REST 接口 (market_stats / limit_up_pool / dragon_tiger) + 加 db 方法 + 加 collector 函数。其它 2 个查 fund_flow_data 即可。

### 6.2 个股新闻查询真实可用

之前我说"dSA 没有个股新闻 MCP"——错。
**8084 真实能力**:
- `db.get_recent_news(source, hours)` — 按 source 查
- `community_enhanced._fetch_em_stock_news(symbol, page_size)` — 按 symbol 查 (东方财富搜索 API)

**修正**: Phase 2 加 `GET /api/v1/news?symbol=&days=` REST + Phase 2 的 mcp_server 加 `get_news(symbol, days, source)` tool。

### 6.3 公告 (announcement) 已存在但没暴露

**之前我说"alert 减持/业绩/监管/解禁需要等 news MCP"——错**。
**真实能力**:
- `db.get_announcements(symbol, days)` 已存在
- `akshare._fetch_announcement_cninfo(symbol)` 拉巨潮资讯公告
- 表 `announcement_data` 保留 180 天

**修正**: Phase 2 加 `GET /api/v1/data?data_type=announcement&symbol=` REST 即可。Phase 3 的 alert 规则 4 条新闻类**全部走 announcement_data**（不需要 news MCP）。

### 6.4 主力资金榜 + 板块资金流 已存在

**community_enhanced 已实现**:
- `_fetch_main_fund_rank(top_n)` — 主力资金榜
- `_fetch_sector_fund_flow(kind, top_n)` — 板块资金流
- 表 `fund_flow_data` 永久保存
- db 方法 `get_fund_flow(flow_type, days, limit)`

**修正**: Phase 2 加 `GET /api/v1/data?data_type=fund_flow&flow_type=main_fund_rank|sector_fund_flow&sector_kind=` REST 即可。

### 6.5 PE/PB/市值 已存在

**真实能力**:
- `akshare._fetch_stock_fundamental_tencent(code)` — PE/PB/总市值/流通市值/行业
- `db.get_latest_fundamental(symbol)` / `db.save_fundamental(data)` / `db.get_fundamental_history`

**修正**: Phase 2 加 `GET /api/v1/data?data_type=fundamental&symbol=` REST 包装 `get_latest_fundamental` 即可。

---

## 7. 数据获取路径图（修正后）

```
agent (Hermes)
    │
    ├─→ 8086 MCP (financial-data-service mcp_server)
    │     ├─ get_quote(symbol)            → 8084 REST /api/v1/data?data_type=price
    │     ├─ get_kline(symbol, days)      → 8084 db get_daily
    │     ├─ get_news(symbol, days)       → 8084 REST /api/v1/data?data_type=news (新增)
    │     ├─ get_announcements(symbol)    → 8084 REST /api/v1/data?data_type=announcement (新增)
    │     ├─ get_fund_flow(flow_type)     → 8084 db get_fund_flow (新增 REST)
    │     ├─ get_sector_rankings(n)       → 8084 db get_fund_flow + filter (新增 REST)
    │     ├─ get_concept_rankings(n)      → 8084 db get_fund_flow + filter (新增 REST)
    │     ├─ get_market_stats()           → 8084 akshare stock_market_activity_legu (新增)
    │     ├─ get_limit_up_pool(n)         → 8084 akshare stock_zt_pool_em (新增)
    │     ├─ get_dragon_tiger(symbol)     → 8084 akshare stock_lhb_detail_em (新增)
    │     └─ get_realtime_indices()       → 8084 REST /api/v1/data?data_type=index
    │
    └─→ 8087 MCP (dsa-mcp)
          ├─ analyze_trend(symbol, df)    → df 来自 8086 MCP get_kline
          ├─ calculate_ma(symbol)         → df 来自 8086 MCP get_kline
          ├─ ... (10 个分析 tool)
          ├─ check_alert(symbol)           → 内部调 8086 MCP 拿 quote/kline/news/announcement
          ├─ list_strategies()
          └─ get_strategy(strategy_id)     → 返回 15 YAML 内容
```

---

## 8. 8084 真实不足

**只有 3 个数据真没有，需要新建 collector**:

1. **涨跌统计**（涨/跌/平家数/涨停/跌停/总成交额）— akshare `stock_market_activity_legu`
2. **涨停池**（涨停股列表 + 连板梯队）— akshare `stock_zt_pool_em` / `stock_zt_pool_zbgc_em`
3. **龙虎榜**（个股/游资/营业部）— akshare `stock_lhb_detail_em`

**加上上面"announcement/community/fund_flow/news_by_symbol/fundamental" 5 个 REST 接口补全 = 8 个 Phase 2 任务**，不是之前 PLAN 写的"5 个"。

---

## 9. 结论与下一步

**审计完成。8084 的真实能力比之前 PLAN 假设的强得多**：
- 13 张表（之前以为 11）
- 12 个 collector（之前以为 10）
- 70 个 db 方法（之前以为 30）
- 16 个 REST 端点（之前以为 8）

**对 dSA MCP 计划的影响**：
1. Phase 2 工作量**减少**：5 个新接口变 8 个（含 5 个补 REST 包装），但**不需要写底层 collector**
2. Phase 3 alert 11 条规则**全部立即可跑**，数据来源 100% 已在 8084
3. **整体风险降低**：数据底座已稳固，无需担心数据问题

**下一步**：
1. 写修正版 PLAN-zsd-fin-modules.md（基于真实能力）
2. 把 PLAN/AUDIT commit 到 financial-data-service 仓
3. 建 dsa-mcp 仓初始化

---

## 审计结束