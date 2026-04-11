# Financial Data Service

**轻量级金融数据采集平台** — 一键启动，自动采集基金净值、股票行情、日K历史、北向资金、财经新闻。

![Python](https://img.shields.io/badge/Python-3.11+-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Platform](https://img.shields.io/badge/Platform-Linux%20|%20macOS%20|%20Windows-orange)

---

## 功能特性

| 功能 | 说明 |
|------|------|
| **基金净值** | 天天基金、东方财富，支持盘中估算净值 |
| **股票行情** | 多源降级（akshare → massive），自动切换 |
| **日K历史** | akshare → tushare → tickflow 三级降级 |
| **北向资金** | 沪深港通 Top10 持仓（Tushare） |
| **财经新闻** | 全网聚合（HackerNews + GitHub Trending + 同花顺） |
| **美股行情** | Massive API，实时 + 分钟K（Bearer Token 认证） |
| **股吧舆情** | 东方财富股吧帖子采集 |
| **公告数据** | 巨潮资讯个股公告 |
| **RSS订阅** | 财经 RSS Feed 定时抓取 |
| **智能调度** | 按订阅自动采集，支持交易时段限制 |
| **REST API** | FastAPI + Swagger 文档，端口 8084 |

---

## 快速开始

### 1. 克隆

```bash
git clone https://github.com/BZ-coding/financial-data-service.git
cd financial-data-service
```

### 2. 安装依赖

```bash
cd market_service
python3 -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. 配置

```bash
cp config/config.yaml.example config/config.yaml
# 编辑 config.yaml，填入你的 API Token：
#   tushare.token      → https://tushare.pro/register
#   massive.api_key    → https://api.massive.com
#   tickflow           → 免费层，无需 key
```

### 4. 初始化数据库

```bash
sqlite3 data/market.db < schema.sql
```

### 5. 启动

```bash
# 前台（调试）
python run_server_8084.py

# 后台
nohup python run_server_8084.py > logs/app.log 2>&1 &
```

### 6. 验证

```bash
curl http://localhost:8084/

# 查看统计
curl http://localhost:8084/api/v1/stats | python3 -m json.tool
```

访问 http://localhost:8084/api/docs 查看完整 API 文档。

### Web 管理界面

```bash
# 管理界面（订阅管理、数据查看、错误日志）
open http://localhost:8084/admin
```

---

## 数据类型

| data_type | 说明 | 采集器 | 数据库表 |
|-----------|------|--------|---------|
| `nav` | 基金净值 | akshare | `nav_data` |
| `estimate_nav` | 盘中估算净值 | akshare | `estimate_nav_data` |
| `price` | 股票实时行情 | akshare | `price_data` |
| `daily` | 日K历史 | akshare→tushare→tickflow | `daily_data` |
| `minute` | 分钟K线 | akshare | `minute_data` |
| `index` | 大盘指数 | akshare | `index_data` |
| `hsgt` | 北向资金 Top10 | tushare | `hsgt_data` |
| `news` | 财经新闻 | akshare | `news_data` |
| `community` | 股吧社区 | akshare | `community_data` |
| `announcement` | 公告 | akshare | `announcement_data` |
| 美股实时/分钟K | 美股行情 | massive | `price_data` |
| RSS | 财经订阅源 | rss | `news_data` |
| 全网聚合 | 多平台新闻 | news_aggregator | `news_aggregator_data` |

---

## API 示例

```bash
# 基金净值
curl "http://localhost:8084/api/v1/data?source=akshare&symbol=008114&data_type=nav"

# 股票行情
curl "http://localhost:8084/api/v1/data?source=akshare&symbol=600000.SH&data_type=price"

# 历史净值
curl "http://localhost:8084/api/v1/history?symbol=008114&data_type=nav&days=30"

# 北向资金
curl "http://localhost:8084/api/v1/data?source=tushare&symbol=000001.SZ&data_type=hsgt"
```

---

## 架构

```
market_service/
├── api.py              # FastAPI 服务入口
├── scheduler.py         # 调度器（按 subscriptions 表触发）
├── database.py         # SQLite 操作层
├── schema.sql          # 数据库表定义
├── run_server_8084.py  # 启动脚本
├── collectors/
│   ├── base.py         # BaseCollector / CollectResult
│   ├── akshare.py      # 股票/基金/指数/新闻（主力）
│   ├── tushare.py      # 日K历史 / 北向资金
│   ├── tickflow.py     # 日K历史（降级兜底）
│   ├── massive.py      # 美股实时/分钟K
│   ├── router.py       # 多源降级路由器
│   ├── rss.py          # RSS 订阅
│   └── news_aggregator.py  # 全网聚合新闻
├── config/
│   └── config.yaml      # 配置文件（不上传，见 config.yaml.example）
└── static/
    └── admin.html       # Web 管理界面
```

---

## 添加订阅

新标的必须插入订阅记录，调度器才会自动采集：

```sql
-- 基金（非交易时段也采集）
INSERT INTO subscriptions (name, type, symbol, data_types, frequency_min, trading_hours_only, enabled)
VALUES ('天弘红利ETF', 'akshare', '008114', '["nav","estimate_nav"]', 30, 0, 1);

-- 股票（仅交易时段）
INSERT INTO subscriptions (name, type, symbol, data_types, frequency_min, trading_hours_only, enabled)
VALUES ('浦发银行', 'akshare', '600000', '["price","daily"]', 10, 1, 1);
```

---

## 关键坑点

- **基金配 `daily` 无数据**：`daily` 是股票日K，基金用 `nav` + `estimate_nav`
- **标的不采集**：检查 `subscriptions` 表是否有该标的的记录
- **SQLite WAL 锁定**：`PRAGMA wal_checkpoint(FULL);`
- **非交易时段无数据**：`trading_hours_only=1` 时仅 9:30-11:30 / 13:00-15:00 采集

---

## License

MIT — 商用/修改/闭源均可。
