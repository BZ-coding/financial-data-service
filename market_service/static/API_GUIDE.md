# Market Data Service - API 使用说明书

**版本**: v1.0  
**最后更新**: 2026-04-07  
**服务地址**: http://192.168.10.70:8084

---

## 📋 目录

1. [接口概述](#接口概述)
2. [快速开始](#快速开始)
3. [核心接口](#核心接口)
4. [数据格式](#数据格式)
5. [错误码说明](#错误码说明)
6. [调用示例](#调用示例)
7. [最佳实践](#最佳实践)
8. [常见问题](#常见问题)

---

## 接口概述

### 基础信息

- **Base URL**: `http://192.168.10.70:8084`
- **协议**: HTTP/HTTPS（当前HTTP）
- **数据格式**: JSON
- **认证**: 无需（内网服务）
- **跨域**: 已启用CORS

### 核心能力

| 数据类型 | 说明 | 示例代码 |
|---------|------|----------|
| `price` | 股票实时行情 | `600519` |
| `nav` | 基金净值 | `008114` |
| `news` | 财经新闻 | `*`（全部） |

---

## 快速开始

### 1. 健康检查

```bash
curl http://192.168.10.70:8084/api/v1/stats
```

**预期响应**（200 OK）：
```json
{
  "records": {
    "nav": 120,
    "price": 450,
    "news": 30
  },
  "scheduler": {
    "running": true,
    "last_tick": "2026-04-07T14:52:00+08:00"
  },
  "uptime_seconds": 3600
}
```

如果返回 `{"status":"error"}` 或 5xx，说明服务异常，请联系管理员。

---

### 2. 查询股票最新行情

```bash
curl "http://192.168.10.70:8084/api/v1/data?source=akshare&symbol=600519&data_type=price"
```

**响应示例**：
```json
{
  "source": "tencent",
  "symbol": "600519",
  "data_type": "price",
  "trade_time": "2026-04-07T14:42:10+08:00",
  "price": 1441.96,
  "volume": 2270470,
  "amount": 3298949601.0,
  "high": 1470.0,
  "low": 1435.05,
  "open": 1441.19,
  "prev_close": 1460.05
}
```

---

### 3. 查询基金净值

```bash
curl "http://192.168.10.70:8084/api/v1/data?source=akshare&symbol=008114&data_type=nav"
```

**响应示例**：
```json
{
  "source": "akshare",
  "symbol": "008114",
  "data_type": "nav",
  "nav_date": "2026-04-03",
  "nav": 1.0254,
  "acc_nav": 1.3256,
  "change_pct": "+0.15%"
}
```

---

### 4. 查询财经新闻

```bash
curl "http://192.168.10.70:8084/api/v1/data?source=akshare&symbol=*&data_type=news"
```

**响应示例**：
```json
[
  {
    "source": "akshare",
    "symbol": null,
    "data_type": "news",
    "title": "A股三大指数集体收涨",
    "summary": "今日A股三大指数集体收涨，沪指涨0.5%，深成指涨0.8%，创业板指涨1.2%",
    "url": "https://finance.sina.com.cn/...",
    "published": "2026-04-07T14:30:00+08:00"
  },
  ...
]
```

---

## 核心接口

### 1. 健康检查

```http
GET /api/v1/stats
```

**说明**: 查看服务状态、各数据表记录数、调度器状态

**响应**:
```json
{
  "records": {
    "nav": 整数,      // 净值表总记录数
    "price": 整数,    // 价格表总记录数
    "news": 整数      // 新闻表总记录数
  },
  "scheduler": {
    "running": true/false,
    "last_tick": "ISO时间",
    "next_tick": "ISO时间"
  },
  "uptime_seconds": 数值
}
```

---

### 2. 查询最新数据

```http
GET /api/v1/data?source={source}&symbol={symbol}&data_type={data_type}
```

**参数**:

| 参数 | 必填 | 说明 | 示例 |
|------|------|------|------|
| `source` | 是 | 数据源，当前固定 `akshare` | `akshare` |
| `symbol` | 是 | 代码，股票`600519`，基金`008114`，新闻`*` | `600519` |
| `data_type` | 是 | 数据类型：`price`/`nav`/`news` | `price` |

**响应**:
- `price` → 单个JSON对象
- `nav` → 单个JSON对象
- `news` → JSON数组

---

### 3. 查询历史数据

```http
GET /api/v1/history?symbol={symbol}&data_type={data_type}&days={days}
```

**参数**:

| 参数 | 必填 | 说明 | 默认 |
|------|------|------|------|
| `symbol` | 是 | 代码 | - |
| `data_type` | 是 | `price`/`nav` | - |
| `days` | 否 | 回溯天数 | 30 |

**响应**: JSON数组（按时间倒序）

```json
[
  {
    "source": "tencent",
    "symbol": "600519",
    "data_type": "price",
    "trade_time": "2026-04-07T14:42:10+08:00",
    "price": 1441.96,
    ...
  },
  ...
]
```

---

### 4. 订阅列表

```http
GET /api/v1/subscriptions
```

**说明**: 查看所有订阅配置（参数、间隔、状态）

**响应**: JSON数组
```json
[
  {
    "id": 3,
    "name": "600519茅台行情",
    "type": "akshare",
    "symbol": "600519",
    "data_types": ["price"],
    "frequency_min": 10,
    "trading_hours_only": true,
    "backup_sources": "tencent,sina",
    "enabled": true,
    "last_collected": "2026-04-07T14:42:10+08:00"
  },
  ...
]
```

---

### 5. 错误日志

```http
GET /api/v1/errors?limit={limit}
```

**参数**:
- `limit`: 返回条数（默认10）

**响应**:
```json
[
  {
    "subscription_id": 3,
    "source": "akshare",
    "symbol": "600519",
    "data_type": "price",
    "status": "failed",
    "error_message": "远程连接被拒绝",
    "started_at": "2026-04-07T14:18:00+08:00"
  },
  ...
]
```

---

### 6. API 文档

```http
GET /api/docs
```

Swagger UI交互式文档（推荐浏览器打开）。

---

## 数据格式

### price（股票实时行情）

```json
{
  "source": "tencent",           // 数据源：tencent/sina/akshare_spot/akshare_hist
  "symbol": "600519",
  "data_type": "price",
  "trade_time": "2026-04-07T14:42:10+08:00",  // 交易时间（UTC+8）
  "price": 1441.96,               // 最新价
  "volume": 2270470,              // 成交量（股）
  "amount": 3298949601.0,         // 成交额（元）
  "high": 1470.0,                 // 最高价
  "low": 1435.05,                 // 最低价
  "open": 1441.19,                // 开盘价
  "prev_close": 1460.05           // 昨收价
}
```

### nav（基金净值）

```json
{
  "source": "akshare",
  "symbol": "008114",
  "data_type": "nav",
  "nav_date": "2026-04-03",       // 净值日期
  "nav": 1.0254,                  // 单位净值
  "acc_nav": 1.3256,              // 累计净值
  "change_pct": "+0.15%"          // 日涨跌幅
}
```

### news（财经新闻）

```json
{
  "source": "akshare",
  "symbol": null,                 // 新闻无特定代码
  "data_type": "news",
  "title": "新闻标题",
  "summary": "新闻摘要...",
  "url": "https://...",           // 原文链接
  "published": "2026-04-07T14:30:00+08:00"
}
```

---

## 错误码说明

### HTTP 状态码

| 状态码 | 说明 | 处理建议 |
|--------|------|----------|
| 200 | 成功 | - |
| 404 | 接口不存在 | 检查URL拼写 |
| 500 | 服务器内部错误 | 查看服务日志，或联系管理员 |
| 502 | 外部数据源故障 | 等待自动降级或重试 |

### 业务错误（`/data` 返回）

| 错误信息 | 说明 |
|----------|------|
| "实时行情数据不可用（所有源失败）" | 腾讯、新浪全部不可用，稍后重试 |
| "基金净值数据不可用" | 天天基金接口异常，稍后重试 |
| "新闻数据不可用" | 新浪新闻接口异常，稍后重试 |
| "未知数据类型" | data_type参数错误 |

---

## 调用示例

### Python 示例

```python
import requests
import time

BASE_URL = "http://192.168.10.70:8084"

def get_stock_price(symbol: str):
    """获取股票最新行情"""
    url = f"{BASE_URL}/api/v1/data"
    params = {
        "source": "akshare",
        "symbol": symbol,
        "data_type": "price"
    }
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    else:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")

def get_fund_nav(symbol: str):
    """获取基金净值"""
    url = f"{BASE_URL}/api/v1/data"
    params = {
        "source": "akshare",
        "symbol": symbol,
        "data_type": "nav"
    }
    return requests.get(url, params=params, timeout=10).json()

def get_price_history(symbol: str, days: int = 7):
    """获取历史价格"""
    url = f"{BASE_URL}/api/v1/history"
    params = {
        "symbol": symbol,
        "data_type": "price",
        "days": days
    }
    return requests.get(url, params=params, timeout=10).json()

# 使用示例
if __name__ == "__main__":
    try:
        price = get_stock_price("600519")
        print(f"贵州茅台最新价: {price['price']} (来源: {price['source']})")
        
        nav = get_fund_nav("008114")
        print(f"天弘红利净值: {nav['nav']} ({nav['nav_date']})")
        
        hist = get_price_history("600519", days=3)
        print(f"最近3天数据: {len(hist)}条")
    except Exception as e:
        print(f"调用失败: {e}")
```

### JavaScript/Node.js 示例

```javascript
const axios = require('axios');

const BASE_URL = 'http://192.168.10.70:8084';

async function getStockPrice(symbol) {
  const url = `${BASE_URL}/api/v1/data`;
  const params = {
    source: 'akshare',
    symbol: symbol,
    data_type: 'price'
  };
  const resp = await axios.get(url, { params, timeout: 10000 });
  return resp.data;
}

// 使用
getStockPrice('600519')
  .then(data => console.log(`价格: ${data.price} (${data.source})`))
  .catch(err => console.error('调用失败:', err.message));
```

### Shell (curl) 示例

```bash
# 查询股票
curl "http://192.168.10.70:8084/api/v1/data?source=akshare&symbol=600519&data_type=price" \
  | python3 -m json.tool

# 查询基金
curl "http://192.168.10.70:8084/api/v1/data?source=akshare&symbol=008114&data_type=nav"

# 查询历史（最近7天）
curl "http://192.168.10.70:8084/api/v1/history?symbol=600519&data_type=price&days=7"
```

---

## 最佳实践

### 1. 重试机制

网络或数据源可能临时故障，建议实现**指数退避重试**：

```python
import time
import requests

def retry_with_backoff(func, max_retries=3):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            if i == max_retries - 1:
                raise
            wait = (2 ** i) + 1  # 1, 2, 4 秒
            time.sleep(wait)

# 使用
retry_with_backoff(lambda: get_stock_price('600519'))
```

### 2. 超时设置

```python
requests.get(url, params=params, timeout=10)  # 10秒超时
```

### 3. 幂等性

接口是幂等的，重复调用不会产生副作用，可以安全重试。

### 4. 频率限制

- 股票订阅默认 **10分钟** 采集一次
- 高频调用（如每秒1次）不会加快数据更新，反而可能触发保护机制
- 建议应用层缓存结果，避免重复查询

### 5. 交易时段感知

股票订阅受 `trading_hours_only` 限制：
- **交易时段**: 9:30-11:30, 13:00-15:00（A股）
- **非交易时段**: 接口可能返回旧数据或无数据

应用端应判断：
```python
from datetime import datetime, timezone, timedelta

now = datetime.now(timezone(timedelta(hours=8)))
is_trading = (
    (now.hour == 9 and now.minute >= 30) or
    (10 <= now.hour < 11) or
    (now.hour == 11 and now.minute < 30) or
    (13 <= now.hour < 15)
)
if not is_trading:
    print("注意：非交易时段，数据可能不是最新的")
```

### 6. 监控告警

建议监控：
- `/api/v1/stats` 的 `records.price` 是否持续增长
- `/api/v1/errors` 是否有大量失败记录
- HTTP 状态码异常（非200）

---

## 常见问题

### Q1: 返回的数据时间戳是采集时间还是交易时间？

**A**: `trade_time` 是数据源提供的交易时间（如腾讯返回的是撮合时间），不是采集时间。

---

### Q2: 数据实时性如何？

**A**:
- 腾讯/新浪实时行情：**1-5秒延迟**（正常）
- 采集间隔：订阅配置决定（如10分钟）
- 调度器每分钟检查，满足条件立即采集
- **总延迟** = 交易时间 + (10分钟 - 上一次采集后时间) + 网络传输

---

### Q3: 如果数据源失败，会返回什么？

**A**: 返回 `500` 或 `{"error": "..." }`，具体看失败原因。建议客户端重试。

---

### Q4: 数据源优先级是什么？

**A**: 订阅的 `backup_sources` 字段定义了降级顺序（如 `tencent,sina`）：
1. 先尝试 `tencent`
2. 失败则尝试 `sina`
3. 都失败才返回错误

记录中的 `source` 字段会显示实际使用的是哪个源。

---

### Q5: 可以查询任意股票吗？

**A**:
- 目前订阅已配置的代码：`600519`（茅台）、`008114`（基金）
- 其他代码需先添加订阅（联系管理员）
- 新闻订阅 `*` 可以获取全部

---

### Q6: 历史数据最多能查多久？

**A**: 默认保留最近30天。更早的数据可能已被清理。

---

### Q7: 接口有频率限制吗？

**A**:
- 无硬性频率限制，但数据更新频率由订阅间隔决定
- 建议查询间隔 > 1秒，避免对服务器造成压力
- 频繁调用不会获得更实时数据（受采集间隔约束）

---

### Q8: 如何判断数据是否最新？

**A**:
1. 检查 `trade_time` 是否接近当前时间（交易时段内应接近）
2. 监控 `collection_log` 的最新 `started_at`
3. 健康检查 `/api/v1/stats` 的 `records.price` 持续增长

---

### Q9: 出现错误时如何排查？

**A**:
1. **先自检**: 网络是否可达？参数是否正确？
2. **查健康**: `curl /api/v1/stats` 看服务是否正常
3. **看错误**: `curl /api/v1/errors` 看最近失败原因
4. **联系管理员**: 提供 `error_message` 和 `started_at`

---

### Q10: 可以推送数据吗（Webhook）？

**A**: 当前版本只支持拉取（Polling），不支持推送。建议客户端定时轮询。

---

## 附录

### A. 接口变更记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-04-07 | 初始版本 |

---

### B. 联系支持

如遇到文档未覆盖的问题，请提供：
1. 请求的URL和参数
2. 返回的HTTP状态码和Body
3. 发生时间
4. 客户端环境（语言、IP等）

---

**祝使用顺利！** 🎉

---

## 订阅管理 API

> **⚠️ 注意**: 订阅配置更改后，**不会立即采集**，需要等待下一个调度周期（通常1-10分钟，取决于 `frequency_min` 和当前时间）。

### 7.1 创建订阅

```http
POST /api/v1/subscriptions
Content-Type: application/json

{
  "name": "600519茅台行情",
  "type": "akshare",
  "symbol": "600519",
  "data_types": ["price"],
  "frequency_min": 10,
  "trading_hours_only": true,
  "backup_sources": "tencent,sina"
}
```

**响应**:
```json
{
  "id": 4,
  "status": "created"
}
```

### 7.2 更新订阅

```http
PUT /api/v1/subscriptions/4
Content-Type: application/json

{
  "frequency_min": 5,
  "enabled": true
}
```

**响应**:
```json
{
  "id": 4,
  "status": "updated",
  "fields": ["frequency_min", "enabled"]
}
```

### 7.3 删除订阅

```http
DELETE /api/v1/subscriptions/4
```

**响应**:
```json
{
  "id": 4,
  "status": "deleted"
}
```

### 7.4 切换启用状态

```http
PATCH /api/v1/subscriptions/4/toggle
```

**响应**:
```json
{
  "id": 4,
  "enabled": true
}
```

### 7.5 查看订阅列表

```http
GET /api/v1/subscriptions
```

**响应**:
```json
[
  {
    "id": 3,
    "name": "600519茅台行情",
    "type": "akshare",
    "symbol": "600519",
    "data_types": ["price"],
    "frequency_min": 10,
    "trading_hours_only": true,
    "backup_sources": "tencent,sina",
    "enabled": true,
    "last_collected": "2026-04-07T14:42:10+08:00"
  },
  ...
]
```

---

## 订阅生效时间说明

> **重要**: 创建或修改订阅后，**数据不会立即出现**，需要等待：
>
> 1. **调度周期**: 调度器每分钟检查一次（`scheduler.check_interval_seconds = 60`）
> 2. **满足触发条件**: `last_collected` 为空 或 `上次采集时间 + frequency_min <= 当前时间`
> 3. **交易时段**（如启用）: 仅 A股交易时段（9:30-11:30, 13:00-15:00）触发
>
> **举例**:
> - 当前时间 14:50，创建订阅 `frequency_min=10`
> - 调度器会在 14:51 检查时发现满足条件，立即触发
> - 预计 14:51-14:55 完成采集并入库
> - 可通过 `/api/v1/stats` 的 `records.price` 增长确认
>
> **如果超过 2*interval 分钟仍无数据**，检查：
> - 订阅 `enabled` 是否为 `true`
> - 是否在交易时段（股票）
> - `/api/v1/errors` 是否有失败记录
> - 数据源是否可用（腾讯/新浪）

---

**祝使用顺利！** 🎉

---

### 8. 基本面数据查询

> 目前支持股票基本面（PE/PB/市值/行业），数据来源：腾讯财经

```http
GET /api/v1/data?source=akshare&symbol=600519&data_type=fundamental
```

**响应示例**:
```json
{
  "symbol": "600519",
  "date": "2026-04-08",
  "price": 1440.02,
  "pe_ttm": 7.94,
  "pb": null,
  "total_mv": null,
  "circ_mv": null,
  "industry": null,
  "raw_data": "v_sh600519=\"1~贵州茅台~600519~...\"",
  "ingested_at": "2026-04-08T01:09:55"
}
```

**注意**：部分字段（如pb、total_mv）的索引映射仍在校准中，当前版本返回 `null`。可依赖 `pe_ttm` 和 `raw_data` 自行解析。

---

### 9. 订阅数据采集

#### 9.1 创建基本面订阅

```http
POST /api/v1/subscriptions
Content-Type: application/json

{
  "name": "600519基本面采集",
  "type": "akshare",
  "symbol": "600519",
  "data_types": ["fundamental"],
  "frequency_min": 1440,
  "trading_hours_only": false,
  "backup_sources": "tencent"
}
```

- `frequency_min`: 建议 `1440`（每天一次）或 `60`（每小时）
- `trading_hours_only`: 基本面数据不受交易时段限制，建议 `false`

#### 9.2 查看订阅列表

```http
GET /api/v1/subscriptions
```

---

**祝使用顺利！** 🎉
