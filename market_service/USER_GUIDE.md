# Market Data Service - 使用说明书

## 📖 目录

1. [概述](#概述)
2. [功能特性](#功能特性)
3. [系统要求](#系统要求)
4. [快速开始](#快速开始)
5. [配置说明](#配置说明)
6. [订阅管理](#订阅管理)
7. [API 参考](#api-参考)
8. [管理界面](#管理界面)
9. [故障排查](#故障排查)
10. [运维与监控](#运维与监控)
11. [数据源说明](#数据源说明)
12. [FAQ](#faq)

---

## 概述

Market Data Service 是一个轻量级的金融数据采集与服务平台，支持：

- ✅ **基金净值**（天天基金、东方财富）
- ✅ **股票实时行情**（腾讯、新浪、akshare多源降级）
- ✅ **财经新闻**（新浪财经）
- ✅ **自动调度**（可配置采集间隔）
- ✅ **交易时段限制**（A股交易时间智能控制）
- ✅ **多源降级**（主源失败自动切换备用源）
- ✅ **REST API**（FastAPI，自动文档）
- ✅ **Web Admin**（可视化配置管理）

---

## 功能特性

| 特性 | 说明 |
|------|------|
| **多数据源** | 支持多个数据源，主源失败自动降级 |
| **智能调度** | 基于时间间隔和交易时段的智能触发 |
| **幂等性** | 重复采集自动去重，保证数据一致性 |
| **容错机制** | 单次采集失败不影响后续调度 |
| **实时监控** | 提供健康检查、统计、错误日志API |
| **Web管理** | 可视化订阅配置、数据查看、日志查询 |
| **轻量级** | 纯Python实现，SQLite存储，单文件部署 |

---

## 系统要求

### 软件环境

- **Python**: 3.11+（推荐3.11或3.12）
- **操作系统**: Linux / macOS / Windows（Linux推荐）
- **网络**: 可访问外部财经API（腾讯、新浪等）

### 依赖库

```txt
fastapi==0.104.1
uvicorn[standard]==0.24.0
akshare==1.12.0  # 可选（主数据源当前不可用）
pandas==2.1.4
numpy==1.24.3
pydantic==2.5.0
pyyaml==6.0.1
requests==2.31.0
sqlite3  # Python内置
```

---

## 快速开始

### 1. 准备虚拟环境（可选）

```bash
cd /vol1/@apphome/trim.openclaw/data/workspace

# 如果已有虚拟环境，跳过
python3 -m venv akshare_venv

# 激活（Linux/macOS）
source akshare_venv/bin/activate

# Windows: akshare_venv\Scripts\activate
```

### 2. 安装依赖

```bash
# 进入虚拟环境后
pip install fastapi uvicorn[standard] pandas numpy pydantic pyyaml requests akshare
```

### 3. 启动服务

```bash
# 前台启动（调试）
python market_service/run_server_8084.py

# 后台启动（生产）
nohup python market_service/run_server_8084.py > /tmp/market_service.log 2>&1 &
```

### 4. 验证服务

```bash
# 健康检查
curl http://127.0.0.1:8084/api/v1/stats | python3 -m json.tool

# 预期输出
{
  "records": { "nav": 0, "price": 0, "news": 0 },
  "scheduler": { "running": true, ... },
  "uptime_seconds": 12.34
}
```

### 5. 访问管理页面

打开浏览器访问：
- **API文档**: http://localhost:8084/api/docs
- **管理界面**: http://localhost:8084/admin
- **健康检查**: http://localhost:8084/api/v1/stats

---

## 配置说明

### 配置文件

位置：`market_service/config/config.yaml`

```yaml
# 服务配置
api:
  host: "0.0.0.0"
  port: 8084
  reload: false          # 生产环境关闭热重载
  workers: 1             # SQLite不支持多进程

# 数据库配置
database:
  path: "market_service/data/market.db"
  wal_mode: true         # 启用WAL模式，提高并发
  journal_mode: "WAL"
  busy_timeout: 5000     # 忙等待5秒

# 调度器配置
scheduler:
  check_interval_seconds: 60  # 每分钟检查一次订阅
  max_concurrent: 3           # 最大并发采集数

# 日志配置
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

### 修改端口

编辑 `config.yaml` 中的 `api.port`，然后重启服务。

---

## 订阅管理

### 查看订阅列表

**API**:
```bash
curl http://127.0.0.1:8084/api/v1/subscriptions | python3 -m json.tool
```

**管理页面**: http://localhost:8084/admin

### 订阅字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | int | 订阅ID（自增） |
| `name` | str | 订阅名称（自定义） |
| `type` | str | 数据源类型：`akshare` |
| `symbol` | str | 代码（如`600519`、`008114`，基金或`*`表示所有） |
| `data_types` | json list | 数据类型：`["price"]`、`["nav"]`、`["news"]` |
| `frequency_min` | int | 采集间隔（分钟） |
| `trading_hours_only` | bool | 是否仅交易时段采集（A股：9:30-11:30, 13:00-15:00） |
| `backup_sources` | str | 备用源列表（逗号分隔，如`"tencent,sina"`） |
| `enabled` | bool | 是否启用 |
| `last_collected` | str | 上次采集时间（ISO格式） |

### 添加新订阅（通过数据库）

当前管理页面暂不支持编辑，需直接操作SQLite：

```bash
sqlite3 market_service/data/market.db

# 插入新订阅（示例：600519股票行情，10分钟间隔，交易时段限制，双源降级）
INSERT INTO subscriptions (name, type, config, symbol, data_types, frequency_min, trading_hours_only, backup_sources, enabled)
VALUES ('600519茅台行情', 'akshare', '{}', '600519', '["price"]', 10, 1, 'tencent,sina', 1);
```

### 启用/禁用订阅

```sql
UPDATE subscriptions SET enabled = 1 WHERE id = 3;   -- 启用
UPDATE subscriptions SET enabled = 0 WHERE id = 3;   -- 禁用
```

### 修改采集间隔

```sql
UPDATE subscriptions SET frequency_min = 5 WHERE id = 3;   -- 改为5分钟
```

### 修改备用数据源

```sql
UPDATE subscriptions SET backup_sources = 'tencent,sina' WHERE id = 3;
```

**可用数据源选项**：
- `tencent` - 腾讯财经（推荐）
- `sina` - 新浪财经（需Referer）
- `akshare_hist` - akshare历史数据（同主源，建议不用）

---

## API 参考

### 基础信息

- **Base URL**: `http://localhost:8084`
- **格式**: JSON
- **跨域**: 启用CORS

### 端点列表

#### 1. 健康检查

```http
GET /api/v1/stats
```

响应：
```json
{
  "records": {
    "nav": 120,
    "price": 450,
    "news": 30
  },
  "scheduler": {
    "running": true,
    "last_tick": "2026-04-07T14:52:00+08:00",
    "next_tick": "2026-04-07T14:53:00+08:00"
  },
  "uptime_seconds": 3600
}
```

#### 2. 查询最新数据

```http
GET /api/v1/data?source={source}&symbol={symbol}&data_type={data_type}
```

参数：
- `source`: 数据源（目前固定`akshare`）
- `symbol`: 代码（股票`600519`，基金`008114`，新闻`*`）
- `data_type`: 类型（`price`、`nav`、`news`）

示例：
```bash
curl "http://localhost:8084/api/v1/data?source=akshare&symbol=600519&data_type=price"
```

#### 3. 查询历史数据

```http
GET /api/v1/history?symbol={symbol}&data_type={data_type}&days={days}
```

参数：
- `symbol`: 代码
- `data_type`: `price`/`nav`/`news`
- `days`: 回溯天数（默认30）

示例：
```bash
curl "http://localhost:8084/api/v1/history?symbol=600519&data_type=price&days=7"
```

#### 4. 订阅列表

```http
GET /api/v1/subscriptions
```

返回所有订阅配置（JSON数组）。

#### 5. 错误日志

```http
GET /api/v1/errors?limit={limit}
```

获取最近采集失败记录。

参数：
- `limit`: 返回条数（默认10）

#### 6. API文档

```http
GET /api/docs
```

Swagger UI自动文档（推荐浏览器访问）。

---

## 管理界面

### 访问

http://localhost:8084/admin

### 功能

- 📊 **统计数据**: 各数据表记录数、服务运行时间
- 📋 **订阅列表**: 查看所有订阅配置（配置、间隔、状态、上次运行）
- 📜 **最近错误**: 查看最近采集失败记录（含错误信息）
- 🔄 **实时更新**: 每30秒自动刷新

### 订阅列表列说明

| 列名 | 说明 |
|------|------|
| ID | 订阅ID |
| 名称 | 订阅描述名称 |
| 类型 | 数据源类型（akshare） |
| Symbol | 股票/基金代码 |
| 数据类型 | price/nav/news |
| 频率(min) | 采集间隔 |
| 交易时段限制 | ✅ 是 / ❌ 否 |
| 备用源 | 降级数据源列表（如`tencent,sina`） |
| 状态 | 启用/禁用 |
| 上次运行 | 上次成功或失败时间 |

---

## 故障排查

### 服务无法启动

**现象**: 运行 `python run_server_8084.py` 无输出或立即退出

**排查**:
1. 检查Python版本：`python3 --version`（需3.11+）
2. 检查依赖是否安装：`pip list | grep fastapi`
3. 检查端口是否被占用：`lsof -i:8084`
4. 查看错误日志：`tail -f /tmp/market_service.log`

### API返回500错误

**原因**: 数据库异常、采集器异常

**排查**:
```bash
# 查看服务日志
tail -100 /tmp/market_service.log | grep -A5 "Traceback"

# 检查数据库文件
ls -lh market_service/data/market.db

# 测试数据库连接
python3 -c "from market_service.database import Database; db=Database('market_service/data/market.db'); print('OK')"
```

### 数据一直为空

**可能原因**:
1. 订阅未启用或配置错误
2. 不在交易时段（股票）
3. 外部API全部失败
4. 调度器未触发

**排查**:
```bash
# 1. 检查订阅状态
curl http://localhost:8084/api/v1/subscriptions | python3 -m json.tool

# 2. 检查采集日志
sqlite3 market_service/data/market.db "SELECT * FROM collection_log ORDER BY started_at DESC LIMIT 5;"

# 3. 手动触发测试
python3 -c "
import asyncio
from market_service.collectors.akshare import AKShareFetcher
from market_service.database import Database

async def test():
    db = Database('market_service/data/market.db')
    f = AKShareFetcher(30)
    result = await f.collect('600519', 'price', backup_sources=['tencent','sina'])
    print(result)
    db.close()
asyncio.run(test())
"
```

### 外部API不可用

当前股票实时行情依赖以下免费源：

| 数据源 | 状态 | 说明 |
|--------|------|------|
| 腾讯 (qt.gtimg.cn) | ✅ 稳定 | 主推 |
| 新浪 (hq.sinajs.cn) | ✅ 稳定 | 备用 |
| 东方财富 | ❌ 阻断 | 不可用 |

如果腾讯和新浪同时失败（概率极低），检查网络：
```bash
ping qt.gtimg.cn
ping hq.sinajs.cn
```

### 日志文件过大

日志位于 `/tmp/market_service.log`，定期清理：

```bash
# 查看大小
du -h /tmp/market_service.log

# 清空（服务运行时）
> /tmp/market_service.log

# 或配置logrotate（推荐）
```

---

## 运维与监控

### 查看服务状态

```bash
# 进程状态
ps aux | grep run_server_8084.py | grep -v grep

# 端口监听
ss -tlnp | grep 8084

# 资源占用
top -p $(pgrep -f run_server_8084.py)
```

### 查看实时日志

```bash
# 跟踪日志
tail -f /tmp/market_service.log

# 只看错误
tail -f /tmp/market_service.log | grep -E "ERROR|Traceback|failed"
```

### 重启服务

```bash
# 停止
pkill -9 -f "run_server_8084.py"

# 启动
cd /vol1/@apphome/trim.openclaw/data/workspace
nohup python market_service/run_server_8084.py > /tmp/market_service.log 2>&1 &

# 验证
sleep 3 && curl -s http://127.0.0.1:8084/api/v1/stats | python3 -m json.tool
```

### 开机自启（systemd）

创建 `/etc/systemd/system/market_data.service`：

```ini
[Unit]
Description=Market Data Service
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/vol1/@apphome/trim.openclaw/data/workspace
Environment="PYTHONPATH=/vol1/@apphome/trim.openclaw/data/workspace"
ExecStart=/vol1/@apphome/trim.openclaw/data/workspace/akshare_venv/bin/python /vol1/@apphome/trim.openclaw/data/workspace/market_service/run_server_8084.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

启用并启动：
```bash
sudo systemctl daemon-reload
sudo systemctl enable market_data
sudo systemctl start market_data
sudo systemctl status market_data
```

### 数据库维护

```bash
# 查看数据库大小
du -h market_service/data/market.db

# 备份
cp market_service/data/market.db market_service/data/market.db.backup_$(date +%Y%m%d)

# 清理旧日志（保留30天）
sqlite3 market_service/data/market.db "DELETE FROM collection_log WHERE started_at < datetime('now', '-30 days');"

# 查看表大小
sqlite3 market_service/data/market.db "SELECT name, COUNT(*) FROM nav_data GROUP BY name;"
```

---

## 数据源说明

### 可用数据源

| 名称 | 优先级 | 稳定性 | 速率限制 | 说明 |
|------|--------|--------|----------|------|
| **腾讯** | 1 | ⭐⭐⭐⭐⭐ | 无 | 直接HTTP，无需Header |
| **新浪** | 2 | ⭐⭐⭐⭐ | 需Referer | `hq.sinajs.cn`，带Referer头 |

### 数据源配置

在订阅中通过 `backup_sources` 字段配置（逗号分隔）：

```sql
-- 仅腾讯
UPDATE subscriptions SET backup_sources = 'tencent' WHERE id = 3;

-- 腾讯→新浪
UPDATE subscriptions SET backup_sources = 'tencent,sina' WHERE id = 3;
```

### 降级逻辑

采集时按顺序尝试：
1. `tencent` → 成功则记录 `source='tencent'`，结束
2. `tencent` 失败 → 尝试 `sina`
3. `sina` 失败 → 返回失败

---

## FAQ

### Q: 服务启动后，访问8084端口不通？

**A**:
- 检查服务是否启动：`ps aux | grep run_server_8084.py`
- 检查防火墙：`sudo ufw status`（关闭测试：`sudo ufw disable`）
- 检查端口监听：`ss -tlnp | grep 8084`
- 确认访问地址：内网用 `http://192.168.x.x:8084`，本机用 `http://localhost:8084`

### Q: 股票数据一直不更新？

**A**:
1. 检查订阅 `trading_hours_only` 是否在交易时段（非交易时段不会采集）
2. 检查 `frequency_min` 间隔是否太长
3. 查看采集日志是否有失败：`SELECT * FROM collection_log ORDER BY started_at DESC LIMIT 5;`
4. 手动测试数据源：`curl "https://qt.gtimg.cn/q=sh600519"`

### Q: 数据源失败，如何快速切换？

**A**:
修改订阅的 `backup_sources` 字段，将可用源放前面。例如：
```sql
UPDATE subscriptions SET backup_sources = 'tencent,sina' WHERE id = 3;
```

### Q: 如何添加新的订阅？

**A**:
插入数据库 `subscriptions` 表，参考现有记录。关键字段：
- `name`: 描述名称
- `symbol`: 代码（600519、008114、*）
- `data_types`: JSON数组 `["price"]` 或 `["nav"]` 或 `["news"]`
- `frequency_min`: 间隔（分钟）
- `trading_hours_only`: 是否交易时段限制（股票设为1，基金设为0）
- `backup_sources`: 数据源（`tencent,sina`）
- `enabled`: 1启用

### Q: 历史数据如何导出？

**A**:
```bash
# 导出价格数据为CSV
sqlite3 -header -csv market_service/data/market.db "SELECT * FROM price_data;" > price_data.csv

# 导出净值数据
sqlite3 -header -csv market_service/data/market.db "SELECT * FROM nav_data;" > nav_data.csv
```

### Q: 如何实现自己的数据源？

**A**:
1. 在 `market_service/collectors/` 创建新文件（如 `my Collector.py`）
2. 继承 `BaseCollector`，实现 `collect(symbol, data_type)` 方法
3. 在 `scheduler.py` 的 `COLLECTORS` 字典注册：
   ```python
   from .collectors.mycollector import MyCollector
   COLLECTORS = {
       'mycollector': MyCollector(),
   }
   ```
4. 订阅的 `type` 字段填 `mycollector`

详细参考 `collectors/base.py` 和 `collectors/akshare.py`。

### Q: 生产环境建议？

**A**:
- ✅ 使用虚拟环境隔离依赖
- ✅ 使用 systemd 自启动
- ✅ 定期备份数据库（`market.db`）
- ✅ 设置日志轮转（logrotate）
- ✅ 监控服务进程和端口
- ✅ 配置防火墙仅允许内网访问（如仅192.168.10.0/24）
- ✅ 使用Nginx反向代理（如果需要HTTPS或外部访问）

---

## 版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| v1.0 | 2026-04-07 | 初始版本，支持腾讯/新浪双源、交易时段限制、多源降级 |

---

**技术支持**: 如有问题，请查看日志 `/tmp/market_service.log` 或管理页面 `/admin`
