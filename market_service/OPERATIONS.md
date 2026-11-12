# Market Data Service - 部署与调用说明

## 1. 服务信息

- **当前端口**: 8084
- **监听地址**: 0.0.0.0
- **访问地址**: http://192.168.10.70:8084
- **数据库**: SQLite (market_service/data/market.db)
- **运行方式**: 前台或后台 Python 进程

---

## 2. 启动与停止

### 2.1 启动服务

```bash
cd /vol1/@apphome/trim.openclaw/data/workspace
akshare_venv/bin/python market_service/run_server_8084.py
```

**后台启动（推荐）：**
```bash
cd /workspace
nohup akshare_venv/bin/python market_service/run_server_8084.py > /tmp/market_service.log 2>&1 &
```

### 2.2 停止服务

```bash
# 查找进程并杀死
pkill -f "run_server_8084.py"
# 或
kill $(pgrep -f "run_server_8084.py")
```

### 2.3 检查服务状态

```bash
# 1. 检查进程
ps aux | grep run_server_8084.py | grep -v grep

# 2. 检查端口监听
netstat -tlnp | grep 8084
# 或
ss -tlnp | grep 8084

# 3. 健康检查API
curl -s http://127.0.0.1:8084/api/v1/stats | python3 -m json.tool
```

期望输出包含：
```json
{
  "records": { "nav": ..., "price": ..., "news": ... },
  "scheduler": { "running": true, ... }
}
```

---

## 3. API 调用指南

### 3.1 基础信息

- **API文档**: http://192.168.10.70:8084/api/docs
- **管理页面**: http://192.168.10.70:8084/admin
- **统计接口**: `GET /api/v1/stats`

### 3.2 常用接口

#### 获取最新净值
```bash
curl "http://127.0.0.1:8084/api/v1/data?source=akshare&symbol=008114&data_type=nav"
```

#### 获取历史净值
```bash
curl "http://127.0.0.1:8084/api/v1/history?symbol=008114&data_type=nav&days=30"
```

#### 获取最近新闻
```bash
curl "http://127.0.0.1:8084/api/v1/data?source=akshare&symbol=*&data_type=news"
```

#### 查询订阅列表
```bash
curl "http://127.0.0.1:8084/api/v1/subscriptions"
```

---

## 4. 调用失败排查流程

### 步骤1: 检查服务是否运行

```bash
# 检查进程
ps aux | grep run_server_8084.py | grep -v grep

# 如果无输出，说明服务未启动
```

### 步骤2: 如果服务未启动，尝试启动

```bash
cd /workspace
akshare_venv/bin/python market_service/run_server_8084.py
```

观察终端输出，看是否有错误：
- 端口被占用 → 换端口或kill占用进程
- 配置文件缺失 → 检查 `config/config.yaml` 是否存在
- 数据库错误 → 检查 `market_service/data/market.db` 权限

### 步骤3: 检查网络连接

```bash
# 本地回环
curl -v http://127.0.0.1:8084/api/v1/stats

# 如果本地能通，外部不通，检查防火墙
# 临时关闭防火墙测试（仅家庭网络）
sudo ufw disable  # Ubuntu
# 或
sudo systemctl stop firewalld  # CentOS
```

### 步骤4: 查看服务日志

```bash
# 如果后台启动，查看输出日志
tail -f /tmp/market_service.log
```

日志中的关键信息：
- `INFO: Uvicorn running on http://0.0.0.0:8084` → 启动成功
- `RSS配置文件不存在` → 配置文件路径问题（可忽略，如果不需要RSS）
- `UNIQUE constraint failed` → 重复数据，正常（幂等性保证）

---

## 5. 常见问题

### Q1: Connection refused
**原因**: 服务未启动或端口错误
**解决**: 按步骤1-2启动服务

### Q2: 返回500错误
**原因**: 数据库异常或采集器异常
**解决**: 查看日志 `/tmp/market_service.log`，检查数据库文件是否存在且可写

### Q3: 数据为空
**原因**: 尚未采集或订阅配置不对
**解决**: 访问 `/admin` 查看订阅列表，检查 `last_collected` 时间；或手动运行 `test_integration.py` 测试采集

### Q4: 端口被占用
**原因**: 已有其他进程占用8084
**解决**:
```bash
# 查找占用进程
lsof -i:8084
# 杀死或改用其他端口（修改run_server_8084.py或创建新脚本）
```

---

## 6. 自动启动（可选）

如需开机自启，建议使用 **systemd**：

```ini
[Unit]
Description=Market Data Service
After=network.target

[Service]
Type=simple
User=trim.openclaw
WorkingDirectory=/vol1/@apphome/trim.openclaw/data/workspace
Environment="PYTHONPATH=/vol1/@apphome/trim.openclaw/data/workspace"
ExecStart=/vol1/@apphome/trim.openclaw/data/workspace/akshare_venv/bin/python /vol1/@apphome/trim.openclaw/data/workspace/market_service/run_server_8084.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

安装：
```bash
sudo cp market_service.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable market_service
sudo systemctl start market_service
```

查看状态：`sudo systemctl status market_service`

---

## 7. 重要文件位置

```
/workspace/
├── market_service/
│   ├── api.py                    # FastAPI服务
│   ├── database.py               # 数据库操作
│   ├── scheduler.py              # 调度器
│   ├── collectors/               # 采集器
│   ├── config/
│   │   ├── config.yaml           # 服务配置
│   │   └── rss_config.json       # RSS源配置
│   ├── static/admin.html         # 管理页面
│   ├── data/market.db            # 数据库文件
│   ├── run_server_8084.py        # 启动脚本
│   └── ...
├── akshare_venv/                 # Python虚拟环境
└── market_service.service        # systemd unit (可选)
```

---

## 8. 新鲜度探针（market_data_freshness.py）

每 30 分钟跑一次（systemd timer `market-freshness-probe.timer`），日志在
`logs/market-data-freshness.log`，异常时推飞书告警（hermes agent 群，与 dsa-mcp
金融告警同一目标）。

### 8.1 六项指标

| # | 指标 | 判据 | 回答的问题 |
|---|------|------|-----------|
| 1 | 各活跃源最近成功时间 | 超过该源阈值未成功 | 某个源静默坏了 |
| 2 | 全局失败率（近 60min） | > 85% | 采集链路整体退化 |
| 2b | 分 source 失败率（近 24h） | 成功率 < 80% | 全局混合会把小源的问题稀释掉 |
| 3 | 核心表行数增量 | 近 60min 新增 < 1 | 表停止写入 |
| 4 | 新闻类采集器成功次数 | 近 60min 成功 0 次 | news_data 行数受 UNIQUE 去重约束，不能用行数判存活 |
| 5 | 重复错误率（近 24h） | 同一错误占比 ≥ 70% | 接口稳定返回无效内容（非网络抖动） |
| 6 | **业务身份唯一性** | 行数 ≠ distinct 身份数 | **同一实体被存了多份（重复入库）** |

### 8.2 指标 6 为什么必须独立存在（2026-11-13 补）

2026-11-10 公告表出现重复入库（002202 实测 135 行 / 89 标题），**两个多月没人发现**，
原因是三层盲区叠加：

1. **探针只查「活没活」，从不查「是不是同一件事存了多份」**——而重复入库恰恰让表
   **长得更快**，行数增量指标只会认为「更健康」；公告表当时甚至完全不在 CORE_TABLES 里。
2. **探针当时是干跑模式**（`PROBE_DRY_RUN=1` 自 2026-10-26）且 `PROBE_FEISHU_TARGET`
   从未配置 → 就算判出异常也推不出去，只有日志。
3. **数据库层没有业务唯一约束兜底**（老库 `UNIQUE(link)` 拦不住链接变体）。

现在这三层都补上了：
- 指标 6 直接查「行数 vs distinct 身份数」，与「活没活」正交；
- 探针已退出干跑（`~/.config/systemd/user/market-freshness-probe.service.d/alert.conf`
  设 `PROBE_DRY_RUN=0` + `PROBE_FEISHU_TARGET`）；
- announcement_data 有 `announcement_id` 列 + `(source, symbol, announcement_id)` 部分唯一索引。

**身份键必须与表上的业务唯一约束完全一致（含 source 列）**：少一列会把不同来源的同一
实体误判成重复（假警），多一列会漏掉真重复。

### 8.3 手工跑一次

```bash
# 干跑（只写日志，不推送）
PROBE_DRY_RUN=1 market_service/venv/bin/python market_service/market_data_freshness.py

# 实跑（会推飞书）
systemctl --user start market-freshness-probe.service
journalctl --user -u market-freshness-probe -n 30 --no-pager
```

退出码 0 = 全部正常，1 = 检测到问题（unit 里 `SuccessExitStatus=0 1`，不算 systemd 故障）。

---

## 9. 调用方注意事项

调用API时，如果失败：
1. **先检查服务健康**: `curl http://127.0.0.1:8084/api/v1/stats`
2. **如果健康检查失败** → 按本文件第4步排查服务状态
3. **如果健康检查成功但业务API失败** → 检查参数（source/symbol/data_type）是否正确
4. **重试机制**: 建议调用方实现指数退避重试（最多3次）

---

**文档版本**: v1.1
**最后更新**: 2026-11-13
