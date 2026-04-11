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

## 8. 调用方注意事项

调用API时，如果失败：
1. **先检查服务健康**: `curl http://127.0.0.1:8084/api/v1/stats`
2. **如果健康检查失败** → 按本文件第4步排查服务状态
3. **如果健康检查成功但业务API失败** → 检查参数（source/symbol/data_type）是否正确
4. **重试机制**: 建议调用方实现指数退避重试（最多3次）

---

**文档版本**: v1.0
**最后更新**: 2026-04-07
