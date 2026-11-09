# 快速开始（金融数据采集系统）

## 一句话说明

FastAPI 服务，提供基金、股票、新闻数据查询，端口 8084。

## 一键启动

```bash
cd /opt/market_data_service
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python run_server.py
```

访问：http://localhost:8084/api/docs

---

## 主要接口

| 接口 | 说明 | 示例 |
|------|------|------|
| `GET /api/v1/stats` | 服务状态 | `curl http://localhost:8084/api/v1/stats` |
| `GET /api/v1/data` | 查询最新数据 | `?source=akshare&symbol=008114&data_type=nav` |
| `GET /api/v1/history` | 查询历史 | `?symbol=008114&data_type=nav&days=30` |

完整文档：http://localhost:8084/api-guide

---

## 停止与重启

```bash
# 前台运行按 Ctrl+C
# 后台运行
nohup python run_server.py > logs/app.log 2>&1 &

# 查看日志
tail -f logs/app.log

# 停止
pkill -f run_server.py
```

---

## 常见问题

**Q: 端口被占用？**  
A: 修改 `run_server.py` 的 port 参数。

**Q: 数据不更新？**  
A: 查看订阅配置：`SELECT * FROM subscriptions;`

**Q: 想加新标的？**  
A: 插入 `subscriptions` 表或调用 API 创建订阅。

---

**详细迁移文档**: `MIGRATION.md`  
**API完整文档**: `API_GUIDE.md`  
**运维手册**: `USER_GUIDE.md`
