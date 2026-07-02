#!/usr/bin/env python3
"""
FastAPI 服务层
提供数据查询、订阅管理、监控接口
"""

import re
import sys
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import yaml

# 数据库导入
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from market_service.database import Database, now_tz, parse_date
from market_service.collectors.akshare import AKShareFetcher

from .database import Database, now_tz, parse_date
from .scheduler import Scheduler

logger = logging.getLogger(__name__)

# 计算项目根目录（market_service 的父目录）
PROJECT_ROOT = Path(__file__).parent.parent

# 加载配置
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)

# 初始化数据库（使用绝对路径）
db_path = PROJECT_ROOT / CONFIG['database']['path']
import sys
print(f"[DEBUG api.py] CONFIG['database']['path'] = {CONFIG['database']['path']}", file=sys.stderr)
print(f"[DEBUG api.py] PROJECT_ROOT = {PROJECT_ROOT}", file=sys.stderr)
print(f"[DEBUG api.py] db_path = {db_path}", file=sys.stderr)
db = Database(str(db_path))
print(f"[DEBUG api.py] db.db_path = {db.db_path}", file=sys.stderr)
scheduler = None  # 在startup时初始化

app = FastAPI(
    title=CONFIG['admin']['page_title'],
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# 挂载静态文件
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# ==================== 数据查询接口 ====================

@app.get("/api/v1/data")
async def get_data(
    request: Request,
    source: str = Query(..., description="数据源，如 akshare"),
    symbol: str = Query(..., description="股票/基金代码"),
    data_type: str = Query(..., description="数据类型: nav|price|news|index|fundamental|daily"),
    fresh: bool = Query(False, description="是否立即采集最新（否则从库中取）")
):
    """
    获取最新数据
    - fresh=false: 先查库，若无或过期则返回None
    - fresh=true: 立即触发采集，返回采集结果（同步阻塞）
    """
    try:
        if fresh:
            # TODO: 实时触发采集（暂时占位）
            raise HTTPException(501, "fresh模式尚未实现")
        else:
            if data_type == "nav":
                row = db.get_latest_nav(symbol)
                if not row:
                    raise HTTPException(404, "未找到数据")
                return row
            elif data_type == "price":
                # 直接返回最新数据，不做过期检查
                # 调用方可根据 trade_time 自行判断数据时效性
                row = db.get_latest_price(symbol)
                if not row:
                    raise HTTPException(404, "行情数据不存在")
                return row
            elif data_type == "news":
                rows = db.get_recent_news(source=source if source != "*" else None, hours=24)
                return {"items": rows, "count": len(rows)}
            elif data_type == "fundamental":
                row = db.get_latest_fundamental(symbol)
                if not row:
                    raise HTTPException(404, "未找到基本面数据")
                return row
            elif data_type == "estimate_nav":
                row = db.get_latest_estimate_nav(symbol)
                if not row:
                    raise HTTPException(404, "未找到盘中估算净值")
                return row
            elif data_type == "daily":
                row = db.get_latest_daily(symbol)
                if not row:
                    raise HTTPException(404, "未找到日线数据")
                return row
            elif data_type == "hsgt":
                rows = db.get_latest_hsgt()
                if not rows:
                    raise HTTPException(404, "未找到北向资金数据")
                return {"items": rows, "count": len(rows), "trade_date": rows[0]['trade_date'] if rows else None}
            elif data_type == "stock_basic":
                row = db.get_stock_basic(symbol)
                if not row:
                    raise HTTPException(404, "未找到股票基本信息")
                return row
            elif data_type == "index":
                rows = db.get_latest_index(symbol if symbol and symbol != "*" else None)
                if not rows:
                    raise HTTPException(404, "未找到指数数据")
                return rows
            elif data_type == "minute":
                from datetime import datetime, timezone, timedelta
                TZ = timezone(timedelta(hours=8))
                today = datetime.now(TZ).strftime('%Y-%m-%d')
                rows = db.get_minute_bars(symbol, today)
                if not rows:
                    raise HTTPException(404, "未找到分钟数据")
                return {"items": rows, "count": len(rows), "symbol": symbol, "date": today}
            elif data_type == "news_aggregator":
                row = db.get_latest_news_aggregator()
                if not row:
                    raise HTTPException(404, "未找到聚合新闻数据")
                return row
            elif data_type == "announcement":
                days = 90
                rows = db.get_announcements(symbol, days=days)
                if not rows:
                    raise HTTPException(404, f"未找到 {symbol} 的公告数据")
                return {"symbol": symbol, "items": rows, "count": len(rows), "days": days}
            elif data_type == "community":
                days = 30
                rows = db.get_community_posts(symbol, days=days)
                if not rows:
                    raise HTTPException(404, f"未找到 {symbol} 的社区数据")
                return {"symbol": symbol, "items": rows, "count": len(rows), "days": days}
            elif data_type == "fund_flow":
                flow_type = request.query_params.get("flow_type", "main_fund_rank")
                sector_kind = request.query_params.get("sector_kind")
                days = int(request.query_params.get("days", 1))
                limit = int(request.query_params.get("limit", 50))
                rows = db.get_fund_flow(flow_type=flow_type, days=days, limit=limit)
                if sector_kind:
                    rows = [r for r in rows if r.get("sector_kind") == sector_kind]
                if not rows:
                    raise HTTPException(404, f"未找到 {flow_type} 数据")
                return {"flow_type": flow_type, "sector_kind": sector_kind, "items": rows, "count": len(rows)}
            elif data_type == "fundamental":
                row = db.get_latest_fundamental(symbol)
                if not row:
                    raise HTTPException(404, f"未找到 {symbol} 的基本面数据")
                return row
            elif data_type == "market_stats":
                row = db.get_latest_market_stats()
                if not row:
                    raise HTTPException(404, "未找到市场涨跌统计 (sse+szse summary)")
                return row
            elif data_type == "limit_up_pool":
                trade_date = request.query_params.get("trade_date")
                limit = int(request.query_params.get("limit", 50))
                rows = db.get_limit_up_pool(trade_date=trade_date, limit=limit)
                return {"trade_date": trade_date, "items": rows, "count": len(rows)}
            elif data_type == "dragon_tiger":
                period = request.query_params.get("period")  # 今日/近一月/...
                days = int(request.query_params.get("days", 30))
                code = symbol if symbol and symbol != "*" else None
                rows = db.get_dragon_tiger(code=code, days=days, period=period)
                return {"symbol": code, "period": period, "days": days, "items": rows, "count": len(rows)}
            else:
                raise HTTPException(400, f"不支持的数据类型: {data_type}")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"查询失败: {e}")
        raise HTTPException(500, str(e))

@app.get("/api/v1/history")
async def get_history(
    symbol: str = Query(..., description="股票/基金代码"),
    data_type: str = Query(..., description="数据类型: nav|price"),
    days: int = Query(30, ge=1, le=365, description="查询最近N天")
):
    """获取历史数据"""
    try:
        if data_type == "nav":
            rows = db.get_nav_history(symbol, days)
            return {"symbol": symbol, "data_type": "nav", "days": days, "items": rows}
        elif data_type == "price":
            rows = db.get_price_history(symbol, days)
            return {"symbol": symbol, "data_type": "price", "days": days, "items": rows}
        else:
            raise HTTPException(400, f"不支持的历史数据类型: {data_type}")
    except Exception as e:
        logger.exception(f"历史查询失败: {e}")
        raise HTTPException(500, str(e))

@app.get("/api/v1/news/aggregator")
async def get_news_aggregator():
    """获取全网聚合新闻（来源：HackerNews + GitHub Trending + 同花顺）"""
    try:
        row = db.get_latest_news_aggregator()
        if not row:
            raise HTTPException(404, "未找到聚合新闻数据，请稍后重试")
        return row
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"聚合新闻查询失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/api/v1/news")
async def get_news_by_symbol(
    symbol: str = Query(..., description="股票代码"),
    days: int = Query(7, ge=1, le=90, description="最近 N 天"),
    source: str = Query(None, description="可选，按来源过滤 (akshare/rss/community_enhanced)")
):
    """按股票代码查询个股新闻（聚合 akshare + community_enhanced + rss）

    返回该 symbol 在 news_data 表中的最近 N 天新闻。
    注: news_data.symbol 由 collector 写入时填充，部分历史数据可能为空。
    """
    try:
        from datetime import datetime, timezone, timedelta
        TZ = timezone(timedelta(hours=8))
        cutoff = (datetime.now(TZ) - timedelta(days=days)).isoformat()
        cur = db.conn.execute("""
            SELECT id, source, title, summary, link, symbol, published, ingested_at
            FROM news_data
            WHERE symbol = ? AND ingested_at >= ?
              AND (? IS NULL OR source = ?)
            ORDER BY ingested_at DESC
            LIMIT 200
        """, (symbol, cutoff, source, source))
        rows = [dict(r) for r in cur.fetchall()]
        return {"symbol": symbol, "days": days, "source": source, "items": rows, "count": len(rows)}
    except Exception as e:
        logger.exception(f"按 symbol 查新闻失败: {e}")
        raise HTTPException(500, str(e))

# ==================== 订阅管理接口 ====================

@app.get("/api/v1/subscriptions")
async def list_subscriptions():
    """获取所有订阅"""
    subs = db.get_all_subscriptions()
    # 解析JSON字段
    for sub in subs:
        sub['data_types'] = json.loads(sub['data_types']) if isinstance(sub['data_types'], str) else sub['data_types']
        sub['config'] = json.loads(sub['config']) if isinstance(sub['config'], str) else sub['config']
    return {"subscriptions": subs}

@app.post("/api/v1/subscriptions")
async def create_subscription(
    name: str,
    type: str,  # akshare|rss|api
    config: Dict[str, Any],
    data_types: List[str],
    symbol: Optional[str] = None,
    frequency_min: int = 60
):
    """创建新订阅"""
    try:
        sub_id = db.add_subscription(
            name=name,
            type_=type,
            config=config,
            data_types=data_types,
            frequency_min=frequency_min,
            symbol=symbol
        )
        return {"id": sub_id, "status": "created"}
    except Exception as e:
        logger.exception(f"创建订阅失败: {e}")
        raise HTTPException(500, str(e))




@app.post("/api/v1/subscriptions")
async def create_subscription(
    name: str,
    type: str,  # akshare|rss|api
    config: Dict[str, Any],
    data_types: List[str],
    symbol: Optional[str] = None,
    frequency_min: int = 60,
    trading_hours_only: bool = False,
    backup_sources: Optional[str] = None
):
    """创建新订阅"""
    try:
        sub_id = db.add_subscription(
            name=name,
            type_=type,
            config=config,
            data_types=data_types,
            frequency_min=frequency_min,
            symbol=symbol
        )
        if trading_hours_only or backup_sources:
            db.conn.execute("""
                UPDATE subscriptions SET
                    trading_hours_only = ?,
                    backup_sources = ?
                WHERE id = ?
            """, (trading_hours_only, backup_sources, sub_id))
            db.conn.commit()
        return {"id": sub_id, "status": "created"}
    except Exception as e:
        logger.exception(f"创建订阅失败: {e}")
        raise HTTPException(500, str(e))

@app.put("/api/v1/subscriptions/{sub_id}")
async def update_subscription(
    sub_id: int,
    name: Optional[str] = None,
    symbol: Optional[str] = None,
    frequency_min: Optional[int] = None,
    trading_hours_only: Optional[bool] = None,
    backup_sources: Optional[str] = None,
    enabled: Optional[bool] = None
):
    """更新订阅配置"""
    cur = db.conn.execute("SELECT id FROM subscriptions WHERE id = ?", (sub_id,))
    if not cur.fetchone():
        raise HTTPException(404, detail="订阅不存在")

    updates = []
    params = []
    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if symbol is not None:
        updates.append("symbol = ?")
        params.append(symbol)
    if frequency_min is not None:
        updates.append("frequency_min = ?")
        params.append(frequency_min)
    if trading_hours_only is not None:
        updates.append("trading_hours_only = ?")
        params.append(trading_hours_only)
    if backup_sources is not None:
        updates.append("backup_sources = ?")
        params.append(backup_sources)
    if enabled is not None:
        updates.append("enabled = ?")
        params.append(enabled)

    if not updates:
        raise HTTPException(400, detail="没有提供更新字段")

    params.append(sub_id)
    sql = f"UPDATE subscriptions SET {', '.join(updates)} WHERE id = ?"
    db.conn.execute(sql, params)
    db.conn.commit()
    return {"id": sub_id, "status": "updated", "fields": updates}

@app.delete("/api/v1/subscriptions/{sub_id}")
async def delete_subscription(sub_id: int):
    """删除订阅"""
    cur = db.conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
    if cur.rowcount == 0:
        raise HTTPException(404, detail="订阅不存在")
    db.conn.commit()
    return {"id": sub_id, "status": "deleted"}

@app.patch("/api/v1/subscriptions/{sub_id}/toggle")
async def toggle_subscription(sub_id: int):
    """切换订阅启用/禁用状态"""
    cur = db.conn.execute("SELECT enabled FROM subscriptions WHERE id = ?", (sub_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, detail="订阅不存在")
    new_state = 0 if row[0] else 1
    db.conn.execute("UPDATE subscriptions SET enabled = ? WHERE id = ?", (new_state, sub_id))
    db.conn.commit()
    return {"id": sub_id, "enabled": bool(new_state)}


# ==================== 工单接口 ====================

@app.post("/api/v1/tickets")
async def create_ticket(
    title: str = Query(..., description="工单标题"),
    priority: str = Query("P3", description="优先级: P0,P1,P2,P3"),
    description: str = Query(..., description="问题描述")
):
    """提交报修工单"""
    try:
        ticket_id = db.create_ticket(
            title=title,
            priority=priority,
            description=description
        )
        return {"id": ticket_id, "status": "created"}
    except Exception as e:
        logger.exception(f"创建工单失败: {e}")
        raise HTTPException(500, str(e))

@app.get("/api/v1/tickets")
async def list_tickets(
    status: Optional[str] = Query(None, description="按状态筛选: open|investigating|resolved|closed"),
    limit: int = Query(50, ge=1, le=100, description="返回条数")
):
    """查看工单列表（管理员）"""
    tickets = db.get_tickets(status=status, limit=limit)
    return {"items": tickets, "count": len(tickets)}

@app.get("/api/v1/tickets/{ticket_id}")
async def get_ticket(ticket_id: int):
    """查看工单详情"""
    ticket = db.get_ticket(ticket_id)
    if not ticket:
        raise HTTPException(404, detail="工单不存在")
    return ticket

@app.patch("/api/v1/tickets/{ticket_id}/status")
async def update_ticket_status(
    ticket_id: int,
    status: str = Query(..., description="新状态: open|investigating|resolved|closed")
):
    """更新工单状态"""
    success = db.update_ticket_status(ticket_id, status)
    if not success:
        raise HTTPException(404, detail="工单不存在")
    return {"id": ticket_id, "status": status}

@app.post("/api/v1/tickets/{ticket_id}/comment")
async def add_ticket_comment(
    ticket_id: int,
    author: str = Query(..., description="评论者"),
    content: str = Query(..., description="评论内容")
):
    """添加评论/备注"""
    success = db.add_ticket_comment(ticket_id, author, content)
    if not success:
        raise HTTPException(404, detail="工单不存在")
    return {"id": ticket_id, "author": author, "status": "added"}

# ==================== 监控接口 ====================
# ==================== 监控接口 ====================
# ==================== 监控接口 ====================

@app.get("/api/v1/stats")
async def get_stats():
    """获取服务统计"""
    try:
        stats = db.get_stats()
        # 增加订阅数
        subs = db.get_all_subscriptions()
        stats['subscriptions'] = {
            'total': len(subs),
            'enabled': sum(1 for s in subs if s['enabled'])
        }
        # 采集器状态
        if scheduler:
            sched_status = scheduler.get_status()
            # 递归清理所有unhashable类型，确保JSON可序列化
            def make_json_safe(obj):
                if isinstance(obj, dict):
                    # key 也必须是 JSON 合法类型，tuple → 字符串
                    safe_dict = {}
                    for k, v in obj.items():
                        if isinstance(k, tuple):
                            k = ':'.join(str(x) for x in k)
                        safe_dict[k] = make_json_safe(v)
                    return safe_dict
                elif isinstance(obj, (set, frozenset)):
                    return [make_json_safe(x) for x in obj]
                elif isinstance(obj, list):
                    return [make_json_safe(x) for x in obj]
                elif isinstance(obj, tuple):
                    return list(obj)
                elif hasattr(obj, '__dict__'):
                    return make_json_safe(obj.__dict__)
                else:
                    return obj
            stats['scheduler'] = make_json_safe(sched_status)
        return stats
    except Exception as e:
        logger.exception(f"获取统计失败: {e}")
        raise HTTPException(500, str(e))

@app.get("/api/v1/errors")
async def get_recent_errors(hours: int = Query(1, ge=1, le=24)):
    """获取最近采集错误"""
    errors = db.get_recent_errors(hours)
    return {"errors": errors, "count": len(errors)}

@app.post("/api/v1/collect")
async def trigger_collect(
    source: str,
    symbol: str,
    data_type: str
):
    """手动触发单次采集"""
    if scheduler is None:
        raise HTTPException(503, "调度器未启动")

    # TODO: 实现（可调用collector.collect并返回结果）
    raise HTTPException(501, "手动采集暂未实现")

# ==================== 管理页面 ====================

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    """管理页面（简化版）"""
    # DEBUG
    import sys
    print(f"[DEBUG admin_page] db.db_path = {db.db_path}", file=sys.stderr)
    stats = db.get_stats()
    errors = db.get_recent_errors(2)
    subs = db.get_all_subscriptions()[:10]

    # 读取HTML模板
    html_path = Path(__file__).parent / "static" / "admin.html"
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()

    # 替换统计卡片
    total_records = stats['records']['nav'] + stats['records']['price'] + stats['records']['news']
    html = html.replace("{{ stats.records.nav + stats.records.price + stats.records.news }}", str(total_records))
    html = html.replace("{{ stats.records.nav }}", str(stats['records']['nav']))
    html = html.replace("{{ stats.records.price }}", str(stats['records']['price']))
    html = html.replace("{{ stats.records.news }}", str(stats['records']['news']))
    html = html.replace("{{ stats.active_subscriptions }}", str(stats['active_subscriptions']))
    html = html.replace("{{ \"%.1f\"|format(stats.database_size_mb) }} MB", f"{stats['database_size_mb']:.1f} MB")
    html = html.replace("{{ stats.success_rate_24h }}%", f"{stats['success_rate_24h']}%")
    html = html.replace("{{ generated_at }}", now_tz().strftime("%Y-%m-%d %H:%M:%S"))

    # 错误表格
    errors_html = ""
    for err in errors:
        errors_html += f"""<tr class="error">
    <td class="timestamp">{err['started_at']}</td>
    <td>{err['source']}</td>
    <td>{err['symbol'] or ''}</td>
    <td>{err['data_type']}</td>
    <td>{err['error_message']}</td>
</tr>"""
    if not errors:
        errors_html = '<tr><td colspan="5" class="success">✅ 最近2小时无错误</td></tr>'
    html = html.replace("{% for err in errors.errors %}...{% endfor %}", errors_html)
    html = html.replace("{{ errors.count }}", str(len(errors)))

    # 订阅表格
    subs_html = ""
    for sub in subs:
        subs_html += f"""<tr>
    <td>{sub['id']}</td>
    <td>{sub['name']}</td>
    <td>{sub['type']}</td>
    <td>{sub['symbol'] or '-'}</td>
    <td>{', '.join(sub['data_types']) if isinstance(sub['data_types'], list) else sub['data_types']}</td>
    <td>{sub['frequency_min']}</td>
    <td>{'✅ 是' if sub.get('trading_hours_only') else '❌ 否'}</td>
    <td>{sub.get('backup_sources') or '-'}</td>
    <td class="{'success' if sub['enabled'] else 'neutral'}">
        {'启用' if sub['enabled'] else '禁用'}
    </td>
    <td class="timestamp">{sub['last_collected'] or '从未'}</td>
</tr>"""
    # 直接替换整个 tbody 内容
    import re
    pattern = r'<tbody>{% for sub in subscriptions %}.*?{% endfor %}</tbody>'
    replacement = f'<tbody>{subs_html}</tbody>'
    html = re.sub(pattern, replacement, html, flags=re.DOTALL)
    html = html.replace("{{ subscriptions|length }}", str(len(subs)))

    # 链接部分
    html = html.replace('href="/api/v1/subscriptions"', 'href="/api/v1/subscriptions"')

    return HTMLResponse(content=html)

# ==================== 文档接口 ====================

@app.get("/guide", response_class=HTMLResponse)
async def user_guide():
    """使用说明书（Markdown格式）"""
    guide_path = PROJECT_ROOT / "market_service" / "static" / "USER_GUIDE.md"
    if not guide_path.exists():
        raise HTTPException(status_code=404, detail="使用说明书未找到")
    with open(guide_path, 'r', encoding='utf-8') as f:
        content = f.read()
    # 简单HTML包装，展示原始Markdown（避免依赖markdown库）
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>使用说明书 - Market Data Service</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
        h1, h2, h3 {{ color: #2c3e50; margin-top: 1.5em; }}
        code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-family: monospace; }}
        pre {{ background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; }}
        blockquote {{ border-left: 4px solid #ddd; padding-left: 15px; color: #666; margin: 20px 0; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background: #f8f9fa; }}
        .back-link {{ display: inline-block; margin-bottom: 20px; color: #3498db; text-decoration: none; }}
        .back-link:hover {{ text-decoration: underline; }}
        .header {{ border-bottom: 1px solid #eee; padding-bottom: 15px; margin-bottom: 30px; }}
    </style>
</head>
<body>
    <a href="/admin" class="back-link">← 返回管理页面</a>
    <div class="header">
        <h1>Market Data Service 使用说明书</h1>
        <p>版本: v1.0 | 最后更新: 2026-04-07</p>
    </div>
    <div class="markdown-body">
        <pre><code>{content.replace('<', '&lt;').replace('>', '&gt;')}</code></pre>
    </div>
</body>
</html>
"""
    return HTMLResponse(html)

@app.get("/api-guide", response_class=HTMLResponse)
async def api_guide():
    """API使用说明书（调用方文档）"""
    guide_path = PROJECT_ROOT / "market_service" / "static" / "API_GUIDE.md"
    if not guide_path.exists():
        raise HTTPException(status_code=404, detail="API说明书未找到")
    with open(guide_path, 'r', encoding='utf-8') as f:
        content = f.read()
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>API使用说明书 - Market Data Service</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
        h1, h2, h3 {{ color: #2c3e50; margin-top: 1.5em; }}
        code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-family: monospace; }}
        pre {{ background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; }}
        blockquote {{ border-left: 4px solid #ddd; padding-left: 15px; color: #666; margin: 20px 0; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background: #f8f9fa; }}
        .back-link {{ display: inline-block; margin-bottom: 20px; color: #3498db; text-decoration: none; }}
        .back-link:hover {{ text-decoration: underline; }}
        .header {{ border-bottom: 1px solid #eee; padding-bottom: 15px; margin-bottom: 30px; }}
    </style>
</head>
<body>
    <a href="/admin" class="back-link">← 返回管理页面</a>
    <div class="header">
        <h1>Market Data Service - API 使用说明书</h1>
        <p>版本: v1.0 | 最后更新: 2026-04-07 | 服务地址: 192.168.10.70:8084</p>
    </div>
    <div class="markdown-body">
        <pre><code>{content.replace('<', '&lt;').replace('>', '&gt;')}</code></pre>
    </div>
</body>
</html>
"""
    return HTMLResponse(html)

# ==================== 生命周期 ====================

@app.on_event("startup")
async def startup_event():
    """服务启动时初始化"""
    global scheduler
    logger.info("Market Data Service 启动中...")
    scheduler = Scheduler(db)
    await scheduler.start()
    logger.info("服务已就绪")

@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时清理"""
    global scheduler
    if scheduler:
        await scheduler.stop()
    db.close()
    logger.info("服务已停止")

# ==================== 本地测试 ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "market_service.api:app",
        host=CONFIG['api']['host'],
        port=CONFIG['api']['port'],
        reload=CONFIG['api']['reload'],
        workers=CONFIG['api']['workers']
    )
