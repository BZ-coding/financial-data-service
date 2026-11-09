#!/usr/bin/env python3
"""
FastAPI 服务层
提供数据查询、订阅管理、监控接口
"""

import re
import sys
import os
import json
import time
import logging
import hashlib
import ipaddress
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import fin_indicators  # 中性指标库（纯函数，无 IO）
from fastapi import FastAPI, HTTPException, Query, Request
try:
    # 包内导入（`market_service.api`）—— **根治后的正规做法**
    from .safe_ops import safe_delete
except ImportError:
    # 平铺导入回退（`market_service/` 本身在 sys.path 上时）
    # ⚠️ 2026-10-30 实测：只留平铺导入时，`import market_service.api` 直接
    #    ModuleNotFoundError（safe_ops 是兄弟模块，不是包的子模块）。
    from safe_ops import safe_delete
# 统一安全删除（影响行数上限保护）
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import yaml

# 数据库导入
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from market_service.database import Database, now_tz, parse_date, TZ
# 注: 原先有 `from market_service.collectors.akshare import AKShareFetcher`,
# 但实际从未引用 (grep 全文件无 usage). 删除以避免 CI smoke test
# 依赖 akshare 时的 ModuleNotFoundError. 2026-07-03 修复.

from .database import Database, now_tz, parse_date, TZ
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

# ==================== 写接口 Basic Auth 门禁（2026-10-29 阶段2前置） ====================
#
# 背景：8084 的写接口（订阅增删改 / 工单 / 手动采集）原本完全无认证。
#       一旦将来被门户反代到公网，等于把无认证的写能力暴露到公网 ——
#       所以阶段 2 开启前必须先给写接口补认证（Chief 指令，最高优先）。
#
# 设计决策（都经过只读盘点，见服务端发到群里的报告）：
#   ① **只保护写，读全开放** —— 读才是门户的价值（/api/v1/data 等 12 个 GET 不动）。
#   ② **判定用两层**（防未来新增写接口漏配）：
#       层 1：显式写接口清单 `WRITE_PATHS`（8 个，2026-10-29 从运行中 OpenAPI 盘出来的）
#       层 2：**任何非 GET/HEAD 方法**兜底都要认证
#       并集 = 绝不漏。将来加写接口，改了方法就自动被兜底罩住。
#   ③ **不设网段白名单** —— 写操作一律要密码（LAN 免认证对写操作太宽松）。
#   ④ **fail-closed** —— 密码未配置时写接口一律 503 拒绝，绝不放行。
#   ⑤ 密码**另建**、不复用 doc-server 的（权限边界隔离：看报告的密码 ≠ 能改订阅；
#      泄露影响面隔离：一个泄露不拖另一个下水；轮换独立）。
#      注入方式与 doc-server 对齐：systemd LoadCredential= → CREDENTIALS_DIRECTORY。
#      ⚠️ 本服务是 **user 级**单元（user manager 以 zsd 身份读凭据文件），
#      所以源文件是 zsd 所有 0600（放在 ~/.config/market-data-service-credentials/），
#      不是 root-only —— 这是 user 单元的能力边界，已在文档里写明。
#   ⑥ 环境变量 `MARKET_API_PASSWORD` 仅作**本地调试/测试**兜底，生产单元不设它。
#   ⚠️ 已知残余（如实记录，不在本次范围）：
#     - `/api/v1/tickets`、`/api/v1/tickets/{id}/comment`、`/api/v1/collect` 是
#       纯 query 参数的 POST → **简单请求**，浏览器跨源发起时**不触发预检** →
#       Basic Auth 挡不住 CSRF（除非浏览器里缓存了 8084 的凭据）。
#       现状写接口仅 LAN 可达且不会被反代（阶段2 明确不反代写接口），风险面有限。
#       若将来要防，需要求自定义请求头（如 X-Requested-With）或校验 Origin。
#     - `POST /api/v1/subscriptions` 在文件里注册了**两次**（L303/L330，后者是死代码，
#       且 `trading_hours_only`/`backup_sources` 参数永远到不了实际 handler）——
#       历史遗留 bug，不在本次范围，已报告。

WRITE_PATHS = (
    ("POST", "/api/v1/subscriptions"),
    ("PUT", "/api/v1/subscriptions/{sub_id}"),
    ("DELETE", "/api/v1/subscriptions/{sub_id}"),
    ("PATCH", "/api/v1/subscriptions/{sub_id}/toggle"),
    ("POST", "/api/v1/tickets"),
    ("PATCH", "/api/v1/tickets/{ticket_id}/status"),
    ("POST", "/api/v1/tickets/{ticket_id}/comment"),
    ("POST", "/api/v1/collect"),
)
_WRITE_BASIC_REALM = 'Basic realm="Market Data Service", charset="UTF-8"'

import base64 as _base64
import hmac as _hmac


def _load_market_api_password() -> Optional[str]:
    """取写接口密码。顺序：LoadCredential 目录 → 环境变量（调试用）。

    返回 None = 未配置 → 调用方必须 fail-closed（503 拒绝）。
    """
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY", "")
    if cred_dir:
        try:
            p = Path(cred_dir) / "market-api-password"
            v = p.read_text(encoding="utf-8").strip()
            if v:
                return v
        except Exception:
            pass
    v = os.environ.get("MARKET_API_PASSWORD", "").strip()
    return v or None


def _path_is_write(request: Request) -> bool:
    """该请求是否命中显式写接口清单（层 1）—— **方法 + 路径模板都要匹配**。

    ⚠️ 2026-10-29 自测抓到的一个自己的 bug：
       最初只按**路径模板**匹配，忘了方法 → `GET /api/v1/subscriptions` 与
       `POST /api/v1/subscriptions` **共用同一个路径模板** → 读接口被误判成写接口
       → 读接口也 401（实测发现的，不是想出来的）。
       → 必须 (method, template) **成对**匹配。
    """
    path = request.url.path
    for m, tpl in WRITE_PATHS:
        if request.method != m:
            continue
        t_seg = tpl.split("/")
        p_seg = path.split("/")
        if len(t_seg) != len(p_seg):
            continue
        if all(a == b or a.startswith("{") for a, b in zip(t_seg, p_seg)):
            return True
    return False


def _is_write_request(request: Request) -> bool:
    """两层判定：显式清单 + 非 GET/HEAD 方法兜底。"""
    if request.method not in ("GET", "HEAD"):
        return True
    return _path_is_write(request)


def _safe_compare_digest(a: Any, b: Any) -> bool:
    """常量时间比较，**两边都先 encode 成 bytes**。

    ⚠️ **2026-10-29 验收员实测撞到的坑**：`hmac.compare_digest` 对**含非 ASCII 的 str**
       会抛 **TypeError**（它只支持 ASCII-only 的 str，或两边都是 bytes）。
       → 客户端送一个非 ASCII 的密码/签名 → TypeError → 冒泡成 **500**，
         而不是应有的 **401/403**（验收员第一次撞到时以为"所有写接口都坏了"）。

       修法：**两边都 `.encode("utf-8")`** → 类型统一 → 不会抛 TypeError，
       非 ASCII 的密码只是**比对不相等** → 正常返回 401/403。

    ⚠️ 这也顺带保证了"客户端送什么类型都不会把服务打成 500"。
    """
    try:
        return _hmac.compare_digest(
            str(a).encode("utf-8"), str(b).encode("utf-8"))
    except Exception:
        return False


def _check_write_auth(request: Request):
    """返回 (放行, reason)。密码未配置 → (False, "no_password") 即 fail-closed。"""
    pw = _load_market_api_password()
    if not pw:
        return False, "no_password_configured"
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Basic "):
        return False, "missing_credentials"
    try:
        decoded = _base64.b64decode(auth[6:]).decode("utf-8")
    except Exception:
        return False, "malformed_credentials"
    _, _, pwd = decoded.partition(":")
    if _safe_compare_digest(pwd, pw):
        return True, "ok"
    return False, "bad_credentials"


# ---------------------------------------------------------------------------
# CSRF 双保险（2026-10-29，Chief 批准，与 Basic Auth 叠加）
# ---------------------------------------------------------------------------
#
# 为什么要第二道：Basic Auth **挡不住 CSRF**。
#   浏览器的 HTTP Auth 缓存是**按 origin** 的：用户在 8084 上认证过一次后，
#   任意网站都能用 `fetch(..., {credentials:'include'})` 让浏览器**自动带上
#   已缓存的 Basic Auth** → 跨站页面就能改订阅、触发采集。
#
#   而 `/api/v1/tickets`、`/tickets/{id}/comment`、`/api/v1/collect` 是
#   **纯 query 参数的 POST** → 属于 CORS 定义的**简单请求** → **不触发预检**
#   → 浏览器直接就把请求发出去 → Basic Auth 被自动带上 → CSRF 成立。
#
# 两道保险（**任一不满足即 403**）：
#   ① **必须带自定义头 `X-Requested-With: market-api`**
#      → 自定义头会让请求变成**非简单请求** → 浏览器**必须先发 CORS 预检**，
#        而 8084 **没有 CORS 配置** → 预检拿不到允许头 → 跨站请求**发不出去**。
#        这是主力防线。
#   ② **Origin 校验**（Origin 存在时必须属于可信来源；`null` 一律拒绝）
#      → 双保险：即使将来给 8084 加了 CORS，预检放行了，Origin 这道还在。
#
# ⚠️ **不能只靠 Origin**：curl / 脚本**默认不带 Origin** → 只靠 Origin 会把
#    正常脚本消费方全挡掉。所以 Origin **缺失时不判负**，由 ① 兜底。
#    （curl 消费方需要自己加 `-H "X-Requested-With: market-api"`，已写进文档）
#
# ⚠️ **绝不加在读接口上** —— 读接口对消费方必须完全无感（dsa-mcp / 探针 / hermes
#    全部只 GET，零打断是硬要求）。本检查**只在写路径**执行。
CSRF_HEADER = "X-Requested-With"
CSRF_HEADER_VALUE = "market-api"
# 可信来源主机（Origin 的 host 部分）。8084 只在本机 / LAN 可达。
CSRF_TRUSTED_HOSTS = ("127.0.0.1", "localhost", "::1")
CSRF_TRUSTED_NETS = ("192.168.10.0/24",)


def _origin_host_trusted(origin: str) -> bool:
    """Origin 的 host 是否可信。"""
    if not origin or origin == "null":
        return False
    # 取 scheme://host[:port] 的 host 部分
    rest = origin.split("://", 1)[1] if "://" in origin else origin
    host = rest.split("/", 1)[0]
    if host.startswith("["):                      # [::1]:8084
        host = host[1:host.find("]")] if "]" in host else host
    else:
        host = host.rsplit(":", 1)[0] if host.count(":") == 1 else host
    if host.lower() in CSRF_TRUSTED_HOSTS:
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    for net in CSRF_TRUSTED_NETS:
        try:
            if ip in ipaddress.ip_network(net):
                return True
        except ValueError:
            continue
    return False


def _check_csrf(request: Request):
    """CSRF 双保险校验。返回 (放行, reason)。"""
    # ① 自定义头必须带（主力防线：让请求变成非简单请求 → 触发预检 → 跨站发不出去）
    if request.headers.get(CSRF_HEADER) != CSRF_HEADER_VALUE:
        return False, "missing_or_wrong_custom_header"
    # ② Origin（存在时）必须可信；null 一律拒绝
    origin = request.headers.get("origin")
    if origin is not None:
        if origin == "null":
            return False, "null_origin"
        if not _origin_host_trusted(origin):
            return False, "untrusted_origin"
    else:
        # 没有 Origin（curl/脚本）→ 走 Referer 兜底；也没有就靠 ① 兜底
        referer = request.headers.get("referer")
        if referer:
            ref_origin = referer.split("?", 1)[0]
            if not _origin_host_trusted(ref_origin):
                return False, "untrusted_referer"
    return True, "ok"


# ---------------------------------------------------------------------------
# 回源签名校验（门户阶段 2 反代 → 8084）
# ---------------------------------------------------------------------------
#
# 为什么需要：门户反代走回环访问 8084，8084 看来源是 127.0.0.1 → 走"LAN 免认证"。
#   如果只靠这个，门户就成了**放大器**：LAN 的开放读权限被放大到公网，
#   而且**分不清"这是门户的受控反代"还是"有人在 LAN 上直连"**。
#   → 门户回源必须带 HMAC 签名 + 时间戳；8084 侧验签。
#     带 X-Portal-Source: portal 的请求 → **必须验签通过**；
#     不带该头的请求 → 正常路径（LAN 直连 / dsa-mcp / 探针）**不受影响**。
#
# 签名算法：HMAC-SHA256(key, "<timestamp>\n<METHOD>\n<path>")
PORTAL_SOURCE_HEADER = "X-Portal-Source"
PORTAL_SIG_HEADER = "X-Portal-Signature"
PORTAL_TS_HEADER = "X-Portal-Timestamp"
PORTAL_KEY_PATH = Path(os.environ.get(
    "MARKET_PORTAL_PROXY_KEY",
    "/home/zsd/.config/market-data-service-credentials/portal-proxy-key"))
PORTAL_SIG_MAX_SKEW = 300   # 秒；防重放


def _check_portal_signature(request: Request):
    """校验门户回源签名。返回 (放行, reason)。"""
    try:
        key = PORTAL_KEY_PATH.read_text(encoding="utf-8").strip().encode("utf-8")
    except OSError:
        return False, "no_key"
    if not key:
        return False, "no_key"
    ts = request.headers.get(PORTAL_TS_HEADER, "")
    sig = request.headers.get(PORTAL_SIG_HEADER, "")
    if not ts or not sig:
        return False, "missing_signature_headers"
    try:
        t = int(ts)
    except ValueError:
        return False, "bad_timestamp"
    if abs(time.time() - t) > PORTAL_SIG_MAX_SKEW:
        return False, "timestamp_out_of_window"
    msg = f"{ts}\n{request.method.upper()}\n{request.url.path}".encode("utf-8")
    expected = _hmac.new(key, msg, hashlib.sha256).hexdigest()
    if not _safe_compare_digest(sig, expected):
        return False, "bad_signature"
    return True, "ok"


@app.middleware("http")
async def write_auth_middleware(request: Request, call_next):
    """写接口门禁（只拦写，放行读；fail-closed；无白名单）。

    顺序：**回源签名 → Basic Auth → CSRF 双保险**。

    ⚠️ 写接口被拒时**绝不落凭据**（Authorization 头不写日志），
    只记 method/path/client/reason。
    """
    # ── ⓪ 回源签名校验（对**声称来自门户反代**的请求）────────────────
    # 门户阶段 2 的反代会带 X-Portal-Source: portal + HMAC 签名 + 时间戳。
    # 这里校验：**签名错/超时/缺头 → 403**。
    # ⚠️ 不带 X-Portal-Source 的请求走正常路径（LAN 直连、消费方调用）不受影响。
    if request.headers.get("X-Portal-Source") == "portal":
        sig_ok, sig_reason = _check_portal_signature(request)
        if not sig_ok:
            try:
                ip = request.client.host if request.client else "unknown"
            except Exception:
                ip = "unknown"
            logger.warning(
                f"反代签名校验失败：{request.method} {request.url.path} 来自 {ip} "
                f"原因={sig_reason}")
            return JSONResponse(
                status_code=403,
                content={"detail": "回源签名校验失败", "reason": sig_reason},
                headers={"Cache-Control": "no-store"},
            )

    if not _is_write_request(request):
        return await call_next(request)

    ok, reason = _check_write_auth(request)
    if not ok:
        try:
            ip = request.client.host if request.client else "unknown"
        except Exception:
            ip = "unknown"
        if reason == "no_password_configured":
            logger.error(
                f"写接口拒绝：未配置访问密码（MARKET_API_PASSWORD / LoadCredential），"
                f"fail-closed 拒绝一切写操作。{request.method} {request.url.path} 来自 {ip}")
            return JSONResponse(
                status_code=503,
                content={"detail": "写接口未配置访问密码，已拒绝（fail-closed）"},
                headers={"Cache-Control": "no-store"},
            )
        logger.warning(
            f"写接口认证失败：{request.method} {request.url.path} 来自 {ip} 原因={reason}")
        return JSONResponse(
            status_code=401,
            content={"detail": "写接口需要 Basic Auth"},
            headers={
                "WWW-Authenticate": _WRITE_BASIC_REALM,
                "Cache-Control": "no-store",
            },
        )

    # ── Basic Auth 通过 → 再过 CSRF 双保险 ──────────────────────────
    csrf_ok, csrf_reason = _check_csrf(request)
    if not csrf_ok:
        try:
            ip = request.client.host if request.client else "unknown"
        except Exception:
            ip = "unknown"
        logger.warning(
            f"写接口 CSRF 拒绝：{request.method} {request.url.path} 来自 {ip} "
            f"原因={csrf_reason}（凭据已通过，来源不可信）")
        return JSONResponse(
            status_code=403,
            content={
                "detail": "写接口拒绝：CSRF 防护未通过",
                "reason": csrf_reason,
                "required_header": f"{CSRF_HEADER}: {CSRF_HEADER_VALUE}",
            },
            headers={"Cache-Control": "no-store"},
        )

    return await call_next(request)


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
                row = db.get_latest_price(symbol, source=source if source != "*" else None)
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
                return {"symbol": symbol, "items": rows, "count": len(rows), "days": days,
                        "as_of": (rows[-1].get("published") or rows[-1].get("trade_time") or rows[-1].get("nav_date")) if rows else None,
                        "frequency": "daily"}
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
                symbol_param = symbol if symbol and symbol != "*" else None
                rows = db.get_fund_flow(flow_type=flow_type, days=days, limit=limit, symbol=symbol_param)
                if sector_kind:
                    rows = [r for r in rows if r.get("sector_kind") == sector_kind]
                if not rows:
                    raise HTTPException(404, f"未找到 {flow_type} 数据" + (f" (symbol={symbol_param})" if symbol_param else ""))
                return {"flow_type": flow_type, "sector_kind": sector_kind, "symbol": symbol_param, "items": rows, "count": len(rows)}
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
            elif data_type == "stock_news":
                # 个股新闻 (东方财富/雪球); 支持港股 fallback (akshare 无公告)
                days = int(request.query_params.get("days", 30))
                limit = int(request.query_params.get("limit", 50))
                from datetime import datetime, timezone, timedelta
                TZ = timezone(timedelta(hours=8))
                cutoff = (datetime.now(TZ) - timedelta(days=days)).isoformat()
                cur = db.conn.execute("""
                    SELECT id, source, title, summary, link, symbol, published, ingested_at
                    FROM news_data
                    WHERE symbol = ? AND ingested_at >= ?
                    ORDER BY ingested_at DESC
                    LIMIT ?
                """, (symbol, cutoff, limit))
                rows = [dict(r) for r in cur.fetchall()]
                return {"symbol": symbol, "days": days, "items": rows, "count": len(rows)}
            elif data_type == "hkex_announcement":
                # 港交所披露易公告 (Phase 7, 2026-07-06) — source='hkex' 区分 stock_news
                days = int(request.query_params.get("days", 90))
                limit = int(request.query_params.get("limit", 50))
                from datetime import datetime, timezone, timedelta
                TZ = timezone(timedelta(hours=8))
                cutoff = (datetime.now(TZ) - timedelta(days=days)).isoformat()
                cur = db.conn.execute("""
                    SELECT id, source, title, summary, link, symbol, published, ingested_at
                    FROM news_data
                    WHERE symbol = ? AND source = 'hkex' AND ingested_at >= ?
                    ORDER BY ingested_at DESC
                    LIMIT ?
                """, (symbol, cutoff, limit))
                rows = [dict(r) for r in cur.fetchall()]
                return {"symbol": symbol, "days": days, "items": rows, "count": len(rows), "source_filter": "hkex"}
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
            return {"symbol": symbol, "data_type": "nav", "days": days, "items": rows,
                    "as_of": (str(rows[-1].get("nav_date")) if rows else None),
                    "frequency": "daily"}
        elif data_type == "price":
            rows = db.get_price_history(symbol, days)
            return {"symbol": symbol, "data_type": "price", "days": days, "items": rows,
                    "as_of": (rows[0].get("trade_time") if rows else None),
                    "frequency": "intraday_snapshot"}
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
    frequency_min: int = 60,
    trading_hours_only: bool = False,
    backup_sources: Optional[str] = None
):
    """创建新订阅。

    ⚠️ **2026-10-29 合并了原"死代码"版本的逻辑**（Chief 批准）：
       这个端点之前在文件里**注册了两次**，先注册的那份（活）**不透传**
       `trading_hours_only` / `backup_sources`，后注册的那份（死，永远不命中）
       才带这两个参数 —— 所以"建订阅时设 trading_hours_only"这个能力**一直调不通**
       （`db.add_subscription` 本身是支持这两个参数的，是 API 层没往下传）。

       现在直接**一次 INSERT 写全字段**（`db.add_subscription` 原生支持），
       不再走死代码版的"先 INSERT 再 UPDATE"两步 —— 少一次写、更原子。

    ⚠️ **默认行为完全不变**（回归硬要求）：
       不传 `trading_hours_only` → 默认 False → 落库 0；
       不传 `backup_sources`    → 默认 None  → 落库 NULL；
       与合并前逐字节一致。
    """
    try:
        sub_id = db.add_subscription(
            name=name,
            type_=type,
            config=config,
            data_types=data_types,
            frequency_min=frequency_min,
            symbol=symbol,
            trading_hours_only=trading_hours_only,
            backup_sources=backup_sources,
        )
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
    """删除订阅

    ⚠️ 2026-10-29：改走 safe_delete()（不再用 cursor.rowcount）——
    Python 3.12+ 的 sqlite3 statement cache 会让 rowcount 恒为 0，
    表现为"**删成功了却报 404 订阅不存在**"。
    safe_delete 另有**影响行数上限**保护（max_rows=1，删多了自动回滚）。
    """
    n = safe_delete(db.conn, "DELETE FROM subscriptions WHERE id = ?", (sub_id,),
                    max_rows=1, reason=f"API 删除订阅 id={sub_id}")
    if n == 0:
        raise HTTPException(404, detail="订阅不存在")
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

# ─────────────────────────────────────────────────────────────────────────────
# 指标端点（2026-10-31，指标服务：同时服务门户看板与模型）
#
# ⚠️ RSI 口径：本端点用 **Wilder 平滑**（与通达信/同花顺一致），
#    与 dsa-mcp 告警里用的 SMA 历史口径**不同**，两者数字会有差异，刻意保留。
#    响应里用 `rsi_method` 字段显式标注。
#
# ⚠️ 数据新鲜度：指标基于**最后一根日 K 线**，响应里必须带 `data_as_of`，
#    前端要显示出来，避免用户把盘后数据当实时。
# ─────────────────────────────────────────────────────────────────────────────
_SYMBOL_RE = re.compile(r"^[0-9A-Za-z.]{1,16}$")


def _validate_symbol(symbol: str) -> str:
    """symbol 校验（后端自己的防线，不依赖前端）。

    只允许数字/字母/点，长度 1–16 —— 与门户主机清单同一套收紧思路：
    这里不需要中文、不需要斜杠、不需要任何 URL 语义字符。
    """
    s = (symbol or "").strip()
    if not _SYMBOL_RE.match(s):
        raise HTTPException(422, "symbol 格式非法：只允许数字/字母/点，长度 1–16")
    return s


@app.get("/api/v1/indicator")
async def get_indicator(
    symbol: str = Query(..., description="股票/基金代码"),
    days: int = Query(60, ge=20, le=250, description="回看天数（K 线根数上限）"),
):
    """计算并返回单个标的的技术指标（只读）。

    数据链路：本库 price_data 盘中快照 → fin_indicators.bars_from_intraday 整形为日K
              → fin_indicators 计算指标（与 dsa-mcp 同一份代码）。
    """
    sym = _validate_symbol(symbol)
    try:
        items = db.get_price_history(sym, days)
        if not items:
            raise HTTPException(404, f"未找到 {sym} 的行情数据（price_data 无记录）")
        bars = fin_indicators.bars_from_intraday(items)
        if not bars:
            raise HTTPException(404, f"{sym} 的行情数据无法整形为日K线")

        import pandas as pd
        df = pd.DataFrame(bars)
        close = df["close"]
        last = float(close.iloc[-1])

        ma = fin_indicators.ma_series(df, (5, 20))
        ma5 = float(ma["MA5"].iloc[-1]) if pd.notna(ma["MA5"].iloc[-1]) else None
        ma20 = float(ma["MA20"].iloc[-1]) if pd.notna(ma["MA20"].iloc[-1]) else None

        macd = fin_indicators.macd_series(df)
        dif = macd["MACD_DIF"]; dea = macd["MACD_DEA"]; bar = macd["MACD_BAR"]
        death_cross = None
        if len(df) >= 2:
            death_cross = bool((dif.iloc[-2] >= dea.iloc[-2]) and (dif.iloc[-1] < dea.iloc[-1]))
        golden_cross = None
        if len(df) >= 2:
            golden_cross = bool((dif.iloc[-2] <= dea.iloc[-2]) and (dif.iloc[-1] > dea.iloc[-1]))

        hl = fin_indicators.high_low_n(df, df, n=20)

        return {
            "symbol": sym,
            # N1（2026-10-31）：**不返回 name** —— 库里没有可靠标的名来源
            # （minute_data.name 基本为空串、nav_data.fund_name 只覆盖基金），
            # 返回 null 只是噪音。MCP 工具 get_indicators 同样不带该键，两处一致。
            "data_as_of": bars[-1]["date"],
            "bars": len(bars),
            "close": round(last, 3),
            "ma": {
                "ma5": round(ma5, 3) if ma5 is not None else None,
                "ma20": round(ma20, 3) if ma20 is not None else None,
                "bias_ma5_pct": round((last - ma5) / ma5 * 100, 2) if ma5 else None,
            },
            "macd": {
                "dif": round(float(dif.iloc[-1]), 4),
                "dea": round(float(dea.iloc[-1]), 4),
                "bar": round(float(bar.iloc[-1]), 4),
                "death_cross": death_cross,
                "golden_cross": golden_cross,
            },
            "rsi": {
                "rsi6": round(float(fin_indicators.rsi_wilder(df, 6).iloc[-1]), 2),
                "rsi12": round(float(fin_indicators.rsi_wilder(df, 12).iloc[-1]), 2),
                "rsi24": round(float(fin_indicators.rsi_wilder(df, 24).iloc[-1]), 2),
            },
            "rsi_method": "wilder",
            "levels": {
                "high_20d": round(hl["high"], 3) if hl["high"] is not None else None,
                "low_20d": round(hl["low"], 3) if hl["low"] is not None else None,
                "at_20d_high": bool(hl["high"] is not None and last >= hl["high"]),
                "at_20d_low": bool(hl["low"] is not None and last <= hl["low"]),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"指标计算失败: {e}")
        raise HTTPException(500, "指标计算失败")


# ─────────────────────────────────────────────────────────────────────────────
# 监控页聚合端点（2026-11-01，阶段 1）
#
# 为什么聚合：门户反代限速 **30 次/分钟**（app/proxy.py）。若前端按标的循环
#   （8 只 × 行情+指标 = 16 次/轮，30 秒一轮 = 32 次/分）会直接撞限速，
#   还会连累其它反代功能。→ 一次请求返回全部，30 秒轮询只占 2 次/分钟。
#
# 数据源：全部来自本库（不跨线读 ~/.hermes/）。
# 清单：`config/watchlist.json`（用户可编辑，按 mtime 热加载，改完不用重启）。
# ─────────────────────────────────────────────────────────────────────────────
_SYMBOL_RE_DASH = re.compile(r"^[0-9A-Za-z.]{1,16}$")
_KIND_ALLOWED = ("stock", "fund")

# 缓存：stamp = (清单 mtime, 各标的最新数据时间) → 数据一变 stamp 就变，缓存自然失效；
#      另有 60 秒 TTL 兜底（防止"数据没变但统计口径变了"这类）
_DASH_CACHE: Dict[str, Any] = {"stamp": None, "at": 0.0, "payload": None}
_DASH_TTL_SECONDS = 60

# 股票指标的取数窗口 —— **必须与 /api/v1/indicator 的默认 days 相同**（60）
_INDICATOR_LOOKBACK_DAYS = 60
# 日线回退阈值：price_data bars < 20（= insufficient 阈值）才允许切 daily_data 源
_IND_FALLBACK_MIN_BARS = 20

_NEWS_SOURCE_KEYS = ("caijing", "eeo", "sina_live", "ths_hk")
_NEWS_DEFAULT_LIMIT = 20


def _norm_news_ts(value) -> Optional[str]:
    """把三种不同格式的时间戳归一化成 'YYYY-MM-DD HH:MM:SS'（本地时区）。

    实测三种（2026-11-01）：
      · eeo      `pub_date: "2026-1101"`      ← 少一个连字符（上游格式可疑），容错补齐
      · sina_live`create_time: "2026-11-01 00:49:40"`
      · ths_hk   `ctime: "1793458081"`        ← unix 秒（字符串）
    解析不出来 → None（调用方丢弃该条，不猜时间）
    """
    if value is None or value == "":
        return None
    # unix 时间戳（数字或纯数字字符串，10 位秒 / 13 位毫秒）
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        try:
            ts = float(value)
            if ts > 1e11:      # 毫秒
                ts /= 1000.0
            return datetime.fromtimestamp(ts, TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    s = str(value).strip().replace("T", " ")
    # "2026-1101" → "2026-11-01"（4 位年 + 4 位月日）
    m = re.match(r"^(\d{4})-(\d{2})(\d{2})$", s)
    if m:
        s = f"{m.group(1)}-{m.group(2)}-{m.group(3)} 00:00:00"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:19] if len(s) >= 19 else s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def _last_data_ts(symbol: str, kind: str) -> str:
    """该标的最新数据时间（用于缓存 stamp 失效判定）。走索引，很轻。"""
    try:
        if kind == "stock":
            row = db.conn.execute(
                "SELECT max(trade_time) FROM price_data WHERE symbol = ?", (symbol,)).fetchone()
        else:
            row = db.conn.execute(
                "SELECT max(nav_date) FROM nav_data WHERE symbol = ?", (symbol,)).fetchone()
        return str(row[0]) if row and row[0] else ""
    except Exception:
        return ""


def _stale_days(data_as_of) -> Optional[int]:
    """数据距今天数（按日期算）。解析不了 → None（调用方按"未知"处理）。"""
    if not data_as_of:
        return None
    raw = str(data_as_of)[:10].replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            d = datetime.strptime(raw if fmt == "%Y-%m-%d" else raw.replace("-", ""), fmt).replace(tzinfo=TZ)
            return (datetime.now(TZ) - d).days
        except ValueError:
            continue
    return None


def _quote_of(symbol: str, kind: str) -> Dict[str, Any]:
    """单只标的的最新行情。股票=实时价；基金=盘中估值优先、否则 T+1 净值。"""
    out: Dict[str, Any] = {"last": None, "change_pct": None,
                           "data_as_of": None, "estimated": False}
    if kind == "stock":
        row = db.get_latest_price(symbol)
        if row is None:
            # 资金流个股：price_data 没有它，用 fund_flow 最新快照行当 quote
            # （勘查实测 769 个 main_fund_rank 标的只有这张表有价/涨跌/净流入）
            try:
                fr = db.conn.execute(
                    "SELECT price, change_pct, main_net_inflow, main_net_pct, collected_at"
                    " FROM fund_flow_data WHERE flow_type='main_fund_rank' AND symbol = ?"
                    " ORDER BY collected_at DESC LIMIT 1", (symbol,)).fetchone()
            except Exception:
                fr = None
            if fr:
                out["last"] = float(fr[0]) if fr[0] is not None else None
                out["change_pct"] = round(float(fr[1]) * 100, 2) if fr[1] is not None else None
                out["main_net_inflow"] = float(fr[2]) if fr[2] is not None else None
                out["main_net_pct"] = float(fr[3]) if fr[3] is not None else None
                # ⚠️ fund_flow_data.collected_at 是 DEFAULT CURRENT_TIMESTAMP（UTC），
                #   必须转本地——与 L2 资金流列表 items[].data_as_of 同一口径。
                #   不转会让详情页比看板列表早 8 小时（同一张卡两个时区）。
                #   （本函数另外两处时间不能转：market_time 是行情时间、已是本地；
                #     estimate_time 来自上游行情、也是本地，见 estimate_nav_data
                #     的 estimate_time(03:11 本地) vs ingested_at(19:12 UTC)。）
                out["data_as_of"] = _utc_to_local(fr[4]) if fr[4] else None
            return out
        if row:
            last = row.get("price")
            prev = row.get("prev_close")
            out["last"] = float(last) if last is not None else None
            if out["last"] is not None and prev:
                try:
                    out["change_pct"] = round((out["last"] - float(prev)) / float(prev) * 100, 2)
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
            # ⚠️ **必须用市场时间，不是采集时间**（2026-11-01 实测踩到）：
            #   price_data.trade_time 是**采集时间**（离盘时段也在采，周日也会采），
            #   raw_data.raw.trade_time 才是**行情时间**。用采集时间会让用户以为
            #   "周日下午 13:25 还有实时价"，实际那是上一个交易日的收盘价。
            #   fin_indicators.market_time() 就是干这个的（与日K整形同一套逻辑）。
            mt = fin_indicators.market_time(row)
            # 腾讯港股的市场时间形如 "2026/10/30 16:08:12" → 统一成 ISO 破折号形式，
            # 否则前端显示与后端解析（_stale_days）会不一致
            out["data_as_of"] = mt.replace("/", "-") if mt else None
        return out

    # 基金：盘中估值优先（更新更勤），没有就退回 T+1 净值
    est = db.get_latest_estimate_nav(symbol)
    if est and est.get("estimate_nav") is not None:
        out["last"] = float(est["estimate_nav"])
        g = est.get("estimate_growth")
        if g is not None:
            try:
                out["change_pct"] = round(float(g), 2)
            except (TypeError, ValueError):
                pass
        ts = est.get("estimate_time")
        out["data_as_of"] = str(ts)[:19].replace("T", " ") if ts else None
        out["estimated"] = True
        return out
    nav = db.get_latest_nav(symbol)
    if nav:
        if nav.get("nav") is not None:
            out["last"] = float(nav["nav"])
        g = nav.get("change_pct")
        if g is not None:
            try:
                out["change_pct"] = round(float(g), 2)
            except (TypeError, ValueError):
                pass
        d = nav.get("nav_date")
        out["data_as_of"] = f"{str(d)[:10]} 00:00:00" if d else None
    return out


def _indicators_of(symbol: str, kind: str) -> Dict[str, Any]:
    """单只标的的指标。股票走盘中快照→日K；基金走净值→退化日K（只用 close）。"""
    import pandas as pd

    out: Dict[str, Any] = {
        "ma5": None, "ma20": None, "bias_ma5_pct": None,
        "macd": {"dif": None, "dea": None, "bar": None,
                 "death_cross": None, "golden_cross": None},
        "rsi6": None, "rsi12": None, "rsi24": None,
        "high_20d": None, "low_20d": None, "at_20d_high": None, "at_20d_low": None,
        "bars": 0, "data_as_of": None, "insufficient": True,
    }
    try:
        if kind == "stock":
            # ⚠️ **必须与 /api/v1/indicator 的 days 口径一致**（都是 60）：
            #   EMA 类指标（MACD/RSI）依赖预热长度，两个端点用不同 days
            #   会对同一标的给出不同数值 → 用户会以为其中一个算错了。
            bars = fin_indicators.bars_from_intraday(
                db.get_price_history(symbol, _INDICATOR_LOOKBACK_DAYS))
            # ★ 日线回退（2026-11-03 用户需求：资金流榜个股也要显示技术指标）：
            #   资金流榜 300 只在 price_data 里 0 条盘中快照 → bars=0 → 指标区一直空。
            #   回退到 daily_data（东财日线回填/增量，scripts/backfill_daily_kline.py），
            #   拼出与 bars_from_intraday **完全相同结构**的 bars（date/open/high/low/
            #   close/volume，按日期升序）→ 下游 ma_series/macd_series/rsi_wilder/
            #   high_low_n 纯函数零改动。
            #   ⚠️ 只在 price_data 严重不足（<20 根，即 insufficient 阈值）时才切源，
            #      且 daily_data 行数必须更多才替换——持仓股的既有路径零改动。
            #      daily 有真实 OHLC → has_hl=True（20日高低有意义）。
            if len(bars) < _IND_FALLBACK_MIN_BARS:
                try:
                    daily = db.get_daily_history(symbol, _INDICATOR_LOOKBACK_DAYS)
                    if daily and len(daily) > len(bars):
                        bars = [{"date": str(r["trade_date"]),
                                 "open": r["open"], "high": r["high"],
                                 "low": r["low"], "close": r["close"],
                                 "volume": float(r["vol"] or 0)} for r in daily]
                        logger.info("指标日线回退生效 %s: price bars→daily bars=%d",
                                    symbol, len(bars))
                except Exception as e:
                    logger.warning("指标日线回退失败 %s（保持 price_data 结果）: %s: %s",
                                   symbol, type(e).__name__, e)
            has_hl = True
        elif kind == "index":
            # 指数：index_data → 日K（**内置去重**：剔周末 + 复用快照，见 indexbars 文档）
            rows = [dict(r) for r in db.conn.execute(
                "SELECT trade_date, price, open, high, low, volume FROM index_data "
                "WHERE code = ? ORDER BY trade_date ASC", (symbol,)).fetchall()]
            bars = fin_indicators.bars_from_index(rows)
            has_hl = True         # 指数有真实 high/low
        else:
            bars = fin_indicators.bars_from_nav(db.get_nav_history(symbol, 400))
            has_hl = False        # 净值退化后 high/low 无意义 → 新高/新低不适用
    except Exception as e:
        logger.error(f"指标取数失败 {symbol}: {type(e).__name__}: {e}")
        return out

    out["bars"] = len(bars)
    if not bars:
        return out
    out["data_as_of"] = bars[-1]["date"]

    df = pd.DataFrame(bars)
    close = df["close"]
    last = float(close.iloc[-1])

    # 小数位数：股票 3 位（与 /api/v1/indicator 完全一致，便于用户对照）；
    #           基金 4 位（净值本身只有 1.0~1.2 量级，3 位会丢精度）
    nd = 3 if kind == "stock" else 4
    ma = fin_indicators.ma_series(df, (5, 20))
    ma5 = ma["MA5"].iloc[-1]
    ma20 = ma["MA20"].iloc[-1]
    if pd.notna(ma5):
        out["ma5"] = round(float(ma5), nd)
        if ma5:
            out["bias_ma5_pct"] = round((last - float(ma5)) / float(ma5) * 100, 2)
    if pd.notna(ma20):
        out["ma20"] = round(float(ma20), nd)

    macd = fin_indicators.macd_series(df)
    dif, dea, bar = macd["MACD_DIF"], macd["MACD_DEA"], macd["MACD_BAR"]
    out["macd"]["dif"] = round(float(dif.iloc[-1]), 4)
    out["macd"]["dea"] = round(float(dea.iloc[-1]), 4)
    out["macd"]["bar"] = round(float(bar.iloc[-1]), 4)
    if len(df) >= 2:
        out["macd"]["death_cross"] = bool((dif.iloc[-2] >= dea.iloc[-2]) and (dif.iloc[-1] < dea.iloc[-1]))
        out["macd"]["golden_cross"] = bool((dif.iloc[-2] <= dea.iloc[-2]) and (dif.iloc[-1] > dea.iloc[-1]))

    for p, key in ((6, "rsi6"), (12, "rsi12"), (24, "rsi24")):
        try:
            out[key] = round(float(fin_indicators.rsi_wilder(df, p).iloc[-1]), 2)
        except Exception:
            out[key] = None

    if has_hl:
        hl = fin_indicators.high_low_n(df, df, n=20)
        if hl["high"] is not None:
            out["high_20d"] = round(hl["high"], nd)
            out["at_20d_high"] = bool(last >= hl["high"])
        if hl["low"] is not None:
            out["low_20d"] = round(hl["low"], nd)
            out["at_20d_low"] = bool(last <= hl["low"])

    # 数据不足：bar 太少（<20 算不出 MA20）或最后一根太旧（>7 天，含周末）
    out["insufficient"] = False
    if out["bars"] < 20:
        out["insufficient"] = True
        out["insufficient_reason"] = f"样本不足（{out['bars']} 根 < 20）"
    elif out["data_as_of"]:
        try:
            last_dt = datetime.strptime(str(out["data_as_of"])[:10], "%Y-%m-%d").replace(tzinfo=TZ)
            if (datetime.now(TZ) - last_dt).days > 7:
                out["insufficient"] = True
                out["insufficient_reason"] = f"数据过期（最后 {out['data_as_of']}）"
        except Exception:
            pass
    return out


def _news_block(limit: int = _NEWS_DEFAULT_LIMIT) -> Dict[str, Any]:
    """4 个中文源的快讯：时间归一化 → 倒序 → 取前 N 条；同时报告每个源的状态。"""
    row = None
    try:
        row = db.get_latest_news_aggregator()
    except Exception as e:
        logger.error(f"聚合新闻读取失败: {type(e).__name__}: {e}")

    sources: Dict[str, Any] = {}
    items: List[Dict[str, Any]] = []
    if row:
        for key in _NEWS_SOURCE_KEYS:
            raw_items = row.get(f"{key}_items") or []
            status = row.get(f"{key}_status")
            count = row.get(f"{key}_count")
            sources[key] = {"status": status, "count": count if count is not None else len(raw_items)}
            for it in raw_items:
                if not isinstance(it, dict):
                    continue
                ts = _norm_news_ts(
                    it.get("create_time") or it.get("ctime") or it.get("pub_date")
                    or it.get("time") or it.get("date"))
                if not ts:
                    continue           # 时间解析不出来的丢掉（不猜）
                title = (it.get("title") or it.get("content") or it.get("digest") or "").strip()
                if not title:
                    continue
                items.append({
                    "ts": ts,
                    "source": key,
                    "title": title[:160],
                    "url": it.get("link") or it.get("url") or None,
                })
    else:
        for key in _NEWS_SOURCE_KEYS:
            sources[key] = {"status": "unavailable", "count": 0}

    items.sort(key=lambda x: x["ts"], reverse=True)
    return {
        "items": items[:limit],
        "total": len(items),
        "limit": limit,
        "sources": sources,
        "collected_at": (row or {}).get("collected_at"),
    }


def _health_block() -> Dict[str, Any]:
    """采集健康：成功率 / 近 24h 失败数 / 降级源。"""
    out: Dict[str, Any] = {"success_rate_24h": None, "failed_24h": None,
                           "success_24h": None, "degraded_sources": []}
    try:
        st = db.get_stats()
        c24 = st.get("collection_24h") or {}
        out["failed_24h"] = c24.get("failed")
        out["success_24h"] = c24.get("success")
        out["success_rate_24h"] = st.get("success_rate_24h")
    except Exception as e:
        logger.error(f"健康统计读取失败: {type(e).__name__}: {e}")
    try:
        rows = db.conn.execute(
            "SELECT source, data_type, success_rate, consecutive_success "
            "FROM source_metrics WHERE is_degraded = 1 OR success_rate < 0.9 "
            "ORDER BY success_rate ASC LIMIT 12").fetchall()
        out["degraded_sources"] = [
            {"source": r[0], "data_type": r[1],
             "success_rate": round(float(r[2]), 3) if r[2] is not None else None,
             "consecutive_success": r[3]}
            for r in rows
        ]
    except Exception as e:
        logger.error(f"降级源读取失败: {type(e).__name__}: {e}")
    return out


def _build_dashboard() -> Dict[str, Any]:
    # 2026-11-01：**改为按数据库真实数据**（与 /api/v1/catalog/holdings 同源）。
    #   原来读 watchlist.json（用户预设 8 只）—— 用户明确说"要我看现在有什么"，
    #   预设清单不对路 → watchlist.json 已删除，只保留 name_overrides.json（只补名字）。
    watchlist = _holdings_entries()
    quotes, indicators = [], []
    for entry in watchlist:
        sym, kind = entry["symbol"], entry["kind"]
        q = _quote_of(sym, kind)
        ind = _indicators_of(sym, kind)
        stale_days = _stale_days(q["data_as_of"])
        # 阈值：股票 3 天（覆盖周末+1天缓冲）、基金 4 天（净值 T+1 + 周末）
        threshold = 3 if kind == "stock" else 4
        quotes.append({
            "symbol": sym, "name": entry["name"], "kind": kind,
            "last": q["last"], "change_pct": q["change_pct"],
            "estimated": q["estimated"], "data_as_of": q["data_as_of"],
            "stale_days": stale_days,
            "stale": bool(stale_days is None or stale_days > threshold),
        })
        ind_out = {"symbol": sym, "kind": kind, "data_as_of": ind["data_as_of"]}
        ind_out.update({k: v for k, v in ind.items() if k != "data_as_of"})
        indicators.append(ind_out)
    return {
        "as_of": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "watchlist_size": len(watchlist),
        "quotes": quotes,
        "indicators": indicators,
        "news": _news_block(),
        "health": _health_block(),
    }


@app.get("/api/v1/dashboard")
async def get_dashboard():
    """持仓概览：行情 + 指标 + 新闻 + 健康，**一次请求返回全部**。

    ⚠️ **2026-11-01 改**：清单不再来自预设文件，而是**数据库里真实有数据的标的**
       （股票 = price_data 出现过的；基金 = nav_data 且有近期净值，排除陈年残留）。
       与 /api/v1/catalog/holdings 同源（同一个 `_holdings_entries()`）。
       `watchlist.json` 已删除；只剩 `config/name_overrides.json` 补名字。

    为什么聚合：门户反代限速 30 次/分钟，前端按标的循环必撞限速。
    缓存：stamp =（各标的 + 其最新数据时间）+ 60 秒 TTL。
    """
    global _DASH_CACHE
    try:
        entries = _holdings_entries()
        stamp = tuple((e["symbol"], e["kind"], _last_data_ts(e["symbol"], e["kind"]))
                      for e in entries)
        now = time.time()
        cached = _DASH_CACHE
        if (cached["payload"] is not None and cached["stamp"] == stamp
                and (now - cached["at"]) < _DASH_TTL_SECONDS):
            payload = dict(cached["payload"])
            payload["cached"] = True
            payload["cache_age_sec"] = round(now - cached["at"], 1)
            return payload

        payload = _build_dashboard()
        payload["cached"] = False
        payload["cache_age_sec"] = 0
        _DASH_CACHE = {"stamp": stamp, "at": now, "payload": payload}
        return payload
    except Exception as e:
        logger.exception(f"监控聚合失败: {e}")
        raise HTTPException(500, "监控聚合失败")


# ─────────────────────────────────────────────────────────────────────────────
# 目录（catalog）三层导航 + 详情（detail）—— 2026-11-01 阶段 2 改版
#
# 为什么改：用户要"看现在有什么、一层一层点进去"，**不要输入框**。
#   → 显示什么**由数据库真实数据决定**（探 DISTINCT symbol），不再用预设清单。
#   → 三层：L1 类别（6 类）→ L2 列表 → L3 详情。
#   → watchlist.json 降级为 name_overrides.json（只补库里取不到的名字）。
#
# ⚠️ 非交易日/复用快照的上游缺陷见 fin_indicators.indexbars 模块文档。
# ─────────────────────────────────────────────────────────────────────────────
_NAME_OVERRIDES_PATH = Path(__file__).parent / "config" / "name_overrides.json"
_NAME_OVERRIDES_CACHE: Dict[str, Any] = {"mtime": None, "names": {}}
_NAME_CACHE: Dict[str, str] = {}          # symbol -> name（进程内，落定就不变）

# 各品类"新鲜度阈值"（天）：超过就标 stale。数值按各自发布节奏定的：
#   指数 3（覆盖周末）｜持仓 3/4（股票/基金）｜资金流 2｜龙虎榜/涨停池 10（每日数据，超 10 天基本可判定停更）
_STALE_THRESHOLD = {
    "indices": 3, "holdings_stock": 3, "holdings_fund": 4,
    "fundflow": 2, "dragon": 10, "limitup": 10, "news": 1,
}

# 基金：最新净值超过这么多天就**判定为陈年残留、从"持仓"里排除**
# （实测 2026-11-01 命中 4 只：000001 止于 2002、270002 止于 2004、110011 止于 2008、161725 止于 4 月）
_FUND_RETENTION_DAYS = 30

# 资金流/龙虎榜/涨停池 这类"榜单"在 L2 最多返回多少条（一期不做分页/搜索）
_LIST_CAP = 50

# ⚠️ **按长度倒序**排列：长后缀必须先剥，否则 "-announcement" 会先把
#    "hk03690-hkex-announcement" 剥成 "hkex"，而 "-hkex-announcement" 再也匹配不上。
#    （2026-11-01 单测抓到这个顺序 bug）
_SUB_NAME_SUFFIXES = tuple(sorted((
    "_行情", "-行情", "(综合)", "（综合）", "(净值)", "（净值）",
    "基本面", "-announcement", "_announcement",
    "-hkex-announcement", "_hkex_announcement", "(港股)", "-hkex",
), key=len, reverse=True))


def _load_name_overrides() -> Dict[str, str]:
    """读 name_overrides.json（mtime 缓存）。读不到就返回空表，不影响其他逻辑。"""
    try:
        mtime = _NAME_OVERRIDES_PATH.stat().st_mtime
    except OSError:
        return {}
    if _NAME_OVERRIDES_CACHE["mtime"] == mtime:
        return _NAME_OVERRIDES_CACHE["names"]
    names: Dict[str, str] = {}
    try:
        raw = json.loads(_NAME_OVERRIDES_PATH.read_text(encoding="utf-8"))
        src = raw.get("names") if isinstance(raw, dict) else raw
        for k, v in (src or {}).items():
            k, v = str(k).strip(), str(v).strip()
            if k and v:
                names[k] = v[:40]
    except Exception as e:
        logger.error(f"读取 name_overrides.json 失败: {type(e).__name__}: {e}")
    _NAME_OVERRIDES_CACHE["mtime"], _NAME_OVERRIDES_CACHE["names"] = mtime, names
    return names


def _parse_subscription_name(symbol: str, name: str) -> Optional[str]:
    """从 subscriptions.name 里解析中文名（格式很脏，见 2026-11-01 实测样本）。

        002202金风科技(综合)     → 金风科技
        002202金风科技基本面     → 金风科技
        002202金风-announcement  → 金风
        hk03690_美团_行情        → 美团
    """
    if not name:
        return None
    s = str(name).strip()
    # 去掉开头的代码（允许后面跟 _ 或 -）
    low = s.lower()
    if low.startswith(symbol.lower()):
        s = s[len(symbol):]
    s = s.strip().lstrip("_- ")
    # 反复剥离已知后缀（可能叠加，如 "(综合)" 之后还有东西）
    changed = True
    while changed:
        changed = False
        for suf in _SUB_NAME_SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)].strip().rstrip("_- ")
                changed = True
    s = s.strip("_- ")
    if not s:
        return None
    # 名字必须是**中文名**：剥完只剩纯 ASCII 短标签（如 "hkex"、"announcement"）
    # 说明这行本来就没带名字 → 返回 None，让调用方回退到代码。
    if not any("\u4e00" <= ch <= "\u9fff" for ch in s):
        return None
    return s


def _utc_to_local(ts: str) -> str:
    """DB 时间字段（UTC）→ 本地时间字符串（Asia/Shanghai）。

    ⚠️ 库里 collected_at 存的是 UTC（CURRENT_TIMESTAMP 默认），但这个系统
    只服务一个时区——在 API 输出边界转一次比在 N 个前端位置转更可靠。
    前端不做时区换算，直接显示。
    （2026-11-03 用户报告：看板显示 17:43 实际是本地 01:43，差 8 小时）

    ⚠️ 必须先 replace(tzinfo=timezone.utc) 告诉 Python 输入是 UTC——
       否则 fromisoformat 返回 naive datetime，astimezone 视为已是本地时区 → 不转换。

    ⚠️ 输入**已带偏移**（如 `...00:49:33+08:00`）时不能再按 UTC 加 8 小时——
       那会把 00:49 变成 08:49（未来时间）。批评家 2026-11-03 03:00 就是看到
       这个 08:49 才判 news 时区仍没修好。旧实现用 `str(ts)[:19]` 盲目截掉偏移
       再当 UTC 处理，任何带偏移的入参都会错 8 小时。
       现在：能解析出带偏移的直接按绝对时刻换算成本地（+08:00 入参结果不变，
       未来若有人传入 +00:00 的串也能得到正确本地时间）；无偏移才按库里的 UTC 口径。
    """
    if not ts:
        return ts
    raw = str(ts).strip()
    try:
        dt = datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        dt = None
    if dt is not None:
        if dt.tzinfo is None:
            # 无偏移 = 库里 DEFAULT CURRENT_TIMESTAMP 的 UTC 口径
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
    # 兜底：非 ISO 串，维持旧行为（截 19 位当 UTC）
    try:
        dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
        return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return raw[:19]


def _resolve_name(symbol: str, kind: str) -> str:
    """标的名解析（多级回退，全部失败则返回代码）。结果进程内缓存。"""
    key = f"{kind}:{symbol}"
    if key in _NAME_CACHE:
        return _NAME_CACHE[key]
    name = None
    # ① 覆盖表（最高优先，用户可手工纠正）
    name = _load_name_overrides().get(symbol)
    # ② 指数：index_data.name
    if not name and kind == "index":
        try:
            row = db.conn.execute(
                "SELECT name FROM index_data WHERE code = ? AND name IS NOT NULL AND name != '' LIMIT 1",
                (symbol,)).fetchone()
            if row and row[0]:
                name = str(row[0]).strip()
        except Exception:
            pass
    # ③ 个股：fund_flow_data 快照里带 name（A股覆盖好）
    if not name and kind == "stock":
        for sql in ("SELECT name FROM fund_flow_data WHERE symbol = ? AND name IS NOT NULL AND name != '' LIMIT 1",
                    "SELECT name FROM dragon_tiger_data WHERE code = ? AND name IS NOT NULL AND name != '' LIMIT 1",
                    "SELECT name FROM limit_up_pool_data WHERE code = ? AND name IS NOT NULL AND name != '' LIMIT 1"):
            try:
                row = db.conn.execute(sql, (symbol,)).fetchone()
                if row and row[0]:
                    name = str(row[0]).strip()
                    break
            except Exception:
                continue
    # ④ 基金：estimate_nav_data.fund_name
    if not name and kind == "fund":
        try:
            row = db.conn.execute(
                "SELECT fund_name FROM estimate_nav_data WHERE symbol = ? AND fund_name IS NOT NULL AND fund_name != '' LIMIT 1",
                (symbol,)).fetchone()
            if row and row[0]:
                name = str(row[0]).strip()
        except Exception:
            pass
    # ⑤ 兜底：subscriptions.name 解析（港股唯一来源）
    if not name:
        try:
            rows = db.conn.execute(
                "SELECT name FROM subscriptions WHERE symbol = ? AND name IS NOT NULL AND name != ''",
                (symbol,)).fetchall()
            for r in rows:
                parsed = _parse_subscription_name(symbol, r[0])
                if parsed:
                    name = parsed
                    break
        except Exception:
            pass
    name = (name or symbol)[:40]
    _NAME_CACHE[key] = name
    return name


def _days_since(datestr) -> Optional[int]:
    """距今天数；解析不了返回 None。"""
    if not datestr:
        return None
    raw = str(datestr)[:10].replace("/", "-")
    try:
        d = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=TZ)
    except ValueError:
        return None
    return (datetime.now(TZ) - d).days


def _mk_stale(datestr, threshold: int) -> Dict[str, Any]:
    days = _days_since(datestr)
    return {
        "latest": str(datestr)[:19].replace("T", " ") if datestr else None,
        "stale": bool(days is None or days > threshold),
        "stale_days": days,
    }


# ── L2 各类列表 ─────────────────────────────────────────────────────────────

def _cat_indices() -> Dict[str, Any]:
    """指数列表（14 个）。latest 用**去重后最后一根真实交易日**，不用表内 max(trade_date)
    —— 因为上游在非交易日也写行、价格复用（见 indexbars 模块文档）。"""
    out = {"key": "indices", "name": "指数行情", "items": [], "total": 0}
    try:
        codes = [r[0] for r in db.conn.execute(
            "SELECT DISTINCT code FROM index_data WHERE code IS NOT NULL").fetchall()]
    except Exception as e:
        logger.error(f"指数清单读取失败: {type(e).__name__}: {e}")
        codes = []
    latest_real = None
    for code in sorted(codes):
        try:
            rows = [dict(r) for r in db.conn.execute(
                "SELECT trade_date, price, open, high, low, volume FROM index_data "
                "WHERE code = ? ORDER BY trade_date ASC", (code,)).fetchall()]
        except Exception:
            continue
        bars = fin_indicators.bars_from_index(rows)
        if not bars:
            continue
        last = bars[-1]
        prev_close = bars[-2]["close"] if len(bars) >= 2 else None
        pct = round((last["close"] - prev_close) / prev_close * 100, 2) if prev_close else None
        item = {
            "symbol": code,
            "name": _resolve_name(code, "index"),
            "kind": "index",
            "last": round(last["close"], 4),
            "change_pct": pct,
            "data_as_of": last["date"],
            "bars": len(bars),
        }
        out["items"].append(item)
        if latest_real is None or last["date"] > latest_real:
            latest_real = last["date"]
    out["total"] = len(out["items"])
    out.update(_mk_stale(latest_real, _STALE_THRESHOLD["indices"]))
    out["basis"] = "按 index_data 去重（剔周末 + 复用快照）后的最后一根"
    return out


def _holdings_entries() -> List[Dict[str, Any]]:
    """持仓 = 数据库里真实有数据的标的：
       股票 = price_data 里出现过的 symbol；
       基金 = nav_data 里出现、且**最新净值在保留期内**（排除陈年残留）。
    """
    entries: List[Dict[str, Any]] = []
    try:
        for (sym,) in db.conn.execute(
                "SELECT DISTINCT symbol FROM price_data WHERE symbol IS NOT NULL AND symbol != ''").fetchall():
            entries.append({"symbol": sym, "kind": "stock"})
    except Exception as e:
        logger.error(f"股票清单读取失败: {type(e).__name__}: {e}")
    try:
        rows = db.conn.execute(
            "SELECT symbol, max(nav_date) AS last FROM nav_data "
            "WHERE symbol IS NOT NULL AND symbol != '' GROUP BY symbol").fetchall()
        for sym, last in rows:
            days = _days_since(last)
            if days is None or days > _FUND_RETENTION_DAYS:
                continue          # 陈年残留 → 排除
            entries.append({"symbol": sym, "kind": "fund"})
    except Exception as e:
        logger.error(f"基金清单读取失败: {type(e).__name__}: {e}")
    for e in entries:
        e["name"] = _resolve_name(e["symbol"], e["kind"])
    return entries


def _cat_holdings() -> Dict[str, Any]:
    """持仓列表：行情 + 指标（股票走 price 日K，基金走 nav 日K）。"""
    out = {"key": "holdings", "name": "我的持仓", "items": [], "total": 0}
    latest = None
    for e in _holdings_entries():
        q = _quote_of(e["symbol"], e["kind"])
        ind = _indicators_of(e["symbol"], e["kind"])
        stale_days = _stale_days(q["data_as_of"])
        threshold = _STALE_THRESHOLD["holdings_stock" if e["kind"] == "stock" else "holdings_fund"]
        out["items"].append({
            "symbol": e["symbol"], "name": e["name"], "kind": e["kind"],
            "last": q["last"], "change_pct": q["change_pct"], "estimated": q["estimated"],
            "data_as_of": q["data_as_of"],
            "stale_days": stale_days,
            "stale": bool(stale_days is None or stale_days > threshold),
            "ma5": ind["ma5"], "ma20": ind["ma20"], "rsi6": ind["rsi6"],
            "macd_death_cross": ind["macd"]["death_cross"],
            "macd_golden_cross": ind["macd"]["golden_cross"],
            "at_20d_high": ind["at_20d_high"], "at_20d_low": ind["at_20d_low"],
            "bias_ma5_pct": ind["bias_ma5_pct"],
            "bars": ind["bars"], "insufficient": ind["insufficient"],
        })
        if q["data_as_of"] and (latest is None or q["data_as_of"] > latest):
            latest = q["data_as_of"]
    out["total"] = len(out["items"])
    out.update(_mk_stale(latest, _STALE_THRESHOLD["holdings_stock"]))
    return out


def _cat_fundflow(limit: int = None, offset: int = 0, sort: str = None) -> Dict[str, Any]:
    """资金流排行（个股主力净流入，支持分页与排序）。

    分页（2026-11-02 指标看板定稿）：每只个股只取**最新一批快照**里的行——
    表里同一个 symbol 有历史批次，直接 LIMIT/OFFSET 会翻页翻出旧数据。
    默认行为（limit=None）与旧版一致：最新一批按 rank_no 前 _LIST_CAP 条，兼容旧前端。
    """
    out = {"key": "fundflow", "name": "资金流排行", "items": [], "total": 0}
    try:
        # 数字诚实（2026-11-02 看板定稿）：total = **本批**实际条数（最新一次采集）；
        # 历史累计的 DISTINCT symbol 另放 total_history 供参考——769 点进去只有 20 条
        # 是欺骗性 UI，不能让前端拿到这种 total。
        total = db.conn.execute(
            "SELECT COUNT(*) FROM fund_flow_data WHERE flow_type='main_fund_rank'"
            " AND collected_at >= (SELECT datetime(max(collected_at),'-5 minutes')"
            "   FROM fund_flow_data WHERE flow_type='main_fund_rank')"
        ).fetchone()[0]
        out["total_history"] = int(db.conn.execute(
            "SELECT count(DISTINCT symbol) FROM fund_flow_data"
            " WHERE flow_type='main_fund_rank'").fetchone()[0] or 0)
        out["batch_as_of"] = _utc_to_local(str(db.conn.execute(
            "SELECT max(collected_at) FROM fund_flow_data WHERE flow_type='main_fund_rank'"
        ).fetchone()[0] or ""))[:19]
        # 只取最新一批（同一次 collected_at 采集的完整榜单）
        latest_ts = db.conn.execute(
            "SELECT max(collected_at) FROM fund_flow_data WHERE flow_type='main_fund_rank'"
        ).fetchone()[0]
        if latest_ts is None:
            out["note"] = "暂无数据"
            return out
        _win = (datetime.fromisoformat(latest_ts) - timedelta(minutes=5)) \
            .strftime("%Y-%m-%d %H:%M:%S")
        order = "rank_no ASC"   # 默认：官方榜rank
        if sort == "inflow":
            order = "main_net_inflow DESC"
        elif sort == "change_pct":
            order = "change_pct DESC"
        elif sort == "symbol":      # 按代码排序（批评家阻塞 1：前端按钮有真实价值，后端漏实现）
            order = "symbol ASC"
        if limit is None:
            limit = _LIST_CAP
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))
        rows = db.conn.execute(
            f"SELECT rank_no, symbol, name, price, change_pct, main_net_inflow, main_net_pct, collected_at "
            f"FROM fund_flow_data WHERE flow_type = 'main_fund_rank' AND collected_at >= ? "
            f"ORDER BY {order} LIMIT ? OFFSET ?", (_win, limit, offset)).fetchall()
        latest = None
        items = []
        for r in rows:
            items.append({
                "symbol": r[1], "name": (r[2] or _resolve_name(r[1], "stock"))[:40], "kind": "stock",
                "rank": r[0], "last": r[3], "change_pct": r[4],
                "main_net_inflow": r[5], "main_net_pct": r[6],
                "data_as_of": _utc_to_local(r[7]) if r[7] else None,
            })
            if r[7] and (latest is None or str(r[7]) > latest):
                latest = str(r[7])
        out["items"] = items
        out["total"] = int(total or 0)
        # latest 来自 collected_at（UTC）→ 转本地
        latest = _utc_to_local(latest) if latest else latest
        out.update(_mk_stale(latest, _STALE_THRESHOLD["fundflow"]))
        out["note"] = (f"第 {offset + 1}-{offset + len(items)} 条（本批共 {out['total']} 条）"
                       if limit is not None else f"显示前 {len(items)} 条（本批共 {out['total']} 条）")
    except Exception as e:
        logger.error(f"资金流排行读取失败: {type(e).__name__}: {e}")
        out.update(_mk_stale(None, _STALE_THRESHOLD["fundflow"]))
    return out


def _cat_dragon() -> Dict[str, Any]:
    """龙虎榜（实测停更：最新 2026-07-02）。"""
    out = {"key": "dragon", "name": "龙虎榜", "items": [], "total": 0}
    try:
        total = db.conn.execute("SELECT count(DISTINCT code) FROM dragon_tiger_data").fetchone()[0]
        latest_row = db.conn.execute("SELECT max(trade_date) FROM dragon_tiger_data").fetchone()
        latest = latest_row[0] if latest_row else None
        rows = db.conn.execute(
            "SELECT trade_date, code, name, close_price, change_pct, net_buy_amount "
            "FROM dragon_tiger_data ORDER BY trade_date DESC, net_buy_amount DESC LIMIT ?",
            (_LIST_CAP,)).fetchall()
        out["items"] = [{
            "symbol": r[1], "name": (r[2] or _resolve_name(r[1], "stock"))[:40], "kind": "stock",
            "trade_date": str(r[0])[:10] if r[0] else None,
            "last": r[3], "change_pct": r[4], "net_buy_amount": r[5],
            "data_as_of": str(r[0])[:10] if r[0] else None,
        } for r in rows]
        out["total"] = int(total or 0)
        out.update(_mk_stale(latest, _STALE_THRESHOLD["dragon"]))
        out["note"] = f"显示前 {len(out['items'])} 条（共 {out['total']} 只）"
    except Exception as e:
        logger.error(f"龙虎榜读取失败: {type(e).__name__}: {e}")
        out.update(_mk_stale(None, _STALE_THRESHOLD["dragon"]))
    return out


def _cat_limitup() -> Dict[str, Any]:
    """涨停池（实测停更：最新 2026-07-01）。"""
    out = {"key": "limitup", "name": "涨停池", "items": [], "total": 0}
    try:
        total = db.conn.execute("SELECT count(DISTINCT code) FROM limit_up_pool_data").fetchone()[0]
        latest_row = db.conn.execute("SELECT max(trade_date) FROM limit_up_pool_data").fetchone()
        latest = latest_row[0] if latest_row else None
        rows = db.conn.execute(
            "SELECT trade_date, code, name, price, change_pct, limit_times, amount "
            "FROM limit_up_pool_data ORDER BY trade_date DESC, amount DESC LIMIT ?",
            (_LIST_CAP,)).fetchall()
        out["items"] = [{
            "symbol": r[1], "name": (r[2] or _resolve_name(r[1], "stock"))[:40], "kind": "stock",
            "last": r[3], "change_pct": r[4], "limit_times": r[5], "amount": r[6],
            "data_as_of": str(r[0])[:10] if r[0] else None,
        } for r in rows]
        out["total"] = int(total or 0)
        out.update(_mk_stale(latest, _STALE_THRESHOLD["limitup"]))
        out["note"] = f"显示前 {len(out['items'])} 条（共 {out['total']} 只）"
    except Exception as e:
        logger.error(f"涨停池读取失败: {type(e).__name__}: {e}")
        out.update(_mk_stale(None, _STALE_THRESHOLD["limitup"]))
    return out


def _cat_news() -> Dict[str, Any]:
    """快讯（4 个中文源）。"""
    block = _news_block()
    out = {
        "key": "news", "name": "快讯",
        "items": block["items"], "total": block["total"],
        "sources": block["sources"], "collected_at": block.get("collected_at"),
    }
    # ⚠️ _news_block 已把 collected_at 从 UTC 转成本地时间（带 +08:00 后缀）
    #    这里不能再转（双重转换会把 00:49 变 08:49）
    _news_ts = block.get("collected_at", "")
    out.update(_mk_stale(str(_news_ts)[:19] if _news_ts else None, _STALE_THRESHOLD["news"]))
    return out


_CATALOG_BUILDERS = {
    "indices": _cat_indices,
    "holdings": _cat_holdings,
    "fundflow": _cat_fundflow,
    "dragon": _cat_dragon,
    "limitup": _cat_limitup,
    "news": _cat_news,
}
# 支持分页/排序参数的 builder（其余类别数据量小，前端排即可，保持原样）
_CATALOG_PAGED = {"fundflow"}
_CATALOG_ORDER = ("indices", "holdings", "fundflow", "dragon", "limitup", "news")


_CAT_CACHE: Dict[str, Any] = {"stamp": None, "at": 0.0, "payload": None}
_CAT_TTL_SECONDS = 60

# L2 分级缓存：key=类别 → {stamp, at, payload}
#   为什么需要：holdings 要算 12 只的完整指标（实测 ~0.95s），而用户在 L1↔L2 之间会来回点。
#   stamp 与 L1 同源（_catalog_stamp()），数据一变就全部失效；另有 60s TTL 兜底。
_L2_CACHE: Dict[str, Any] = {}


def _catalog_stamp() -> Any:
    """L1 的失效 stamp：各类别 count + 最新时间。任何一类有变化就重算。"""
    parts = []
    for cat in _CATALOG_ORDER:
        try:
            if cat == "indices":
                row = db.conn.execute("SELECT count(DISTINCT code), max(trade_date) FROM index_data").fetchone()
            elif cat == "holdings":
                a = db.conn.execute("SELECT count(DISTINCT symbol), max(trade_time) FROM price_data").fetchone()
                b = db.conn.execute("SELECT count(DISTINCT symbol), max(nav_date) FROM nav_data").fetchone()
                row = (a[0] + b[0], max(str(a[1] or ""), str(b[1] or "")))
            elif cat == "fundflow":
                row = db.conn.execute("SELECT count(DISTINCT symbol), max(collected_at) FROM fund_flow_data").fetchone()
            elif cat == "dragon":
                row = db.conn.execute("SELECT count(DISTINCT code), max(trade_date) FROM dragon_tiger_data").fetchone()
            elif cat == "limitup":
                row = db.conn.execute("SELECT count(DISTINCT code), max(trade_date) FROM limit_up_pool_data").fetchone()
            else:
                row = db.conn.execute("SELECT count(*), max(ingested_at) FROM news_aggregator_data").fetchone()
            parts.append((cat, row[0], str(row[1] or "")))
        except Exception:
            parts.append((cat, None, ""))
    return tuple(parts)


@app.get("/api/v1/catalog")
async def get_catalog():
    """L1：六个数据类别的元信息（计数 + 新鲜度）。**不含列表内容**，极轻。"""
    global _CAT_CACHE
    try:
        stamp = _catalog_stamp()
        now = time.time()
        if (_CAT_CACHE["payload"] is not None and _CAT_CACHE["stamp"] == stamp
                and (now - _CAT_CACHE["at"]) < _CAT_TTL_SECONDS):
            payload = dict(_CAT_CACHE["payload"])
            payload["cached"] = True
            payload["cache_age_sec"] = round(now - _CAT_CACHE["at"], 1)
            return payload

        cats = []
        for cat in _CATALOG_ORDER:
            try:
                block = _CATALOG_BUILDERS[cat]()
            except Exception as e:
                logger.error(f"类别 {cat} 构建失败: {type(e).__name__}: {e}")
                block = {"key": cat, "name": cat, "total": 0, "latest": None,
                         "stale": True, "stale_days": None, "items": []}
            # ⚠️ `count` 的口径**按类别不同**，故显式给 `unit`，避免歧义：
            #   指数/持仓/榜单 = 标的数；快讯 = **条目数**（不是采集次数）。
            #   实测：快讯 count=60（4 源条目合计）而 news_aggregator_data 只有 16 行快照。
            cats.append({
                "key": block["key"], "name": block["name"],
                # 数字诚实（2026-11-02）：fundflow 的 count 现在是**本批**条数
                # （20），历史累计另放 total_history；其余类别口径不变。
                "count": block.get("total", 0),
                "unit": "条" if block["key"] == "news" else "个",
                # latest 时区口径（2026-11-03 修）：
                #   UTC 列（CURRENT_TIMESTAMP）→ 需转本地：fundflow（collected_at）、news（ingested_at）
                #   日期型（DATE）或应用层本地 → 不转：indices/holdings/dragon/limitup
                "latest": block.get("latest"),
                "stale": block.get("stale", True),
                "stale_days": block.get("stale_days"),
                "batch_as_of": block.get("batch_as_of"),
            })
        payload = {
            "as_of": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "categories": cats,
            "cached": False, "cache_age_sec": 0,
        }
        _CAT_CACHE = {"stamp": stamp, "at": now, "payload": payload}
        return payload
    except Exception as e:
        logger.exception(f"目录聚合失败: {e}")
        raise HTTPException(500, "目录聚合失败")


@app.get("/api/v1/sectors")
async def get_sectors(
    kind: str = Query("industry", description="industry=行业 / concept=概念 / all=两组都返回"),
    limit: int = Query(None, ge=1, le=1000, description="每组条数（默认全部）"),
):
    """板块资金流（sector_fund_flow）：行业/概念两组，按净流入降序。

    2026-11-02 指标看板定稿：资金流分类页「板块」tab 的数据源。
    每个板块只取最新一批快照里的行；leading_stock 是**名字**（上游就没有代码），
    这里顺带用 main_fund_rank 的 name 列反查代码（leading_symbol），可点进个股详情。
    """
    kinds = [kind] if kind in ("industry", "concept") else ["industry", "concept"]
    out: Dict[str, Any] = {"groups": [], "as_of": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")}
    try:
        # 板块名 → 代码 反查表（同一批 main_fund_rank 里 name→symbol，进程内缓存即可，这里直接查）
        for k in kinds:
            # 批次窗口：取 max(collected_at) 往前 5 分钟内的行——
            # 496 次独立 insert 的时间戳各差 1 秒，精确 = 匹配只命中最后一秒的 2 行。
            latest_ts = db.conn.execute(
                "SELECT max(collected_at) FROM fund_flow_data"
                " WHERE flow_type='sector_fund_flow' AND sector_kind=?", (k,)).fetchone()[0]
            if latest_ts is None:
                out["groups"].append({"kind": k, "items": [], "total": 0})
                continue
            from datetime import timedelta
            window_start = (datetime.fromisoformat(latest_ts) - timedelta(minutes=5))                 .strftime("%Y-%m-%d %H:%M:%S")
            total = db.conn.execute(
                "SELECT COUNT(*) FROM fund_flow_data"
                " WHERE flow_type='sector_fund_flow' AND sector_kind=? AND collected_at >= ?",
                (k, window_start)).fetchone()[0]
            g_total_history = int(db.conn.execute(
                "SELECT COUNT(DISTINCT symbol) FROM fund_flow_data"
                " WHERE flow_type='sector_fund_flow' AND sector_kind=?", (k,)).fetchone()[0] or 0)
            lim = min(int(limit), 1000) if limit else total
            rows = db.conn.execute(
                "SELECT symbol, name, price, change_pct, main_net_inflow, main_net_pct,"
                " leading_stock, leading_change_pct, collected_at"
                " FROM fund_flow_data WHERE flow_type='sector_fund_flow' AND sector_kind=?"
                " AND collected_at >= ? ORDER BY main_net_inflow DESC LIMIT ?",
                (k, window_start, lim)).fetchall()
            # 领涨股名字 → 代码：查 **stock_basic_data**（全市场股票列表）。
            # 2026-11-03 A1（批评家点名的换源）：
            #   原来查 fund_flow_data(main_fund_rank) 历史全量，命中 143/495=28%——
            #   领涨股不一定在主力榜里；stock_basic_data 是全市场列表，
            #   命中 1431/1472=97%。2026-11-03 又用 akshare 回填了北交所 920 段
            #   354 只（scripts/backfill_stock_basic.py），北交所领涨股也可点了。
            #   ⚠️ 名字→代码可能一对多（跨市场同名 / 改名），按 updated_at 取最新；
            #      symbol 只在 (symbol, source) 内唯一，跨 source 允许重名。
            #   查不到就 None：前端按"领涨股名字可显示、代码不可点"容错。
            names = [r[6] for r in rows
                     if r[6] and r[6].strip() not in ("-", "—", "")]
            code_map: Dict[str, str] = {}
            if names:
                ph = ",".join("?" * len(names))
                try:
                    for r2 in db.conn.execute(
                        f"SELECT name, symbol FROM stock_basic_data"
                        f" WHERE name IN ({ph})"
                        f" ORDER BY updated_at DESC", names).fetchall():
                        code_map.setdefault(r2[0], r2[1])
                except Exception as e:
                    # 反查失败只影响"可点"，绝不能让整个板块接口 500
                    logger.warning("领涨股反查失败（leading_symbol 置空）: %s: %s",
                                   type(e).__name__, e)
            items = []
            for r in rows:
                items.append({
                    "symbol": r[0], "name": r[1], "kind": "sector",
                    "last": r[2], "change_pct": r[3],
                    "main_net_inflow": r[4], "main_net_pct": r[5],
                    "leading_stock": r[6],
                    "leading_symbol": code_map.get(r[6]),
                    "leading_change_pct": r[7],
                    "data_as_of": _utc_to_local(r[8]) if r[8] else None,
                })
            g = {"kind": k, "name": "行业板块" if k == "industry" else "概念板块",
                 "items": items, "total": int(total or 0),
                 "total_history": g_total_history}
            out["groups"].append(g)
        out["cached"] = False
        return out
    except Exception as e:
        logger.exception(f"板块数据读取失败: {e}")
        raise HTTPException(500, "板块数据读取失败")


@app.get("/api/v1/catalog/{cat}")
async def get_catalog_category(
    cat: str,
    limit: int = Query(None, ge=1, le=1000, description="每页条数（默认 50，兼容旧前端）"),
    offset: int = Query(0, ge=0, description="偏移"),
    sort: str = Query(None, description="fundflow 专用：inflow=按净流入 / change_pct=按涨跌幅；默认按官方 rank"),
):
    """L2：某一类别的列表。fundflow 支持 limit/offset/sort（2026-11-02 看板定稿）。

    缓存键带参数（不同分页各自缓存），stamp 失效策略不变。
    """
    if cat not in _CATALOG_BUILDERS:
        raise HTTPException(404, f"未知类别 {cat!r}；可用：{', '.join(_CATALOG_ORDER)}")
    try:
        stamp = _catalog_stamp()
        now = time.time()
        cache_key = f"{cat}:{limit}:{offset}:{sort}" if cat in _CATALOG_PAGED else cat
        cached = _L2_CACHE.get(cache_key)
        if (cached and cached["stamp"] == stamp and (now - cached["at"]) < _CAT_TTL_SECONDS):
            payload = dict(cached["payload"])
            payload["cached"] = True
            payload["cache_age_sec"] = round(now - cached["at"], 1)
            return payload

        if cat in _CATALOG_PAGED:
            block = _CATALOG_BUILDERS[cat](limit=limit, offset=offset, sort=sort)
        else:
            block = _CATALOG_BUILDERS[cat]()
        block["as_of"] = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        block["cached"] = False
        block["cache_age_sec"] = 0
        _L2_CACHE[cache_key] = {"stamp": stamp, "at": now, "payload": block}
        return block
    except Exception as e:
        logger.exception(f"类别 {cat} 失败: {e}")
        raise HTTPException(500, f"类别 {cat} 读取失败")


@app.get("/api/v1/detail")
async def get_detail(symbol: str = Query(..., description="标的代码（来自列表，用户不手输）")):
    """L3：单标的详情。**自动识别品种**：price_data→股票 / nav_data→基金 / index_data→指数。

    指数与基金都会走各自的 bars 适配器算指标（见 fin_indicators 对应模块）。
    """
    sym = (symbol or "").strip()
    if not _SYMBOL_RE_DASH.match(sym):
        raise HTTPException(422, "symbol 格式非法")
    try:
        # ① 识别品种（按数据存在性，优先级：指数 > 股票 > 基金）
        kind = None
        try:
            if db.conn.execute("SELECT 1 FROM index_data WHERE code = ? LIMIT 1", (sym,)).fetchone():
                kind = "index"
        except Exception:
            pass
        if kind is None:
            try:
                if db.conn.execute("SELECT 1 FROM price_data WHERE symbol = ? LIMIT 1", (sym,)).fetchone():
                    kind = "stock"
            except Exception:
                pass
        if kind is None:
            try:
                if db.conn.execute("SELECT 1 FROM nav_data WHERE symbol = ? LIMIT 1", (sym,)).fetchone():
                    kind = "fund"
            except Exception:
                pass
        # ④ 资金流个股（2026-11-02 指标看板勘查：769 个 main_fund_rank 标的
        #    只存在于 fund_flow_data，price_data/nav_data/index_data 都没有 →
        #    点详情全部 404。加第四级识别，quote 用资金流快照行）。
        if kind is None:
            try:
                if db.conn.execute(
                    "SELECT 1 FROM fund_flow_data WHERE flow_type='main_fund_rank'"
                    " AND symbol = ? LIMIT 1", (sym,)).fetchone():
                    kind = "stock"
            except Exception:
                pass
        if kind is None:
            raise HTTPException(404, f"库里没有 {sym} 的数据")

        name = _resolve_name(sym, kind)
        quote = _quote_of(sym, kind) if kind != "index" else None
        ind = _indicators_of(sym, kind)

        # 指数：quote 用去重后的 bars 末值（与指标同源，避免两处口径不同）
        if kind == "index":
            rows = [dict(r) for r in db.conn.execute(
                "SELECT trade_date, price, open, high, low, volume FROM index_data "
                "WHERE code = ? ORDER BY trade_date ASC", (sym,)).fetchall()]
            bars = fin_indicators.bars_from_index(rows)
            if bars:
                last = bars[-1]
                prev = bars[-2]["close"] if len(bars) >= 2 else None
                quote = {
                    "last": round(last["close"], 4),
                    "change_pct": round((last["close"] - prev) / prev * 100, 2) if prev else None,
                    "data_as_of": last["date"],
                    "estimated": False,
                }
            else:
                quote = {"last": None, "change_pct": None, "data_as_of": None, "estimated": False}

        threshold = {"stock": _STALE_THRESHOLD["holdings_stock"],
                     "fund": _STALE_THRESHOLD["holdings_fund"],
                     "index": _STALE_THRESHOLD["indices"]}[kind]
        stale_days = _stale_days(quote.get("data_as_of"))

        # 该标的的新闻（只有个股新闻表覆盖到的才有）
        news_items = []
        try:
            rows = db.conn.execute(
                "SELECT source, title, link, published FROM news_data WHERE symbol = ? "
                "ORDER BY published DESC LIMIT 10", (sym,)).fetchall()
            for r in rows:
                ts = _norm_news_ts(r[3])
                if not ts or not r[1]:
                    continue
                news_items.append({"ts": ts, "source": r[0], "title": str(r[1])[:160],
                                   "url": r[2]})
        except Exception:
            pass

        ann = []
        try:
            rows = db.conn.execute(
                "SELECT source, title, announcement_time, link FROM announcement_data "
                "WHERE symbol = ? ORDER BY announcement_time DESC LIMIT 10", (sym,)).fetchall()
            ann = [{"source": r[0], "title": str(r[1])[:160],
                    "ts": _norm_news_ts(r[2]), "url": r[3]} for r in rows]
        except Exception:
            pass

        return {
            "symbol": sym, "name": name, "kind": kind,
            "quote": quote,
            "stale": bool(stale_days is None or stale_days > threshold),
            "stale_days": stale_days,
            "indicators": ind,
            "news": news_items,
            "announcements": ann,
            "as_of": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"详情读取失败 {sym}: {e}")
        raise HTTPException(500, "详情读取失败")


# ==================== ETF / 指数画像（对标基金公司 ETF skill，2026-11-10） ====================
# 口径纪律：每个接口都带 as_of（数据日期）与 frequency（数据频率），
# 防止 AI/用户把不同频率的数据混用（南方 ETFirst Skill 的核心经验）。

@app.get("/api/v1/etf/profile")
async def etf_profile(code: str = Query(None, description="ETF 代码，缺省=全市场最新快照"),
                      trade_date: str = Query(None, description="数据日期 YYYY-MM-DD")):
    """ETF 基础画像：价格 / IOPV / 折溢价 / 成交。频率：daily。"""
    rows = db.get_etf_profile(code=code, trade_date=trade_date)
    return {"count": len(rows), "frequency": "daily",
            "as_of": (rows[0]["trade_date"] if rows else None), "items": rows}


@app.get("/api/v1/index/valuation")
async def index_valuation(index_code: str = Query(..., description="指数代码，如 000300")):
    """指数估值：PE(TTM/MR) + 股息率 + 5年分位。频率：daily（中证官网口径）。"""
    code = index_code.strip()
    row = db.get_index_valuation(code) or db.get_index_valuation(code.lstrip("0") or code)
    if not row:
        raise HTTPException(404, f"暂无 {index_code} 的估值数据")
    return row


@app.get("/api/v1/index/constituents")
async def index_constituents(index_code: str = Query(..., description="指数代码"),
                             top: int = Query(10, description="取前 N 大权重")):
    """指数前 N 大权重成份股。频率：weekly（中证官网月/周度更新）。"""
    data = db.get_index_top_weights(index_code, limit=top)
    if not data:
        raise HTTPException(404, f"暂无 {index_code} 的成份股数据")
    return data


@app.get("/api/v1/stock/index-membership")
async def stock_index_membership(symbol: str = Query(..., description="股票代码")):
    """个股 → 所属指数（最新一期），带权重。频率：weekly。"""
    rows = db.get_stock_index_membership(symbol)
    return {"symbol": symbol, "count": len(rows), "frequency": "weekly",
            "as_of": (rows[0]["as_of"] if rows else None), "items": rows}


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

# ==================== 告警历史（只读，给 App 用） ====================

ALERT_HISTORY_PATH = Path.home() / ".hermes" / "alert_history.jsonl"


def _alert_name_map() -> Dict[str, str]:
    """持仓 JSON → {symbol: 中文名}，用于告警列表显示。"""
    names: Dict[str, str] = {}
    portfolio_dir = Path.home() / ".hermes" / "portfolio"
    if not portfolio_dir.exists():
        return names
    for file in portfolio_dir.glob("*.json"):
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            for pos in data.get("positions", []) or []:
                sym = pos.get("symbol") or pos.get("code")
                name = (pos.get("name") or "").strip()
                if sym and name:
                    names[str(sym)] = name
        except Exception:
            continue
    return names


@app.get("/api/v1/alerts")
async def get_alerts(
    limit: int = Query(50, ge=1, le=500),
    symbol: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
):
    """最近告警（来自 alert_daemon 的 alert_history.jsonl），按时间倒序。"""
    if not ALERT_HISTORY_PATH.exists():
        return {"items": [], "total": 0}
    names = _alert_name_map()
    items: List[Dict[str, Any]] = []
    try:
        with ALERT_HISTORY_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except Exception:
                    continue
                ts = record.get("ts") or ""
                for group in record.get("groups", []) or []:
                    sym = str(group.get("sym") or "")
                    for sig in group.get("signals", []) or []:
                        sev = str(sig.get("severity") or "")
                        if symbol and sym != symbol:
                            continue
                        if severity and sev != severity:
                            continue
                        items.append({
                            "ts": str(sig.get("ts") or ts),
                            "symbol": sym,
                            "name": names.get(sym, sym),
                            "severity": sev,
                            "rule_id": sig.get("rule_id") or "",
                            "rule_name": sig.get("name") or "",
                            "reason": sig.get("reason") or "",
                        })
    except Exception as exc:
        logger.warning(f"读取告警历史失败: {exc}")
        return {"items": [], "total": 0}
    items.sort(key=lambda item: item["ts"], reverse=True)
    total = len(items)
    return {"items": items[:limit], "total": total}


# ==================== App 自定义告警规则（Basic Auth 写门禁沿用） ====================

ALERT_RULES_PATH = Path.home() / ".hermes" / "custom_alert_rules.json"
ALERT_RULE_FIELDS = {
    "price", "change_pct", "bias_ma5_pct", "rsi_14",
    "volume_ratio", "ma5", "ma20",
}
ALERT_RULE_OPS = {"gt", "gte", "lt", "lte"}
ALERT_RULE_SEVERITIES = {"low", "medium", "high"}


def _load_alert_rules() -> List[Dict[str, Any]]:
    if not ALERT_RULES_PATH.exists():
        return []
    try:
        data = json.loads(ALERT_RULES_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_alert_rules(rules: List[Dict[str, Any]]) -> None:
    ALERT_RULES_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = ALERT_RULES_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(ALERT_RULES_PATH)


@app.get("/api/v1/alert-rules")
async def list_alert_rules():
    return {"items": _load_alert_rules()}


@app.post("/api/v1/alert-rules")
async def upsert_alert_rule(request: Request):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(400, "规则必须是 JSON 对象")
    rule_id = str(payload.get("id") or "").strip()
    symbol = str(payload.get("symbol") or "").strip()
    field = str(payload.get("field") or "").strip()
    op = str(payload.get("op") or "").strip()
    severity = str(payload.get("severity") or "low").strip()
    try:
        threshold = float(payload.get("value"))
    except (TypeError, ValueError):
        raise HTTPException(400, "阈值必须是数字")
    if not rule_id or not symbol:
        raise HTTPException(400, "id/symbol 不能为空")
    if field not in ALERT_RULE_FIELDS:
        raise HTTPException(400, f"不支持的字段: {field}")
    if op not in ALERT_RULE_OPS:
        raise HTTPException(400, f"不支持的比较符: {op}")
    if severity not in ALERT_RULE_SEVERITIES:
        raise HTTPException(400, f"不支持的级别: {severity}")
    rule = {
        "id": rule_id,
        "symbol": symbol,
        "name": str(payload.get("name") or "").strip() or f"{symbol} {field} {op} {threshold:g}",
        "field": field,
        "field_label": str(payload.get("field_label") or field),
        "op": op,
        "value": threshold,
        "severity": severity,
        "enabled": bool(payload.get("enabled", True)),
    }
    rules = [r for r in _load_alert_rules() if r.get("id") != rule_id]
    rules.append(rule)
    _save_alert_rules(rules)
    return {"item": rule, "total": len(rules)}


@app.delete("/api/v1/alert-rules/{rule_id}")
async def delete_alert_rule(rule_id: str):
    rules = _load_alert_rules()
    kept = [r for r in rules if r.get("id") != rule_id]
    if len(kept) == len(rules):
        raise HTTPException(404, "规则不存在")
    _save_alert_rules(kept)
    return {"deleted": rule_id, "total": len(kept)}


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
