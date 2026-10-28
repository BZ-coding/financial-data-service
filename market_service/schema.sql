-- Market Data Service - SQLite Schema
-- 版本: 1.1
-- 日期: 2026-06-23 (新增 fund_flow_data 表)

-- 注意：SQLite使用TEXT存储ISO8601时间字符串（带时区）
-- Python端通过适配器自动转换datetime对象

-- ============================================
-- 基金净值表（永久保存）
-- ============================================
CREATE TABLE IF NOT EXISTS nav_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    nav_date DATE NOT NULL,
    nav REAL NOT NULL,
    acc_nav REAL,
    change_pct REAL,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, symbol, nav_date)
);

CREATE INDEX IF NOT EXISTS idx_nav_lookup ON nav_data(symbol, nav_date DESC);
CREATE INDEX IF NOT EXISTS idx_nav_ingested ON nav_data(ingested_at DESC);

-- ============================================
-- 实时行情表（订阅的股票，保留60天）
-- ============================================
CREATE TABLE IF NOT EXISTS price_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_time TEXT NOT NULL,
    price REAL NOT NULL,
    volume INTEGER,
    amount REAL,
    high REAL,
    low REAL,
    open REAL,
    prev_close REAL,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, symbol, trade_time)
);

CREATE INDEX IF NOT EXISTS idx_price_lookup ON price_data(symbol, trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_price_cleanup ON price_data(ingested_at);

-- ============================================
-- 财经新闻表（保留30天）
-- ============================================
CREATE TABLE IF NOT EXISTS news_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    link TEXT NOT NULL UNIQUE,
    symbol TEXT,
    published TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_news_source ON news_data(source, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_symbol ON news_data(symbol);

-- ============================================
-- 订阅配置表
-- ============================================
-- ⚠️ 2026-10-26 列序修正（验收员实测发现）：
--    本表原先的列顺序与**生产库实际列顺序不一致**（后 4 列颠倒），导致
--      INSERT INTO subscriptions SELECT * FROM old.subscriptions  → 直接报错
--      sqlite3 .mode csv + .import                                → 静默错位
--    （SELECT * / .import 都按**列序**匹配，不是按列名！）
--    现在列序与生产库严格一致：
--      id,name,type,config,symbol,data_types,frequency_min,last_collected,
--      enabled,created_at,updated_at,trading_hours_only,backup_sources
--    今后**不要**再调整本表的列顺序；要加列请追加到**末尾**，
--    且必须同时用生产库 sqlite_master 的原样 CREATE 覆盖（保持与生产库一致）。
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    config TEXT NOT NULL,
    symbol TEXT,
    data_types TEXT NOT NULL,
    frequency_min INTEGER DEFAULT 60,
    last_collected TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- 仅交易时段采集（A股：9:30-11:30, 13:00-15:00，且须是交易日）
    trading_hours_only BOOLEAN DEFAULT FALSE,
    -- 备用数据源列表（逗号分隔，如 "akshare_hist,tushare"）
    backup_sources TEXT
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled ON subscriptions(enabled);
CREATE INDEX IF NOT EXISTS idx_subscriptions_due ON subscriptions(last_collected, frequency_min);

-- ============================================
-- 采集日志表
-- ============================================
CREATE TABLE IF NOT EXISTS collection_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER,
    source TEXT NOT NULL,
    symbol TEXT,
    data_type TEXT NOT NULL,
    status TEXT NOT NULL,
    items_fetched INTEGER DEFAULT 0,
    items_stored INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    duration_sec REAL,
    FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_log_time ON collection_log(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_log_source ON collection_log(source, status);

-- ============================================
-- 大盘指数数据表
-- ============================================
CREATE TABLE IF NOT EXISTS index_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,           -- 指数代码，如 sh000001
    name TEXT NOT NULL,           -- 指数名称，如 上证指数
    price REAL,                   -- 最新价
    change_pct REAL,              -- 涨跌幅（%）
    change_amount REAL,           -- 涨跌额
    volume REAL,                  -- 成交量
    amount REAL,                  -- 成交额
    high REAL,                    -- 最高价
    low REAL,                    -- 最低价
    open REAL,                    -- 今开
    prev_close REAL,              -- 昨收
    source TEXT DEFAULT 'tencent_sina',
    trade_date DATE NOT NULL,      -- 交易日期
    trade_time TEXT,              -- 采集时间（ISO格式）
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_index_lookup ON index_data(code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_index_date ON index_data(trade_date DESC);

-- ============================================
-- 公告数据表（保留180天）
-- ============================================
CREATE TABLE IF NOT EXISTS announcement_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'cninfo',
    symbol TEXT NOT NULL,
    title TEXT NOT NULL,
    announcement_time TEXT,
    link TEXT NOT NULL UNIQUE,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ann_lookup ON announcement_data(symbol, announcement_time DESC);
CREATE INDEX IF NOT EXISTS idx_ann_cleanup ON announcement_data(ingested_at);

-- ============================================
-- 分钟数据表（股票/ETF分钟K线，保留60天）
-- ============================================
CREATE TABLE IF NOT EXISTS minute_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    name TEXT,
    trade_date TEXT NOT NULL,
    trade_time TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    amount REAL,
    source TEXT DEFAULT "sina",
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, trade_time)
);
CREATE INDEX IF NOT EXISTS idx_minute_symbol_time ON minute_data(symbol, trade_time);
CREATE INDEX IF NOT EXISTS idx_minute_date ON minute_data(trade_date DESC);

-- ============================================
-- 社区消息表（股吧/社区，保留30天）
-- ============================================
CREATE TABLE IF NOT EXISTS community_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'eastmoney_guba',
    symbol TEXT NOT NULL,
    title TEXT NOT NULL,
    author TEXT,
    reply_count INTEGER DEFAULT 0,
    click_count INTEGER DEFAULT 0,
    published TEXT,
    link TEXT NOT NULL UNIQUE,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_community_lookup ON community_data(symbol, published DESC);
CREATE INDEX IF NOT EXISTS idx_community_cleanup ON community_data(ingested_at);

-- ============================================
-- 盘中估算净值表（东方财富 fundgz，保留30天）
-- ============================================
CREATE TABLE IF NOT EXISTS estimate_nav_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    fund_name TEXT,
    nav_date DATE NOT NULL,
    dwjz REAL,
    estimate_nav REAL NOT NULL,
    estimate_growth REAL,
    estimate_time TEXT,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, estimate_time)
);
CREATE INDEX IF NOT EXISTS idx_est_nav_lookup ON estimate_nav_data(symbol, nav_date DESC);

CREATE INDEX IF NOT EXISTS idx_est_nav_lookup ON estimate_nav_data(symbol, estimate_time DESC);
CREATE INDEX IF NOT EXISTS idx_est_nav_cleanup ON estimate_nav_data(ingested_at);

-- ============================================
-- 股票日线数据表（Tushare daily，保留60天）
-- ============================================
CREATE TABLE IF NOT EXISTS daily_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'tushare',
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    pre_close REAL,
    change REAL,
    pct_chg REAL,
    vol REAL,
    amount REAL,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_lookup ON daily_data(symbol, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_cleanup ON daily_data(ingested_at);

-- 沪深港通北向资金Top10数据（Tushare hsgt）
CREATE TABLE IF NOT EXISTS hsgt_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'tushare',
    trade_date DATE NOT NULL,
    ts_code TEXT NOT NULL,
    name TEXT,
    close REAL,
    change REAL,
    pct_chg REAL,
    vol REAL,
    amount REAL,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_hsgt_lookup ON hsgt_data(trade_date DESC, amount DESC);
CREATE INDEX IF NOT EXISTS idx_hsgt_cleanup ON hsgt_data(ingested_at);

-- 全网聚合新闻数据（NewsAggregatorCollector）
CREATE TABLE IF NOT EXISTS news_aggregator_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'news_aggregator',
    collected_at TEXT NOT NULL,
    hackernews_count INTEGER DEFAULT 0,
    hackernews_items TEXT,
    github_count INTEGER DEFAULT 0,
    github_items TEXT,
    ths_count INTEGER DEFAULT 0,
    ths_items TEXT,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, caijing_count INTEGER, caijing_items TEXT, eeo_count INTEGER, eeo_items TEXT, ths_hk_count INTEGER, ths_hk_items TEXT, sina_live_count INTEGER, sina_live_items TEXT,
    UNIQUE(collected_at)
);
CREATE INDEX IF NOT EXISTS idx_news_agg_cleanup ON news_aggregator_data(ingested_at);

-- ============================================
-- 主力资金/板块资金流表 (mcp-eastmoney 主力资金榜, 永久保存)
-- ============================================
CREATE TABLE IF NOT EXISTS fund_flow_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'eastmoney_em',  -- mcp-eastmoney / em_api
    flow_type TEXT NOT NULL,                     -- main_fund_rank / sector_fund_flow
    sector_kind TEXT,                            -- industry / concept (板块资金流)
    rank_no INTEGER,                             -- 排名
    symbol TEXT,                                 -- 个股代码 (主力资金流填)
    name TEXT NOT NULL,                          -- 名称 (个股/板块)
    price REAL,                                  -- 现价
    change_pct REAL,                             -- 涨跌幅 (%)
    main_net_inflow REAL,                        -- 主力净流入 (元)
    main_net_pct REAL,                           -- 主力净流入占比 (%)
    super_large_net REAL,                        -- 超大单净额
    large_net REAL,                              -- 大单净额
    medium_net REAL,                             -- 中单净额
    small_net REAL,                              -- 小单净额
    leading_stock TEXT,                          -- 领涨股 (板块资金流填)
    leading_change_pct REAL,                     -- 领涨股涨跌幅 (%)
    rank_data TEXT NOT NULL,                     -- 排序日期 YYYY-MM-DD
    collected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_data TEXT,
    UNIQUE(source, flow_type, rank_data, name)
);
CREATE INDEX IF NOT EXISTS idx_fund_flow_lookup ON fund_flow_data(flow_type, rank_data DESC, rank_no);
CREATE INDEX IF NOT EXISTS idx_fund_flow_cleanup ON fund_flow_data(collected_at);

-- ============================================
-- 市场涨跌统计表 (Phase 2 新增)
-- 数据源: ak.stock_sse_summary + ak.stock_szse_summary 合并
-- ============================================
CREATE TABLE IF NOT EXISTS market_stats_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE UNIQUE NOT NULL,
    -- 上交所
    sse_listed_count INTEGER,           -- 上市公司数
    sse_stock_count INTEGER,            -- 上市股票数
    sse_total_mv REAL,                  -- 总市值 (亿)
    sse_circ_mv REAL,                   -- 流通市值 (亿)
    sse_avg_pe REAL,                    -- 平均市盈率
    -- 深交所
    szse_stock_count INTEGER,
    szse_total_mv REAL,
    szse_circ_mv REAL,
    szse_amount REAL,                   -- 成交金额
    -- 汇总
    total_listed_count INTEGER,
    total_mv REAL,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_market_stats_date ON market_stats_data(trade_date DESC);

-- ============================================
-- 涨停池表 (Phase 2 新增)
-- 数据源: ak.stock_zt_pool_em(date=YYYYMMDD)
-- 仅保留最近 30 个交易日
-- ============================================
CREATE TABLE IF NOT EXISTS limit_up_pool_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    price REAL,
    change_pct REAL,
    amount REAL,                        -- 成交额
    limit_times INTEGER,                -- 连板次数
    first_limit_time TEXT,              -- 首次涨停时间
    last_limit_time TEXT,               -- 最后涨停时间
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, code)
);
CREATE INDEX IF NOT EXISTS idx_limit_up_lookup ON limit_up_pool_data(trade_date DESC, change_pct DESC);
CREATE INDEX IF NOT EXISTS idx_limit_up_cleanup ON limit_up_pool_data(ingested_at);

-- ============================================
-- 龙虎榜表 (Phase 2 新增)
-- 数据源: ak.stock_lhb_detail_em() 默认当日 + ak.stock_lhb_stock_statistic_em(period)
-- ============================================
CREATE TABLE IF NOT EXISTS dragon_tiger_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    -- 当日上榜详情 (stock_lhb_detail_em)
    close_price REAL,
    change_pct REAL,
    net_buy_amount REAL,                -- 龙虎榜净买额
    buy_amount REAL,                    -- 买入额
    sell_amount REAL,                   -- 卖出额
    total_amount REAL,
    -- 累计统计 (stock_lhb_stock_statistic_em)
    period TEXT,                         -- 近一月/近三月/近六月/近一年
    list_count INTEGER,                  -- 上榜次数
    inst_buy_net REAL,                   -- 机构买入净额
    -- 其他
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, code, period)
);
CREATE INDEX IF NOT EXISTS idx_dragon_tiger_lookup ON dragon_tiger_data(trade_date DESC, code);
CREATE INDEX IF NOT EXISTS idx_dragon_tiger_code ON dragon_tiger_data(code, trade_date DESC);

-- ============================================================================
-- 运行时依赖表（2026-10-26 补齐）
--
-- 背景：这 8 张表历史上由早期的 SQLAlchemy models.py 建出（models.py 已删除），
--       它们的 CREATE 语句从未进入 schema.sql，导致：
--         * 生产库有 25 张表，schema.sql 只有 17 张
--         * 【从 schema.sql 重建库 → 这 8 张表不存在】
--            → upsert_source_metric 报 no such table（信源质量统计/降级恢复全失效）
--            → insert_failed_collection 报 no such table（限频重试队列失效）
--            → insert_failed_transform 报 no such table（死信队列失效）
--            → 工单系统不可用
--
-- 来源：以下 CREATE 语句**从生产库 sqlite_master 原样 dump**，未凭印象手写；
--       唯一改动是把 CREATE TABLE 改为 CREATE TABLE IF NOT EXISTS、
--       补齐 CREATE INDEX 的 IF NOT EXISTS —— 保证与 database.py 的迁移路径幂等不冲突。
--
-- 说明：SQLite 的 executescript() 不支持 .read / ATTACH 式文件引入，
--       database.py 只读取本文件一个 schema，因此补在同一文件内最简单可靠。
-- ============================================================================


-- ---------- source_metrics ----------
CREATE TABLE IF NOT EXISTS source_metrics (
    source          TEXT NOT NULL,
    data_type       TEXT NOT NULL,
    total           INTEGER DEFAULT 0,
    success         INTEGER DEFAULT 0,
    failure         INTEGER DEFAULT 0,
    transform_error INTEGER DEFAULT 0,
    avg_sec         REAL DEFAULT 0,
    success_rate    REAL DEFAULT 1.0,
    is_degraded     BOOLEAN DEFAULT 0,
    updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, consecutive_success INTEGER DEFAULT 0,
    PRIMARY KEY (source, data_type)
);
CREATE INDEX IF NOT EXISTS idx_sm_rate ON source_metrics(success_rate ASC, total DESC);

-- ---------- failed_collections ----------
CREATE TABLE IF NOT EXISTS failed_collections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER,
    source TEXT NOT NULL,
    symbol TEXT,
    data_type TEXT NOT NULL,
    config TEXT,                              -- 原始采集参数（JSON）
    error_message TEXT,                      -- 限频错误信息
    retry_after TEXT,                         -- 冷却截止时间（ISO8601），可空表示尽快重试
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_retry_after ON failed_collections(retry_after);
CREATE INDEX IF NOT EXISTS idx_fc_sub ON failed_collections(subscription_id);

-- ---------- failed_transforms ----------
CREATE TABLE IF NOT EXISTS failed_transforms (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type       TEXT NOT NULL,
    symbol          TEXT,
    source          TEXT NOT NULL,
    raw_sample      TEXT,
    transform_error TEXT,
    stack_trace     TEXT,
    code_version    TEXT,
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at     TEXT,
    resolution      TEXT,
    resolved_by     TEXT
);
CREATE INDEX IF NOT EXISTS idx_ft_unresolved ON failed_transforms(resolved_at) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_ft_data_type ON failed_transforms(data_type, created_at DESC);

-- ---------- tickets ----------
CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    priority TEXT DEFAULT 'P3',  -- P0,P1,P2,P3
    description TEXT,
    status TEXT DEFAULT 'open',  -- open, investigating, resolved, closed
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    comments_json TEXT DEFAULT '[]'  -- JSON数组: [{"author":"...", "content":"...", "at":"..."}]
);

-- ---------- stock_basic_data ----------
CREATE TABLE IF NOT EXISTS stock_basic_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'tushare',
    symbol TEXT NOT NULL,
    ts_code TEXT,
    name TEXT,
    area TEXT,
    industry TEXT,
    market TEXT,
    list_date DATE,
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, source)
);

-- ---------- stock_fundamentals ----------
CREATE TABLE IF NOT EXISTS stock_fundamentals (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    price REAL,          -- 最新价
    pe_ttm REAL,         -- 市盈率(动态)
    pb REAL,             -- 市净率
    total_mv REAL,       -- 总市值（亿元）
    circ_mv REAL,        -- 流通市值（亿元）
    industry TEXT,       -- 行业
    raw_data TEXT,       -- 原始数据（部分）
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(symbol, date)
);

-- ---------- mmx_search_data ----------
CREATE TABLE IF NOT EXISTS mmx_search_data (id INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT NOT NULL, title TEXT, link TEXT, snippet TEXT, date TEXT, collected_at TEXT NOT NULL, ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_mmx_collected ON mmx_search_data(collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_mmx_query ON mmx_search_data(query);

-- ---------- bid_ask_data ----------
CREATE TABLE IF NOT EXISTS bid_ask_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL DEFAULT 'akshare_em',
    symbol TEXT NOT NULL,
    collected_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours')),
    
    -- 盘口数据
    buy1 REAL, buy1_vol INTEGER,
    buy2 REAL, buy2_vol INTEGER,
    buy3 REAL, buy3_vol INTEGER,
    buy4 REAL, buy4_vol INTEGER,
    buy5 REAL, buy5_vol INTEGER,
    sell1 REAL, sell1_vol INTEGER,
    sell2 REAL, sell2_vol INTEGER,
    sell3 REAL, sell3_vol INTEGER,
    sell4 REAL, sell4_vol INTEGER,
    sell5 REAL, sell5_vol INTEGER,
    
    -- 基础行情
    price REAL,
    avg_price REAL,
    change_pct REAL,
    change_val REAL,
    volume REAL,
    amount REAL,
    turnover REAL,
    vol_ratio REAL,
    high REAL,
    low REAL,
    open REAL,
    prev_close REAL,
    upper_limit REAL,
    lower_limit REAL,
    outer_disk REAL,
    inner_disk REAL,
    
    raw_data TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now', '+8 hours'))
);
CREATE INDEX IF NOT EXISTS idx_bid_ask_symbol ON bid_ask_data(symbol);
CREATE INDEX IF NOT EXISTS idx_bid_ask_collected ON bid_ask_data(collected_at);
