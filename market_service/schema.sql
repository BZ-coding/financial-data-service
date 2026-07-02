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
    -- 仅交易时段采集（A股：9:30-11:30, 13:00-15:00）
    trading_hours_only BOOLEAN DEFAULT FALSE,
    -- 备用数据源列表（逗号分隔，如 "akshare_hist,tushare"）
    backup_sources TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    source TEXT NOT NULL DEFAULT 'fundgz',
    symbol TEXT NOT NULL,
    fund_name TEXT,
    nav_date DATE NOT NULL,           -- 昨日确认净值日期
    dwjz REAL,                        -- 昨日单位净值
    estimate_nav REAL NOT NULL,       -- 估算净值
    estimate_growth REAL,             -- 估算增长率（%）
    estimate_time TEXT NOT NULL,      -- 估算时间
    raw_data TEXT,
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, symbol, estimate_time)
);

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
    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
