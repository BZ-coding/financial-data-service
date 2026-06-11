#!/usr/bin/env python3
"""
数据库访问层
使用SQLite，提供类型化的CRUD操作
"""

import sqlite3
import json
from datetime import datetime, date, timezone, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# 时区：东8区
TZ = timezone(timedelta(hours=8))

def adapt_datetime(ts: datetime) -> str:
    """SQLite适配器：datetime -> ISO8601"""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=TZ)
    return ts.isoformat()

def convert_datetime(s: bytes) -> datetime:
    """SQLite转换器：ISO8601 -> datetime"""
    dt_str = s.decode('utf-8')
    # 处理带Z的UTC时间
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1] + '+00:00'
    return datetime.fromisoformat(dt_str).astimezone(TZ)

# 注册适配器
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("TIMESTAMPTZ", convert_datetime)

class Database:
    def __init__(self, db_path: str = "data/market.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES,
            timeout=30,
            check_same_thread=False  # 允许多线程（需配合锁）
        )
        self.conn.row_factory = sqlite3.Row
        self._enable_wal()
        self.init_tables()

    def _enable_wal(self):
        """启用WAL模式，提高并发性能"""
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.commit()

    def init_tables(self):
        """执行schema.sql建表（含迁移）"""
        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        try:
            self.conn.executescript(sql)
            self.conn.commit()
            logger.info(f"数据库初始化完成: {self.db_path}")
        except Exception as e:
            logger.error(f"SQL执行失败: {e}")
            # 打印前几行帮助调试
            lines = sql.split(';')
            for i, line in enumerate(lines[:10], 1):
                logger.error(f"第{i}行: {line.strip()}")
            raise

        # ========== 数据库迁移 ==========
        # 检查 subscriptions 表是否有新增列
        cur = self.conn.execute("PRAGMA table_info(subscriptions)")
        existing_cols = [row[1] for row in cur.fetchall()]
        migrations = []
        if 'trading_hours_only' not in existing_cols:
            migrations.append("ALTER TABLE subscriptions ADD COLUMN trading_hours_only BOOLEAN DEFAULT FALSE")
        if 'backup_sources' not in existing_cols:
            migrations.append("ALTER TABLE subscriptions ADD COLUMN backup_sources TEXT")

        # 建表迁移（如果表不存在）
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='estimate_nav_data'")
        if not cur.fetchone():
            migrations.append("""CREATE TABLE estimate_nav_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL DEFAULT 'fundgz',
                symbol TEXT NOT NULL,
                fund_name TEXT,
                nav_date DATE NOT NULL,
                dwjz REAL,
                estimate_nav REAL NOT NULL,
                estimate_growth REAL,
                estimate_time TEXT NOT NULL,
                raw_data TEXT,
                ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, symbol, estimate_time)
            )""")
            migrations.append("CREATE INDEX idx_est_nav_lookup ON estimate_nav_data(symbol, estimate_time DESC)")
            migrations.append("CREATE INDEX idx_est_nav_cleanup ON estimate_nav_data(ingested_at)")

        # 建表迁移：daily_data（Tushare 股票日线）
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_data'")
        if not cur.fetchone():
            migrations.append("""CREATE TABLE daily_data (
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
            )""")
            migrations.append("CREATE INDEX idx_daily_lookup ON daily_data(symbol, trade_date DESC)")
            migrations.append("CREATE INDEX idx_daily_cleanup ON daily_data(ingested_at)")

        for stmt in migrations:
            try:
                logger.info(f"执行迁移: {stmt}")
                self.conn.execute(stmt)
                self.conn.commit()
            except Exception as e:
                logger.error(f"迁移失败: {e}")

    # ==================== NAV DATA ====================

    def get_latest_nav(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新净值"""
        cur = self.conn.execute("""
            SELECT * FROM nav_data
            WHERE symbol = ?
            ORDER BY nav_date DESC, ingested_at DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return None

    def insert_nav(self, data: Dict[str, Any]) -> bool:
        """插入净值数据，支持UPSERT（存在则更新）"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO nav_data (
                    source, symbol, nav_date, nav, acc_nav, change_pct,
                    raw_data, ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data['source'],
                data['symbol'],
                data['nav_date'],
                data['nav'],
                data.get('acc_nav'),
                data.get('change_pct'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入/更新净值失败: {e}")
            return False

    def insert_daily(self, data: Dict[str, Any]) -> bool:
        """插入股票日线数据（Tushare daily）"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO daily_data (
                    source, symbol, trade_date, open, high, low, close,
                    pre_close, change, pct_chg, vol, amount, raw_data,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'tushare'),
                data['symbol'],
                data['trade_date'],
                data['open'],
                data['high'],
                data['low'],
                data['close'],
                data.get('pre_close'),
                data.get('change'),
                data.get('pct_chg'),
                data.get('vol'),
                data.get('amount'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入日线数据失败: {e}")
            return False

    def insert_hsgt(self, data: Dict[str, Any]) -> bool:
        """插入沪深港通Top10数据（Tushare hsgt）"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO hsgt_data (
                    source, trade_date, ts_code, name, close,
                    change, pct_chg, vol, amount, raw_data,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'tushare'),
                data.get('trade_date'),
                data.get('ts_code') or data.get('symbol'),
                data.get('name'),
                data.get('close'),
                data.get('change'),
                data.get('pct_chg'),
                data.get('vol'),
                data.get('amount'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入HSGT数据失败: {e}")
            return False

    def insert_stock_basic(self, data: Dict[str, Any]) -> bool:
        """插入股票基本信息（Tushare stock_basic）"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO stock_basic_data (
                    source, symbol, ts_code, name, area,
                    industry, market, list_date, raw_data,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'tushare'),
                data.get('symbol'),
                data.get('ts_code'),
                data.get('name'),
                data.get('area'),
                data.get('industry'),
                data.get('market'),
                data.get('list_date'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入股票基本信息失败: {e}")
            return False

    def insert_news_aggregator(self, data: Dict[str, Any]) -> bool:
        """插入全网聚合新闻（news_aggregator采集器）"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO news_aggregator_data (
                    source, collected_at,
                    hackernews_count, hackernews_items,
                    github_count, github_items,
                    ths_count, ths_items,
                    raw_data, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                'news_aggregator',
                data.get('collected_at', ''),
                data.get('hackernews', {}).get('count', 0),
                json.dumps(data.get('hackernews', {}).get('items', []), ensure_ascii=False),
                data.get('github_trending', {}).get('count', 0),
                json.dumps(data.get('github_trending', {}).get('items', []), ensure_ascii=False),
                data.get('ths_news', {}).get('count', 0),
                json.dumps(data.get('ths_news', {}).get('items', []), ensure_ascii=False),
                json.dumps(data, ensure_ascii=False),
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入聚合新闻失败: {e}")
            return False

    def get_latest_news_aggregator(self) -> Optional[Dict[str, Any]]:
        """获取最新聚合新闻"""
        cur = self.conn.execute("""
            SELECT id, source, collected_at,
                   hackernews_count, hackernews_items,
                   github_count, github_items,
                   ths_count, ths_items,
                   ingested_at
            FROM news_aggregator_data
            ORDER BY ingested_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            d = dict(row)
            # 反序列化 JSON 字段
            for field in ['hackernews_items', 'github_items', 'ths_items']:
                if d.get(field) and isinstance(d[field], str):
                    try:
                        d[field] = json.loads(d[field])
                    except Exception:
                        pass
            return d
        return None

    def get_latest_daily(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新日线数据"""
        cur = self.conn.execute("""
            SELECT id, source, symbol, CAST(trade_date AS TEXT) as trade_date,
                   open, high, low, close, pre_close, change, pct_chg,
                   vol, amount, raw_data, ingested_at, updated_at
            FROM daily_data
            WHERE symbol = ?
            ORDER BY trade_date DESC, ingested_at DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return None

    def get_daily(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """获取最近 N 日日线数据"""
        cur = self.conn.execute("""
            SELECT id, source, symbol, CAST(trade_date AS TEXT) as trade_date,
                   open, high, low, close, pre_close, change, pct_chg,
                   vol, amount, raw_data, ingested_at, updated_at
            FROM daily_data
            WHERE symbol = ?
            ORDER BY trade_date DESC
            LIMIT ?
        """, (symbol, days))
        return [dict(row) for row in cur.fetchall()]

    # ========== 沪深港通（北向资金） ==========
    def get_latest_hsgt(self, trade_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取北向资金 Top10"""
        if trade_date:
            cur = self.conn.execute("""
                SELECT id, source, CAST(trade_date AS TEXT) as trade_date,
                       ts_code, name, close, change, pct_chg, vol, amount,
                       raw_data, ingested_at, updated_at
                FROM hsgt_data
                WHERE trade_date = CAST(? AS TEXT)
                ORDER BY amount DESC
                LIMIT 20
            """, (trade_date,))
        else:
            cur = self.conn.execute("""
                SELECT id, source, CAST(trade_date AS TEXT) as trade_date,
                       ts_code, name, close, change, pct_chg, vol, amount,
                       raw_data, ingested_at, updated_at
                FROM hsgt_data
                ORDER BY trade_date DESC, amount DESC
                LIMIT 20
            """)
        return [dict(row) for row in cur.fetchall()]

    def get_stock_basic(self, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """查询股票基本信息"""
        if symbol:
            cur = self.conn.execute("""
                SELECT id, source, symbol, ts_code, name, area, industry, market,
                       CAST(list_date AS TEXT) as list_date,
                       raw_data, ingested_at, updated_at
                FROM stock_basic_data
                WHERE symbol = ? OR ts_code = ?
                ORDER BY ingested_at DESC
                LIMIT 1
            """, (symbol, symbol))
            row = cur.fetchone()
            return dict(row) if row else None
        return None

    # ========== 盘中估算净值 ==========
    def insert_estimate_nav(self, data: Dict[str, Any]) -> bool:
        """插入盘中估算净值"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO estimate_nav_data (
                    source, symbol, fund_name, nav_date, dwjz,
                    estimate_nav, estimate_growth, estimate_time,
                    raw_data, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'fundgz'),
                data['symbol'],
                data.get('fund_name'),
                data.get('nav_date'),
                data.get('dwjz'),
                data['estimate_nav'],
                data.get('estimate_growth'),
                data.get('estimate_time'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入估算净值失败: {e}")
            return False

    def get_latest_estimate_nav(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新盘中估算净值"""
        cur = self.conn.execute("""
            SELECT * FROM estimate_nav_data
            WHERE symbol = ?
            ORDER BY estimate_time DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return None

    def get_nav_history(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """获取历史净值"""
        start_date = (datetime.now(TZ) - timedelta(days=days)).date()
        cur = self.conn.execute("""
            SELECT nav_date, nav, acc_nav, change_pct
            FROM nav_data
            WHERE symbol = ? AND nav_date >= ?
            ORDER BY nav_date ASC
        """, (symbol, start_date))
        return [dict(row) for row in cur.fetchall()]

    # ==================== ESTIMATE NAV DATA ====================

    def insert_estimate_nav(self, data: Dict[str, Any]) -> bool:
        """插入盘中估算净值，支持UPSERT"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO estimate_nav_data (
                    source, symbol, fund_name, nav_date, dwjz,
                    estimate_nav, estimate_growth, estimate_time,
                    raw_data, ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'fundgz'),
                data['symbol'],
                data.get('fund_name'),
                data.get('nav_date'),
                data.get('dwjz'),
                data['estimate_nav'],
                data.get('estimate_growth'),
                data.get('estimate_time'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"插入/更新估算净值失败: {e}")
            return False

    def get_latest_estimate_nav(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新估算净值"""
        cur = self.conn.execute("""
            SELECT * FROM estimate_nav_data
            WHERE symbol = ?
            ORDER BY estimate_time DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return None

    # ==================== PRICE DATA ====================

    def insert_price(self, data: Dict[str, Any]) -> bool:
        """插入实时行情"""
        try:
            self.conn.execute("""
                INSERT INTO price_data (
                    source, symbol, trade_time, price, volume, amount,
                    high, low, open, prev_close, raw_data,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data['source'],
                data['symbol'],
                data['trade_time'],
                data['price'],
                data.get('volume'),
                data.get('amount'),
                data.get('high'),
                data.get('low'),
                data.get('open'),
                data.get('prev_close'),
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # 重复数据，忽略
            return False
        except Exception as e:
            logger.error(f"插入行情失败: {e}")
            return False

    def get_price_history(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """获取历史行情"""
        start_time = datetime.now(TZ) - timedelta(days=days)
        cur = self.conn.execute("""
            SELECT trade_time, price, volume, amount, high, low, open, prev_close
            FROM price_data
            WHERE symbol = ? AND trade_time >= ?
            ORDER BY trade_time ASC
        """, (symbol, start_time))
        return [dict(row) for row in cur.fetchall()]

    def get_latest_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新行情，直接返回最新数据"""
        cur = self.conn.execute("""
            SELECT * FROM price_data
            WHERE symbol = ?
            ORDER BY trade_time DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return None

        return dict(row)

    # ==================== MINUTE DATA ====================

    def insert_minute_bars(self, bars: List[Dict[str, Any]]) -> int:
        """批量插入分钟K线，返回插入数量"""
        count = 0
        now = datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
        for bar in bars:
            try:
                self.conn.execute("""
                    INSERT OR IGNORE INTO minute_data (
                        symbol, name, trade_date, trade_time,
                        open, high, low, close, volume, amount, source, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bar['symbol'],
                    bar.get('name'),
                    bar['trade_date'],
                    bar['trade_time'],
                    bar.get('open'),
                    bar.get('high'),
                    bar.get('low'),
                    bar['close'],
                    bar.get('volume', 0),
                    bar.get('amount'),
                    bar.get('source', 'sina'),
                    now
                ))
                count += 1
            except Exception as e:
                logger.debug(f"分钟数据插入跳过: {bar.get('trade_time')}, {e}")
        self.conn.commit()
        return count

    def get_latest_minute_bar(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取某标的最新的分钟K线"""
        cur = self.conn.execute("""
            SELECT * FROM minute_data
            WHERE symbol = ?
            ORDER BY trade_time DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_minute_bars(self, symbol: str, date: str = None) -> List[Dict[str, Any]]:
        """获取指定标的的分钟K线，date为空则取今日"""
        if date is None:
            date = datetime.now(TZ).strftime('%Y-%m-%d')
        cur = self.conn.execute("""
            SELECT * FROM minute_data
            WHERE symbol = ? AND trade_date = ?
            ORDER BY trade_time ASC
        """, (symbol, date))
        return [dict(row) for row in cur.fetchall()]

    # ==================== NEWS DATA ====================

    def get_news_by_link(self, link: str) -> Optional[Dict[str, Any]]:
        """根据链接查询新闻是否已存在"""
        cur = self.conn.execute("""
            SELECT id FROM news_data WHERE link = ?
        """, (link,))
        row = cur.fetchone()
        return dict(row) if row else None

    def insert_news(self, data: Dict[str, Any]) -> bool:
        """插入新闻，link必须唯一"""
        try:
            self.conn.execute("""
                INSERT INTO news_data (
                    source, title, summary, link, symbol, published,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data['source'],
                data['title'],
                data.get('summary'),
                data['link'],
                data.get('symbol'),
                data.get('published')
            ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # link已存在，去重
            return False

    def get_recent_news(self, source: Optional[str] = None, hours: int = 24) -> List[Dict[str, Any]]:
        """获取最近新闻（用于调试）"""
        start_time = datetime.now(TZ) - timedelta(hours=hours)
        cur = self.conn.execute("""
            SELECT * FROM news_data
            WHERE ingested_at >= ?
            ORDER BY ingested_at DESC
            LIMIT 50
        """, (start_time,))
        return [dict(row) for row in cur.fetchall()]

    def insert_index(self, data: Dict[str, Any]) -> bool:
        """插入指数数据（支持批量）"""
        try:
            # 兼容单条和批量
            if isinstance(data, list):
                for item in data:
                    self._insert_index_one(item)
            else:
                self._insert_index_one(data)
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"insert_index失败: {e}")
            return False

    def _insert_index_one(self, data: Dict[str, Any]):
        """插入单条指数数据"""
        # 优先用 trade_date，否则从 timestamp 提取
        trade_date = data.get('trade_date')
        if not trade_date and data.get('timestamp'):
            trade_date = data['timestamp'][:10]
        if not trade_date:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        self.conn.execute("""
            INSERT OR REPLACE INTO index_data
            (code, name, price, change_pct, change_amount, volume, amount,
             high, low, open, prev_close, source, trade_date, trade_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['code'],
            data['name'],
            data.get('price'),
            data.get('change_pct'),
            data.get('change_amount'),
            data.get('volume'),
            data.get('amount'),
            data.get('high'),
            data.get('low'),
            data.get('open'),
            data.get('prev_close'),
            data.get('source', 'tencent_sina'),
            trade_date,
            data.get('timestamp')
        ))

    def get_latest_index(self, code: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取最新指数数据"""
        if code:
            cur = self.conn.execute("""
                SELECT * FROM index_data WHERE code = ?
                ORDER BY trade_date DESC LIMIT 1
            """, (code,))
        else:
            cur = self.conn.execute("""
                SELECT * FROM index_data
                WHERE code IN ('sh000001', 'sh000300', 'sh000688', 'sz399001', 'sz399006')
                AND trade_date = (SELECT MAX(trade_date) FROM index_data)
                ORDER BY code
            """)
        return [dict(row) for row in cur.fetchall()]

    # ==================== ANNOUNCEMENT DATA ====================

    def insert_announcement(self, data: Dict[str, Any]) -> bool:
        """插入公告数据"""
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO announcement_data (
                    source, symbol, title, announcement_time, link, raw_data,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'cninfo'),
                data['symbol'],
                data['title'],
                data.get('announcement_time'),
                data['link'],
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"insert_announcement失败: {e}")
            return False

    def get_announcements(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """获取最近公告"""
        start_date = (datetime.now(TZ) - timedelta(days=days)).date()
        cur = self.conn.execute("""
            SELECT * FROM announcement_data
            WHERE symbol = ? AND announcement_time >= ?
            ORDER BY announcement_time DESC
            LIMIT 50
        """, (symbol, start_date))
        return [dict(row) for row in cur.fetchall()]

    # ==================== COMMUNITY DATA ====================

    def insert_community(self, data: Dict[str, Any]) -> bool:
        """插入社区/股吧数据"""
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO community_data (
                    source, symbol, title, author, reply_count, click_count,
                    published, link, raw_data,
                    ingested_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('source', 'eastmoney_guba'),
                data['symbol'],
                data['title'],
                data.get('author'),
                data.get('reply_count', 0),
                data.get('click_count', 0),
                data.get('published'),
                data['link'],
                json.dumps(data.get('raw', {}), ensure_ascii=False) if data.get('raw') else None
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"insert_community失败: {e}")
            return False

    def get_community_posts(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """获取最近社区帖子"""
        start_date = (datetime.now(TZ) - timedelta(days=days)).date()
        cur = self.conn.execute("""
            SELECT * FROM community_data
            WHERE symbol = ? AND published >= ?
            ORDER BY published DESC
            LIMIT 50
        """, (symbol, start_date))
        return [dict(row) for row in cur.fetchall()]

    # ==================== SUBSCRIPTIONS ====================

    def get_due_subscriptions(self, now: datetime) -> List[Dict[str, Any]]:
        """获取需要采集的订阅（last_collected + frequency_min <= now）
        注意： caller 需要自行检查 trading_hours_only 限制
        """
        cur = self.conn.execute("""
            SELECT * FROM subscriptions
            WHERE enabled = TRUE
              AND (last_collected IS NULL OR
                   datetime(last_collected, '+' || frequency_min || ' minutes') <= ?)
        """, (now,))
        return [dict(row) for row in cur.fetchall()]

    def get_subscriptions_by_type(self, type_: str) -> List[Dict[str, Any]]:
        """按类型获取所有订阅"""
        cur = self.conn.execute("""
            SELECT * FROM subscriptions WHERE type = ? AND enabled = TRUE
        """, (type_,))
        return [dict(row) for row in cur.fetchall()]

    def get_subscription_by_symbol(self, symbol: str, data_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """根据symbol和data_type查找订阅（用于API逻辑判断）"""
        query = "SELECT * FROM subscriptions WHERE symbol = ? AND enabled = 1"
        params = [symbol]
        if data_type:
            # 注意：data_types是JSON数组字符串，简单LIKE匹配（生产环境建议用SQLite JSON函数）
            query += " AND data_types LIKE ?"
            params.append(f'%"{data_type}"%')
        cur = self.conn.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None


    def save_fundamental(self, data: Dict[str, Any]) -> bool:
        """保存基本面数据（支持股票和基金）"""
        try:
            # 对于基金数据，使用 today 作为 date
            date = data.get('date') or datetime.now().strftime('%Y-%m-%d')
            
            self.conn.execute("""
                INSERT OR REPLACE INTO stock_fundamentals
                (symbol, date, price, pe_ttm, pb, total_mv, circ_mv, industry, raw_data, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            """, (
                data['symbol'],
                date,
                data.get('price'),
                data.get('pe_ttm'),
                data.get('pb'),
                data.get('total_mv'),
                data.get('circ_mv'),
                data.get('industry'),
                # 基金数据存储完整信息，股票数据存原始片段
                json.dumps(data) if (data.get('data_type') == 'fundamental' and 'manager' in data) else (data.get('raw') or str(data.get('raw_parts', [])[:1000]))
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"save_fundamental失败: {e}")
            return False

    def get_latest_fundamental(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新基本面"""
        cur = self.conn.execute("""
            SELECT * FROM stock_fundamentals
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        data = dict(row)
        # 转换日期为字符串
        if isinstance(data.get('date'), (datetime, date)):
            data['date'] = data['date'].strftime('%Y-%m-%d')
        if isinstance(data.get('ingested_at'), (datetime, date)):
            data['ingested_at'] = data['ingested_at'].isoformat()
        return data

    def get_fundamental_history(self, symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        """获取基本面历史（按日期倒序）"""
        cur = self.conn.execute("""
            SELECT * FROM stock_fundamentals
            WHERE symbol = ? AND date >= date('now', '-' || ? || ' days')
            ORDER BY date DESC
        """, (symbol, days))
        return [dict(row) for row in cur.fetchall()]


    def update_subscription_last_run(self, sub_id: int, run_time: datetime):
        """更新订阅上次运行时间"""
        self.conn.execute("""
            UPDATE subscriptions SET last_collected = ? WHERE id = ?
        """, (run_time, sub_id))
        self.conn.commit()

    def add_subscription(self, name: str, type_: str, config: Dict[str, Any],
                        data_types: List[str], frequency_min: int = 60, symbol: Optional[str] = None,
                        trading_hours_only: bool = False, backup_sources: Optional[str] = None) -> int:
        """新增订阅，返回ID"""
        cur = self.conn.execute("""
            INSERT INTO subscriptions (
                name, type, config, symbol, data_types, frequency_min,
                trading_hours_only, backup_sources, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            name,
            type_,
            json.dumps(config, ensure_ascii=False),
            symbol,
            json.dumps(data_types, ensure_ascii=False),
            frequency_min,
            1 if trading_hours_only else 0,
            backup_sources
        ))
        self.conn.commit()
        return cur.lastrowid

    def update_subscription(self, sub_id: int, **kwargs):
        """更新订阅字段"""
        allowed = ['name', 'symbol', 'data_types', 'frequency_min', 'trading_hours_only',
                   'backup_sources', 'enabled', 'config']
        set_parts = []
        values = []
        for key, val in kwargs.items():
            if key not in allowed:
                continue
            if key in ['data_types', 'config'] and isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)
            elif key == 'trading_hours_only':
                val = 1 if val else 0
            set_parts.append(f"{key} = ?")
            values.append(val)
        if not set_parts:
            return False
        values.append(sub_id)
        sql = f"UPDATE subscriptions SET {', '.join(set_parts)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        self.conn.execute(sql, values)
        self.conn.commit()
        return True

    def delete_subscription(self, sub_id: int) -> bool:
        """删除订阅"""
        cur = self.conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def get_all_subscriptions(self) -> List[Dict[str, Any]]:
        """获取所有订阅（显式列以保证新增字段可见）"""
        cur = self.conn.execute("""
            SELECT id, name, type, config, symbol, data_types, frequency_min,
                   last_collected, enabled, trading_hours_only, backup_sources,
                   created_at, updated_at
            FROM subscriptions
            ORDER BY enabled DESC, id ASC
        """)
        return [dict(row) for row in cur.fetchall()]

    # ==================== COLLECTION LOG ====================

    def create_ticket(self, title: str, priority: str, description: str) -> int:
        """创建工单，返回ID"""
        cur = self.conn.execute("""
            INSERT INTO tickets (title, priority, description, status, created_at, updated_at)
            VALUES (?, ?, ?, 'open', datetime('now','localtime'), datetime('now','localtime'))
        """, (title, priority, description))
        self.conn.commit()
        return cur.lastrowid

    def get_tickets(self, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """查询工单列表"""
        query = "SELECT * FROM tickets"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        cur = self.conn.execute(query, params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]

    def get_ticket(self, ticket_id: int) -> Optional[Dict[str, Any]]:
        """查询单个工单"""
        cur = self.conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def update_ticket_status(self, ticket_id: int, status: str) -> bool:
        """更新工单状态"""
        cur = self.conn.execute("""
            UPDATE tickets SET status = ?, updated_at = datetime('now','localtime')
            WHERE id = ?
        """, (status, ticket_id))
        self.conn.commit()
        return cur.rowcount > 0

    def add_ticket_comment(self, ticket_id: int, author: str, content: str) -> bool:
        """添加工单评论（JSON存储）"""
        import json
        # 读取现有评论
        cur = self.conn.execute("SELECT comments_json FROM tickets WHERE id = ?", (ticket_id,))
        row = cur.fetchone()
        if not row:
            return False
        comments = json.loads(row[0] or '[]')
        comments.append({
            "author": author,
            "content": content,
            "at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        # 更新
        self.conn.execute("""
            UPDATE tickets SET comments_json = ?, updated_at = datetime('now','localtime')
            WHERE id = ?
        """, (json.dumps(comments, ensure_ascii=False), ticket_id))
        self.conn.commit()
        return True


    def log_collection(self, subscription_id: Optional[int], source: str, symbol: Optional[str],
                      data_type: str, status: str, items_fetched: int = 0,
                      items_stored: int = 0, error_message: Optional[str] = None,
                      started_at: datetime = None, finished_at: datetime = None):
        """记录采集日志"""
        duration = None
        if started_at and finished_at:
            duration = (finished_at - started_at).total_seconds()

        self.conn.execute("""
            INSERT INTO collection_log (
                subscription_id, source, symbol, data_type, status,
                items_fetched, items_stored, error_message,
                started_at, finished_at, duration_sec
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            subscription_id,
            source,
            symbol,
            data_type,
            status,
            items_fetched,
            items_stored,
            error_message,
            started_at or datetime.now(TZ),
            finished_at,
            duration
        ))
        self.conn.commit()

    def get_recent_errors(self, hours: int = 1) -> List[Dict[str, Any]]:
        """获取最近N小时的错误日志"""
        start = datetime.now(TZ) - timedelta(hours=hours)
        cur = self.conn.execute("""
            SELECT * FROM collection_log
            WHERE status = 'failed' AND started_at >= ?
            ORDER BY started_at DESC
            LIMIT 20
        """, (start,))
        return [dict(row) for row in cur.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        cur = self.conn.execute("SELECT COUNT(*) as cnt FROM nav_data")
        nav_count = cur.fetchone()['cnt']

        cur = self.conn.execute("SELECT COUNT(*) as cnt FROM price_data")
        price_count = cur.fetchone()['cnt']

        cur = self.conn.execute("SELECT COUNT(*) as cnt FROM news_data")
        news_count = cur.fetchone()['cnt']

        cur = self.conn.execute("SELECT COUNT(*) as cnt FROM subscriptions WHERE enabled = TRUE")
        active_subs = cur.fetchone()['cnt']

        # 最近24小时成功率
        cur = self.conn.execute("""
            SELECT status, COUNT(*) as cnt FROM collection_log
            WHERE started_at >= datetime('now', '-1 day')
            GROUP BY status
        """)
        status_counts = {row['status']: row['cnt'] for row in cur.fetchall()}

        total = sum(status_counts.values())
        success_rate = (status_counts.get('success', 0) / total * 100) if total > 0 else 0

        # 数据库文件大小
        db_size_mb = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0

        return {
            'records': {
                'nav': nav_count,
                'price': price_count,
                'news': news_count
            },
            'active_subscriptions': active_subs,
            'collection_24h': status_counts,
            'success_rate_24h': round(success_rate, 2),
            'database_size_mb': round(db_size_mb, 2),
            'generated_at': datetime.now(TZ).isoformat()
        }

    # ==================== CLEANUP ====================

    def cleanup_old_data(self, retention_days: Dict[str, int]) -> Dict[str, int]:
        """清理过期数据，返回各表删除行数"""
        now = datetime.now(TZ)
        deleted_counts = {}

        # 清理新闻
        cutoff_news = now - timedelta(days=retention_days.get('news_days', 30))
        cur = self.conn.execute("DELETE FROM news_data WHERE timestamp < ?", (cutoff_news,))
        deleted_counts['news'] = cur.rowcount

        # 清理行情
        cutoff_price = now - timedelta(days=retention_days.get('price_days', 60))
        cur = self.conn.execute("DELETE FROM price_data WHERE trade_time < ?", (cutoff_price,))
        deleted_counts['price'] = cur.rowcount

        # 清理公告
        cutoff_ann = now - timedelta(days=retention_days.get('announcement_days', 90))
        # TODO: 如有announcement表
        # cur = self.conn.execute(...)

        # 清理日志（30天前）
        cutoff_log = now - timedelta(days=30)
        cur = self.conn.execute("DELETE FROM collection_log WHERE started_at < ?", (cutoff_log,))
        deleted_counts['collection_log'] = cur.rowcount

        self.conn.commit()
        logger.info(f"清理完成: {deleted_counts}")
        return deleted_counts

    # ==================== FAILED TRANSFORMS（死信队列）====================

    def get_unresolved_transforms(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取未解决的转换错误（死信队列）"""
        try:
            cur = self.conn.execute("""
                SELECT id, data_type, symbol, source, raw_sample,
                       transform_error, stack_trace, created_at
                FROM failed_transforms
                WHERE resolved_at IS NULL
                ORDER BY created_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"get_unresolved_transforms 失败: {e}")
            return []

    def mark_transform_resolved(
        self, letter_id: int, resolution: str = "manual", resolved_by: str = "scheduler"
    ) -> bool:
        """标记死信为已解决"""
        try:
            self.conn.execute("""
                UPDATE failed_transforms
                SET resolved_at = ?, resolution = ?, resolved_by = ?
                WHERE id = ?
            """, (now_tz().isoformat(), resolution, resolved_by, letter_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"mark_transform_resolved 失败: {e}")
            return False

    def insert_failed_transform(
        self, data_type: str, symbol: Optional[str], source: str,
        raw_sample: str, transform_error: str, stack_trace: str = ""
    ) -> bool:
        """插入死信（转换失败时由 chain 调用）"""
        try:
            self.conn.execute("""
                INSERT INTO failed_transforms
                (data_type, symbol, source, raw_sample, transform_error, stack_trace)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (data_type, symbol, source, raw_sample, transform_error, stack_trace))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"insert_failed_transform 失败: {e}")
            return False

    # ==================== FAILED COLLECTIONS（限频重试队列）====================

    def insert_failed_collection(
        self, subscription_id: int, source: str, symbol: Optional[str],
        data_type: str, config: Optional[Dict[str, Any]] = None,
        error_message: str = "", retry_after: Optional[datetime] = None
    ) -> bool:
        """插入限频记录（重试队列）"""
        try:
            self.conn.execute("""
                INSERT INTO failed_collections
                (subscription_id, source, symbol, data_type, config,
                 error_message, retry_after)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                subscription_id, source, symbol, data_type,
                json.dumps(config, ensure_ascii=False) if config else None,
                error_message,
                retry_after.isoformat() if retry_after else None,
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"insert_failed_collection 失败: {e}")
            return False

    def get_due_failed_collections(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """获取冷却到期的限频重试记录"""
        if now is None:
            now = now_tz()
        try:
            cur = self.conn.execute("""
                SELECT id, subscription_id, source, symbol, data_type,
                       config, error_message, retry_after, created_at
                FROM failed_collections
                WHERE retry_after IS NULL OR retry_after <= ?
                ORDER BY created_at ASC
            """, (now.isoformat(),))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"get_due_failed_collections 失败: {e}")
            return []

    def remove_failed_collection(self, fail_id: int) -> bool:
        """删除限频重试记录（重试成功后调用）"""
        try:
            self.conn.execute("DELETE FROM failed_collections WHERE id = ?", (fail_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"remove_failed_collection 失败: {e}")
            return False

    def update_failed_collection_retry_after(
        self, fail_id: int, retry_after: datetime
    ) -> bool:
        """更新限频重试的冷却截止时间"""
        try:
            self.conn.execute("""
                UPDATE failed_collections
                SET retry_after = ?, updated_at = ?
                WHERE id = ?
            """, (retry_after.isoformat(), now_tz().isoformat(), fail_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"update_failed_collection_retry_after 失败: {e}")
            return False

    # ==================== SOURCE METRICS（信源质量动态排名）====================

    def get_source_metrics(self, data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取指定数据类型的信源质量（所有信源）"""
        try:
            if data_type:
                cur = self.conn.execute("""
                    SELECT source, data_type, total, success, failure,
                           transform_error, avg_sec, success_rate,
                           is_degraded, updated_at, consecutive_success
                    FROM source_metrics
                    WHERE data_type = ?
                    ORDER BY success_rate DESC, total DESC
                """, (data_type,))
            else:
                cur = self.conn.execute("""
                    SELECT source, data_type, total, success, failure,
                           transform_error, avg_sec, success_rate,
                           is_degraded, updated_at, consecutive_success
                    FROM source_metrics
                    ORDER BY success_rate DESC, total DESC
                """)
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"get_source_metrics 失败: {e}")
            return []

    def get_source_metric(self, source: str, data_type: str) -> Optional[Dict[str, Any]]:
        """获取单个 (source, data_type) 的信源质量"""
        try:
            cur = self.conn.execute("""
                SELECT source, data_type, total, success, failure,
                       transform_error, avg_sec, success_rate,
                       is_degraded, updated_at, consecutive_success
                FROM source_metrics
                WHERE source = ? AND data_type = ?
            """, (source, data_type))
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_source_metric 失败: {e}")
            return None

    def upsert_source_metric(
        self, source: str, data_type: str,
        success: bool, is_transform_error: bool = False, duration: float = 0.0
    ) -> bool:
        """
        增量更新 source_metrics（成功/失败计数 + 移动平均耗时 + 成功率 + 连续成功）。
        每次采集都调一次，用 UPSERT 保证原子。
        """
        try:
            inc_total = 1
            inc_success = 1 if success else 0
            inc_failure = 0 if success else 1
            inc_te = 1 if is_transform_error else 0

            # 连续成功：成功时 +1，失败时清零
            if success:
                consec_expr = "COALESCE(consecutive_success, 0) + 1"
            else:
                consec_expr = "0"

            self.conn.execute("""
                INSERT INTO source_metrics
                (source, data_type, total, success, failure, transform_error,
                 avg_sec, success_rate, is_degraded, updated_at, consecutive_success)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                ON CONFLICT(source, data_type) DO UPDATE SET
                    total = total + ?,
                    success = success + ?,
                    failure = failure + ?,
                    transform_error = transform_error + ?,
                    avg_sec = (avg_sec * total + ?) / (total + 1),
                    success_rate = CAST(success + ? AS REAL) / (total + 1),
                    updated_at = ?,
                    consecutive_success = ?
            """, (
                source, data_type, inc_total, inc_success, inc_failure, inc_te,
                duration,
                1.0 if success else 0.0,   # 首次插入的 success_rate
                now_tz().isoformat(),
                1 if success else 0,
                # ON CONFLICT 后的更新参数
                inc_total, inc_success, inc_failure, inc_te,
                duration,            # 移动平均的当前耗时
                inc_success,         # 新增的成功数
                now_tz().isoformat(),
                consec_expr,         # SQL 表达式字符串 → 但参数化的是字面 0/1
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"upsert_source_metric 失败: {e}")
            return False

    def set_source_degraded(
        self, source: str, data_type: str, is_degraded: bool
    ) -> bool:
        """设置/解除信源降级标志"""
        try:
            self.conn.execute("""
                UPDATE source_metrics
                SET is_degraded = ?, updated_at = ?
                WHERE source = ? AND data_type = ?
            """, (1 if is_degraded else 0, now_tz().isoformat(), source, data_type))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"set_source_degraded 失败: {e}")
            return False

    def close(self):
        """关闭连接"""
        self.conn.close()

# ==================== 工具函数 ====================

def now_tz() -> datetime:
    """获取当前东8区时间"""
    return datetime.now(TZ)

def parse_date(date_str: str) -> datetime:
    """解析日期字符串为datetime（带时区）"""
    if isinstance(date_str, datetime):
        return date_str.astimezone(TZ) if date_str.tzinfo else date_str.replace(tzinfo=TZ)
    # 尝试多种格式
    for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y-%m-%d %H:%M:%S']:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=TZ)
        except ValueError:
            continue
    # 最后尝试 fromisoformat
    try:
        dt = datetime.fromisoformat(date_str)
        return dt.astimezone(TZ) if dt.tzinfo else dt.replace(tzinfo=TZ)
    except:
        raise ValueError(f"无法解析日期: {date_str}")

if __name__ == "__main__":
    # 简单测试
    logging.basicConfig(level=logging.INFO)
    db = Database()
    print("数据库初始化成功")
    stats = db.get_stats()
    print("统计:", json.dumps(stats, indent=2, ensure_ascii=False))
    db.close()
