"""Regression tests for the probe's business-identity uniqueness check (2026-11-13).

背景：**公告重复入库为什么两个多月没被发现**（002202 实测 135 行 / 89 标题）。

探针原本只查"活没活"（源有没有成功、表有没有长行），从不查
"同一件事是不是被当成多件事存了"。而重复入库恰恰让表**长得更快**——
行数增量指标只会认为"更健康"，永远不会报警；公告表甚至完全不在
CORE_TABLES 里，探针连它的行数都不看。

指标 6 补的就是这条正交的判据：每张有业务身份键的表，
行数必须等于 distinct 身份数；不等 = 同一实体存了多份。

这些测试锁定三件事：
1. 约束缺失（迁移失败/表被重建）导致重复时 → 必须报出来
2. 正常同标题多份公告、同 symbol 多来源快照 → 不能假警
   （否则 30 分钟一次会把人淹了，探针会被无视）
3. announcement_id 缺失（身份键空）→ 单独告警
"""
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from market_service.market_data_freshness import (  # noqa: E402
    _check_identity_uniqueness,
)


def _conn(tmp_path: Path, *, with_constraints: bool = True) -> sqlite3.Connection:
    """建最小库。

    with_constraints=True  → 与生产一致（表上有业务唯一约束）
    with_constraints=False → 模拟**约束缺失**的老库形态：唯一索引没建成功
                             （2026-11-12 实测 init_tables 报
                              "no such column: announcement_id" → 整段建索引失败），
                             此时重复入库没有任何数据库层拦截，只能靠探针发现。
    """
    db = tmp_path / "probe.db"
    con = sqlite3.connect(db)
    ann_unique = (
        "CREATE UNIQUE INDEX idx_ann_identity ON announcement_data(source, symbol, announcement_id) "
        "WHERE announcement_id IS NOT NULL;" if with_constraints else ""
    )
    news_link = " UNIQUE" if with_constraints else ""
    price_uniq = ", UNIQUE(source, symbol, trade_time)" if with_constraints else ""
    con.executescript(f"""
        CREATE TABLE announcement_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL DEFAULT 'cninfo',
            symbol TEXT NOT NULL,
            title TEXT NOT NULL,
            announcement_time TEXT,
            link TEXT NOT NULL,
            raw_data TEXT,
            ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            announcement_id TEXT
        );
        {ann_unique}

        CREATE TABLE news_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT,
            link TEXT NOT NULL{news_link},
            symbol TEXT,
            published TEXT,
            ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE price_data (
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
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP{price_uniq}
        );
    """)
    return con


def _ins_ann(con, *, source, symbol, title, atime, link, ann_id):
    con.execute(
        """INSERT INTO announcement_data
           (source, symbol, title, announcement_time, link, announcement_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (source, symbol, title, atime, link, ann_id))


# ------------------------------------------------------------------ 正常态：不报

def test_no_duplicates_reports_no_problem(tmp_path):
    con = _conn(tmp_path)
    _ins_ann(con, source="cninfo", symbol="002202", title="公告A", atime="2026-11-12",
              link="http://x/1", ann_id="cninfo:1")
    _ins_ann(con, source="cninfo", symbol="002202", title="公告B", atime="2026-11-11",
              link="http://x/2", ann_id="cninfo:2")
    con.execute("""INSERT INTO news_data (source, title, link, symbol)
                   VALUES ('rss','新闻A','http://n/1','002202')""")
    con.execute("""INSERT INTO price_data (source, symbol, trade_time, price)
                   VALUES ('sina','002202','2026-11-12T15:00:00',10.0)""")
    con.commit()

    problems = _check_identity_uniqueness(con)
    assert problems == [], problems
    con.close()


def test_same_title_different_announcement_id_is_not_duplicate(tmp_path):
    """同标题但 announcement_id 不同 = 两份不同公告（如多次「H股公告」），不是重复。"""
    con = _conn(tmp_path, with_constraints=False)
    for i in (1, 2, 3):
        _ins_ann(con, source="cninfo", symbol="002202", title="H股公告",
                  atime=f"2026-09-2{i}", link=f"http://x/{i}", ann_id=f"cninfo:{i}")
    con.commit()
    problems = _check_identity_uniqueness(con)
    assert problems == [], problems
    con.close()


def test_same_symbol_different_source_is_not_duplicate(tmp_path):
    """同 symbol 但不同 source 的行情快照不是重复（身份键必须含 source）。"""
    con = _conn(tmp_path, with_constraints=False)
    con.execute("""INSERT INTO price_data (source, symbol, trade_time, price)
                   VALUES ('sina','002202','2026-11-12T15:00:00',10.0)""")
    con.execute("""INSERT INTO price_data (source, symbol, trade_time, price)
                   VALUES ('tencent','002202','2026-11-12T15:00:00',10.1)""")
    con.commit()
    problems = _check_identity_uniqueness(con)
    assert problems == [], problems
    con.close()


# ------------------------------------------------------------------ 异常态：必须报

def test_duplicate_announcement_identity_is_detected(tmp_path):
    """同一 announcement_id 存了两份 → 必须报（这正是 11-12 修的那个 bug）。

    复现真实形态：巨潮对同一 announcementId 会给两种时间格式，link 内嵌
    announcementTime= 参数随之变化 → UNIQUE(link) 拦不住 → 同一公告两行。
    这里用"约束缺失"的老库形态（唯一索引没建成功）来构造。
    """
    con = _conn(tmp_path, with_constraints=False)
    _ins_ann(con, source="cninfo", symbol="002202", title="同一公告", atime="2026-09-22",
              link="http://x/detail?announcementId=123&announcementTime=2026-09-22",
              ann_id="cninfo:123")
    _ins_ann(con, source="cninfo", symbol="002202", title="同一公告",
              atime="2026-09-22 00:00:00",
              link="http://x/detail?announcementId=123&announcementTime=2026-09-22 00:00:00",
              ann_id="cninfo:123")
    con.commit()

    problems = _check_identity_uniqueness(con)
    assert problems, "重复的 announcement_id 必须被报出来"
    joined = " ".join(problems)
    assert "announcement_data" in joined
    assert "重复业务身份" in joined
    con.close()


def test_duplicate_news_link_is_detected(tmp_path):
    con = _conn(tmp_path, with_constraints=False)
    con.execute("""INSERT INTO news_data (source, title, link, symbol)
                   VALUES ('rss','新闻A','http://n/1','002202')""")
    con.execute("""INSERT INTO news_data (source, title, link, symbol)
                   VALUES ('zhihu','新闻A','http://n/1','002202')""")
    con.commit()
    problems = _check_identity_uniqueness(con)
    assert any("news_data" in p for p in problems), problems
    con.close()


def test_missing_announcement_id_is_flagged(tmp_path):
    """announcement_id 为空 → 身份键缺失，去重只能靠 link，跨链接变体无法识别。"""
    con = _conn(tmp_path)
    _ins_ann(con, source="cninfo", symbol="002202", title="公告A", atime="2026-11-12",
              link="http://x/1", ann_id=None)
    con.commit()
    problems = _check_identity_uniqueness(con)
    assert any("announcement_id" in p for p in problems), problems
    con.close()


def test_null_identity_rows_are_not_counted_as_duplicates(tmp_path):
    """身份键为 NULL 的行**不能**参与分组统计。

    SQLite 会把所有 NULL 归成同一组 → 若不排除，「缺身份的行数」会被误算成
    「重复行数」（实测 413 行缺身份被报成 428 行重复，严重放大）。
    缺身份的行由 announcement_id 空值检查单独报。
    """
    con = _conn(tmp_path, with_constraints=False)
    for i in range(3):
        _ins_ann(con, source="em", symbol="002202", title=f"公告{i}",
                  atime="2026-11-12", link=f"http://pdf/{i}", ann_id=None)
    _ins_ann(con, source="cninfo", symbol="002202", title="正常公告",
              atime="2026-11-12", link="http://x/1", ann_id="cninfo:1")
    con.commit()

    problems = _check_identity_uniqueness(con)
    # 重复行数应为 0（没有两个非空身份重复）；只报缺身份这一条
    dup_problems = [p for p in problems if "重复业务身份" in p]
    assert dup_problems == [], dup_problems
    assert any("缺 announcement_id" in p for p in problems), problems
    con.close()


def test_duplicate_count_matches_actual_extra_rows(tmp_path):
    """报出的重复行数必须等于「多出来的行数」，不是组数也不是被放大的数字。"""
    con = _conn(tmp_path, with_constraints=False)
    # 3 行同一身份 → 多出 2 行；另 2 行另一身份 → 多出 1 行；合计多出 3 行
    for i in range(3):
        _ins_ann(con, source="cninfo", symbol="002202", title="A",
                  atime=f"2026-11-1{i}", link=f"http://x/a{i}", ann_id="cninfo:1")
    for i in range(2):
        _ins_ann(con, source="cninfo", symbol="002202", title="B",
                  atime=f"2026-11-2{i}", link=f"http://x/b{i}", ann_id="cninfo:2")
    con.commit()

    problems = _check_identity_uniqueness(con)
    dup = [p for p in problems if "重复业务身份" in p]
    assert len(dup) == 1, problems
    assert "3 行是重复副本" in dup[0], dup[0]
    assert "重复身份组 2" in dup[0], dup[0]
    con.close()
