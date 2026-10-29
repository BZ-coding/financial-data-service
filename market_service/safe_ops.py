r"""safe_ops —— 统一的安全删除 / 安全计数封装（防"LIKE 通配符误删"类事故）

背景（2026-10-29 生产事故）
--------------------------
清理测试数据时执行了：

    DELETE FROM tickets WHERE title LIKE '__%'      # 想删"名字以 __ 开头"的测试数据

**SQLite 的 LIKE 里 `_` 是单字符通配符、不是字面下划线**，
所以 `'__%'` 的真实含义是"至少两个字符的任意字符串" → **全表命中**，
28 条生产订阅被清零，**无报错、无异常、无告警**。

同一场事故里还犯了第二次：恢复后的校验脚本用**同一个 LIKE**
去数"应为 0 的残留"→ 又匹配到 28 行，差点误判"没恢复成功"。
**说明 LIKE 前缀匹配在"数残留"时同样不可靠。**

本模块提供三条防线
------------------
1. :func:`safe_delete` —— 带**影响行数上限**的删除；超阈值自动回滚并抛错。
2. :func:`safe_count` —— 计数同样拒绝"未转义的 LIKE 通配符"（数残留也要准）。
3. :func:`assert_no_unescaped_like` —— SQL 里的 LIKE 若含 `_`/`%` 字面量
   又没有 ``ESCAPE`` 子句 → 直接拒绝执行（除非显式声明通配符意图）。

字面下划线的正确写法
--------------------
    -- 错：'_' 是通配符
    WHERE name LIKE '__%'
    -- 对：用 ESCAPE 把**每一个** '_' 都转义成字面量
    --     ⚠️ SQLite 的 LIKE 里 '_' 是单字符通配符；
    --        匹配字面下划线必须**每个都转义**：两个 '_' → '\_\_'
    WHERE name LIKE '__\_\_%' ESCAPE '\'
    -- 或者：干脆不用 LIKE
    WHERE substr(name, 1, 2) = '__'
    -- 或者用 GLOB（通配符是 * ?，没有 '_'）
    WHERE name GLOB '__*'

若确实**想用通配符**（如 ``trade_time LIKE '____-__-__%'`` 匹配日期格式），
在 SQL 里加一行注释标记 :data:`WILDCARD_INTENT` 显式声明意图。
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any, Iterable, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

#: 显式声明"这里的 LIKE 里 _ / % 是通配符（有意为之）"的注释标记
WILDCARD_INTENT = "safe-like-intent"

#: 默认删除行数上限：超过就中止（除非 force=True）
DEFAULT_MAX_ROWS = 5

#: **全局兜底上限**：无论调用点把 max_rows 传多大（哪怕 10**9），
#: 实际生效的上限都不会超过这里 —— "忘传 max_rows"或"传了个巨大值"
#: 都不可能变成"无上限删除"。
#: 确需批量删除时，走 expect_rows（精确期望值护栏）或显式 max_rows=None。
HARD_MAX_ROWS = 1000

#: 哨兵：**区分"调用点没传 max_rows"与"显式传了某个值"**。
#: - 没传 + 给了 expect_rows → 上限取 expect_rows（不能被默认值误伤）
#: - 没传 + 没给 expect_rows → 上限取 DEFAULT_MAX_ROWS
#: - 显式传 None → 显式取消上限（打 WARNING）
#: - 显式传整数 → min(该值, HARD_MAX_ROWS)
_UNSET = object()

#: 命中 LIKE 的字面量模式（单引号或双引号）
_LIKE_LITERAL_RE = re.compile(r"\bLIKE\s+('([^']*)'|\"([^\"]*)\")", re.IGNORECASE)


class UnsafeSQLError(RuntimeError):
    """SQL 命中了安全护栏（未转义的 LIKE 通配符 / 删除行数超限）。"""


class UnsafeDeleteError(UnsafeSQLError):
    """删除影响行数超过上限，已自动回滚。"""


def _literal_of(match: re.Match) -> Optional[str]:
    """取 LIKE 后面的字面量内容；参数化（?）返回 None。"""
    single, double = match.group(2), match.group(3)
    if single is not None:
        return single
    if double is not None:
        return double
    return None


def assert_no_unescaped_like(sql: str) -> None:
    """SQL 里的 LIKE 若含 **未转义** 的 `_`/`%`，直接拒绝。

    - 含 ``ESCAPE`` 子句 → 视为已正确转义，放行
    - SQL 含 :data:`WILDCARD_INTENT` 标记 → 视为有意使用通配符，放行
    - 参数化占位符（``LIKE ?``）→ 无法静态判断，放行（由调用方保证）
    """
    if WILDCARD_INTENT in sql:
        return
    upper = sql.upper()
    if "ESCAPE" in upper:
        return
    for match in _LIKE_LITERAL_RE.finditer(sql):
        literal = _literal_of(match)
        if literal is None:
            continue
        if "_" in literal or "%" in literal:
            raise UnsafeSQLError(
                f"SQL 中的 LIKE 字面量含未转义通配符: {literal!r} —— "
                f"'_' 在 LIKE 里是**单字符通配符**，会误匹配（如 '__%' = 至少两个字符的任意串）。\n"
                f"  正确写法：LIKE '__\\_\\_%' ESCAPE '\\'（**每个 '_' 都要转义**）\n"
                f"            或 substr(name,1,2)='__'  /  GLOB '__*'（GLOB 没有 '_' 通配符）\n"
                f"  若确实想用通配符，请在 SQL 里加注释标记: {WILDCARD_INTENT}\n"
                f"  原 SQL: {sql.strip()[:200]}"
            )


def like_prefix(prefix: str) -> Tuple[str, str]:
    """构造"字面前缀匹配"的 (pattern, escape_clause)。

    >>> like_prefix("__")
    ('\\_\\_%', " ESCAPE '\\'")
    #        ↑ 两个下划线**各自**都被转义（只转义一个会匹配不到 → 静默漏删）
    """
    escaped = prefix.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")
    return f"{escaped}%", " ESCAPE '\\'"


def _delta_total_changes(conn: sqlite3.Connection, before: int) -> int:
    return conn.total_changes - before


def safe_delete(
    conn: sqlite3.Connection,
    sql: str,
    params: Sequence[Any] = (),
    *,
    max_rows: Optional[int] = _UNSET,   # type: ignore[assignment]
    force: bool = False,
    expect_rows: Optional[int] = None,
    reason: str = "",
) -> int:
    """带**影响行数上限**的删除。

    流程：
      1. 先做 LIKE 通配符护栏检查（:func:`assert_no_unescaped_like`）
      2. ``BEGIN IMMEDIATE`` 开事务
      3. 执行 DELETE，用 ``Connection.total_changes`` 的**增量**计行数
         （⚠️ 不能用 ``cursor.rowcount``：Python 3.12+ 的 statement cache 会让它恒为 0）
      4. 实际行数 > ``max_rows`` 且未 ``force`` → **ROLLBACK** 并抛
         :class:`UnsafeDeleteError`（生产库上"删多了"必须中止，不能事后补救）
      5. 否则 COMMIT，返回实际删除行数

    Args:
        max_rows: 允许删除的最大行数。**受全局兜底 :data:`HARD_MAX_ROWS` 约束**：
            传再大的值也不会超过它 → "忘传"或"传巨大值"都不可能变成无上限删除。
            - **不传** 且给了 ``expect_rows`` → 上限自动取 ``expect_rows``
              （否则默认值会误伤"期望删 100 行"这种合法批量删除）
            - **不传** 且没给 ``expect_rows`` → 上限取 :data:`DEFAULT_MAX_ROWS`
            - 显式传 ``None`` → **显式取消上限**（仅用于已人工确认过的批量清理，
              会打 WARNING 日志留痕）
        expect_rows: **精确期望值**护栏 —— 实际删除行数必须**正好等于**它，
            否则回滚。适合"清理历史数据"这类行数本就很大的场景
            （此时 max_rows 的硬上限会误伤，用 expect_rows 更合适）。
        force: True 时跳过上限检查（仅用于已人工确认过的批量清理）。
    """
    assert_no_unescaped_like(sql)

    # ---- 全局兜底：无论调用点怎么传，都不可能变成"无上限删除" ----
    if max_rows is None:
        cap = None
        logger.warning(
            "safe_delete **显式取消行数上限**（max_rows=None）—— 仅限已人工确认的批量清理: %s",
            sql.strip()[:120],
        )
    elif max_rows is _UNSET:
        # 没传：给了 expect_rows 就以它为上限（否则默认值会误伤合法批量删除）
        cap = int(expect_rows) if expect_rows is not None else DEFAULT_MAX_ROWS
    else:
        cap = int(max_rows)
        if cap > HARD_MAX_ROWS:
            logger.warning(
                "safe_delete max_rows=%s 超过全局兜底 %s → 已按 %s 执行: %s",
                cap, HARD_MAX_ROWS, HARD_MAX_ROWS, sql.strip()[:120],
            )
            cap = HARD_MAX_ROWS

    before = conn.total_changes
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        # 已在事务中（sqlite3 默认 isolation_level 会隐式开事务）
        pass
    try:
        conn.execute(sql, tuple(params))
        deleted = _delta_total_changes(conn, before)
        if expect_rows is not None and deleted != expect_rows and not force:
            # 精确期望值护栏（更具体，优先于上限检查）
            conn.rollback()
            raise UnsafeDeleteError(
                f"删除影响 {deleted} 行，与**期望值 {expect_rows} 行**不符 → **已回滚，未删除任何数据**。"
                f"{'（reason: ' + reason + '）' if reason else ''}\n"
                f"  行数不符通常意味着：并发写入 / 判据漂移 / WHERE 条件被误写。\n"
                f"  SQL: {sql.strip()[:200]}"
            )
        if cap is not None and deleted > cap and not force:
            conn.rollback()
            raise UnsafeDeleteError(
                f"删除影响 {deleted} 行 > 上限 {cap} 行 → **已回滚，未删除任何数据**。"
                f"{'（reason: ' + reason + '）' if reason else ''}\n"
                f"  全局兜底上限为 {HARD_MAX_ROWS}（传再大的 max_rows 也不会超过它）。\n"
                f"  如果这确实是预期删除量：请显式传 max_rows={deleted}（≤{HARD_MAX_ROWS}）"
                f"或改用 expect_rows={deleted}（精确期望值）。\n"
                f"  SQL: {sql.strip()[:200]}"
            )
        conn.commit()
        logger.info(
            "safe_delete 执行完成：删除 %s 行（上限 %s%s%s）: %s",
            deleted,
            "不限" if cap is None else cap,
            f", 期望 {expect_rows}" if expect_rows is not None else "",
            ", force" if force else "", sql.strip()[:120],
        )
        return deleted
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def safe_count(
    conn: sqlite3.Connection,
    sql: str,
    params: Sequence[Any] = (),
) -> int:
    """安全计数：同样拒绝"未转义的 LIKE 通配符"。

    ⚠️ "数残留"用 LIKE 一样不可靠（事故里就栽在这）——
    校验"应该是 0 条残留"时，请用**精确 id / 精确 hash / substr() 精确比较**。
    """
    assert_no_unescaped_like(sql)
    cur = conn.execute(sql, tuple(params))
    row = cur.fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)


def count_by_ids(conn: sqlite3.Connection, table: str, ids: Iterable[int]) -> int:
    """按**精确 id 列表**计数（校验残留首选，不受 LIKE 通配符影响）。"""
    id_list = [int(i) for i in ids]
    if not id_list:
        return 0
    placeholders = ",".join("?" for _ in id_list)
    return safe_count(conn, f"SELECT COUNT(*) FROM {table} WHERE id IN ({placeholders})", id_list)
