#!/usr/bin/env python3
"""check_unsafe_sql —— 扫代码里的"危险删除"写法（2026-10-29 事故的护栏）

背景
----
事故 SQL：``DELETE FROM tickets WHERE title LIKE '__%'``
—— ``LIKE`` 里 ``_`` 是**单字符通配符**，全表命中，28 条生产订阅被清零。

本脚本在**代码层面**把这类写法挡在生产之外：

1. **VIOLATION**：`DELETE` / `UPDATE` 里出现 **未转义的** ``LIKE`` 字面量
   （含 ``_`` 或 ``%``，又没有 ``ESCAPE`` 子句，也没有 ``safe-like-intent`` 标记）
   → 退出码非 0，CI / 定时器都会看到。
2. **WARN**：``LIKE`` 出现在**计数**语句里（"数残留"）→ 同样不可靠，提示改用精确匹配。
3. **WARN**：直接 ``conn.execute("DELETE ...")`` 而没有走 :func:`safe_ops.safe_delete`
   → 提示收口到统一封装（影响行数上限保护）。

用法
----
    python3 check_unsafe_sql.py            # 扫描并报告，违规时退出码 1
    python3 check_unsafe_sql.py --quiet    # 只输出结论
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path
from typing import List, Tuple

BASE = Path(__file__).resolve().parent
WILDCARD_INTENT = "safe-like-intent"

# keep in sync with safe_ops.DEFAULT_MAX_ROWS
DEFAULT_MAX_ROWS = 5

#: LIKE + 字面量
LIKE_LITERAL_RE = re.compile(r"\bLIKE\s+('([^']*)'|\"([^\"]*)\")", re.IGNORECASE)
#: 写操作语句里含 LIKE
WRITE_LIKE_RE = re.compile(r"\b(DELETE\s+FROM|UPDATE)\b", re.IGNORECASE)
#: 直接执行 DELETE 的调用
RAW_DELETE_RE = re.compile(r"\.execute\(\s*(f?[\"'])(.*?DELETE\s+FROM)", re.IGNORECASE | re.DOTALL)
#: 计数语句里含 LIKE
COUNT_LIKE_RE = re.compile(r"COUNT\s*\(", re.IGNORECASE)


def _like_literals(sql: str) -> List[str]:
    out = []
    for m in LIKE_LITERAL_RE.finditer(sql):
        lit = m.group(2) if m.group(2) is not None else m.group(3)
        if lit is None:
            continue
        if "_" in lit or "%" in lit:
            out.append(lit)
    return out


def scan_file(path: Path) -> Tuple[List[str], List[str]]:
    """返回 (violations, warnings)"""
    try:
        src = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return [], []
    violations: List[str] = []
    warnings: List[str] = []

    # 逐个字符串常量做检查（SQL 基本都在字符串里）
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = None

    literals: List[str] = []
    if tree is not None:
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                literals.append((node.lineno, node.value))
    else:
        literals = [(i + 1, l) for i, l in enumerate(src.splitlines())]

    for lineno, text in literals:
        up = text.upper()
        has_like = "LIKE" in up
        if not has_like:
            continue
        lits = _like_literals(text)
        if not lits:
            continue
        has_escape = "ESCAPE" in up
        has_intent = WILDCARD_INTENT in text
        is_write = bool(WRITE_LIKE_RE.search(text))
        is_count = bool(COUNT_LIKE_RE.search(text))

        if is_write and not has_escape and not has_intent:
            violations.append(
                f"{path.name}:{lineno} 写操作里的 LIKE 含未转义通配符 {lits!r} → "
                f"LIKE 的 '_' 是单字符通配符，会全表命中。修法：LIKE '__\\_%' ESCAPE '\\' "
                f"或改用 substr()/GLOB；确实要用通配符就加注释标记 {WILDCARD_INTENT}"
            )
        elif is_count and not has_escape and not has_intent:
            warnings.append(
                f"{path.name}:{lineno} 计数语句用了 LIKE（{lits!r}）→ "
                f"「数残留」用 LIKE 同样不可靠（事故里就栽在这）。建议用精确 id / substr()"
            )

    # 裸 DELETE（未经 safe_delete）—— 用 AST 精确判定，避免跨语句误报
    if path.name != "safe_ops.py" and tree is not None:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr in ("execute", "executemany")):
                continue
            if not node.args:
                continue
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                if re.search(r"\bDELETE\s+FROM\b", first.value, re.IGNORECASE):
                    warnings.append(
                        f"{path.name}:{first.lineno} 直接 execute('DELETE ...') → "
                        f"建议改走 safe_ops.safe_delete()（有影响行数上限，超阈值自动回滚）"
                    )

    # safe_delete call sites: did the caller pass a row limit explicitly?
    if tree is not None:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Name) and func.id == "safe_delete"):
                continue
            kw_names = {k.arg for k in node.keywords if k.arg}
            if not (kw_names & {"max_rows", "expect_rows"}):
                warnings.append(
                    f"{path.name}:{node.lineno} safe_delete() without explicit "
                    f"max_rows/expect_rows -> falls back to default cap "
                    f"{DEFAULT_MAX_ROWS} (still safe, but state the intent)"
                )

    return violations, warnings


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    files = [
        p for p in BASE.rglob("*.py")
        if ".bak" not in p.name
        and "__pycache__" not in str(p)
        and "/venv" not in str(p)
        and "venv." not in str(p)      # venv.py312-dead-… 之类的备份 venv
        and "/data" not in str(p)
        and "/logs" not in str(p)
        and "/static" not in str(p)
    ]
    all_v: List[str] = []
    all_w: List[str] = []
    for p in files:
        v, w = scan_file(p)
        all_v += v
        all_w += w

    if not args.quiet:
        for w in all_w:
            print(f"[WARN]  {w}")
    for v in all_v:
        print(f"[VIOLATION] {v}")

    print(f"\n扫描 {len(files)} 个文件：VIOLATION {len(all_v)} 项，WARN {len(all_w)} 项")
    if all_v:
        print("❌ 存在危险删除写法 —— 必须修复后才能合入")
        return 1
    print("✅ 未发现未转义 LIKE 的写操作")
    return 0


if __name__ == "__main__":
    sys.exit(main())
