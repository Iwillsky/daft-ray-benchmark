"""TPC-H 结果校验: 把 Daft 的查询结果和期望答案逐行逐列比较。

期望答案有两种来源 (见 tpch_bench.py):
  official   TPC-H 官方 SF1 标准答案 (job/tpch_answers_sf1.json, 取自 DuckDB tpch 扩展的 tpch_answers(), 仅 SF1 有)
  reference  同一份数据集上由 DuckDB 独立计算出的结果 (job/tpch_reference.py 生成, 存在 S3)
两种模式都按"列位置"比较 (Daft 与标准答案的列名不同, 列顺序一致)。

排序键并列的处理 (tie_spec=(order_cols, id_col)):
  Q3 / Q10 / Q11 / Q18 的 ORDER BY 键在大数据集上会出现并列 (Q3 的前 10 名就是 5 对收入和日期完全相同的订单),
  并列行的先后顺序在两个引擎里不确定。这时分两步校验, 仍然不放过真正的错误:
    A) 排序键所在的列 (order_cols) 必须逐位置一致 —— 保证排序本身是对的;
    B) 再把两边都按唯一标识列 (id_col) 排序后, 整行逐列比较 —— 保证行的集合与内容是对的。
  不用 tie_spec 的题 (排序键唯一) 仍然严格按位置逐行逐列比较。
  限制: 如果 LIMIT 恰好把一组并列行截断, 两个引擎可能选了不同的行, 会报不一致 (这是真实的歧义, 不是误报)。
"""
from __future__ import annotations

import datetime
import json
import math
from decimal import Decimal


def norm(v):
    """统一成可比较的值: 数值 -> float, 日期 -> 'YYYY-MM-DD', 其它 -> str; 看起来像数字的字符串也转 float。"""
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()[:10]
    if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
        return float(v)
    s = str(v)
    try:
        return float(s)
    except ValueError:
        return s


def _equal(a, e, rel_tol, abs_tol):
    if isinstance(a, float) and isinstance(e, float):
        return math.isclose(a, e, rel_tol=rel_tol, abs_tol=abs_tol)
    return a == e


def compare(actual: dict, expected_rows: list, rel_tol: float, abs_tol: float, max_report: int = 5, tie_spec=None) -> list:
    """actual: Daft 的 to_pydict() 结果 {列名: [值...]}; expected_rows: [[值...], ...]。返回不一致项列表 (空 = 通过)。"""
    cols = list(actual.keys())
    n_actual = len(actual[cols[0]]) if cols else 0
    problems = []
    if n_actual != len(expected_rows):
        problems.append(f"row count: got {n_actual}, expected {len(expected_rows)}")
    if expected_rows and len(cols) != len(expected_rows[0]):
        problems.append(f"column count: got {len(cols)} {cols}, expected {len(expected_rows[0])}")
        return problems
    n = min(n_actual, len(expected_rows))

    def report(msg):
        problems.append(msg)
        return len(problems) >= max_report

    if tie_spec is None:
        for i in range(n):
            for j, c in enumerate(cols):
                a, e = norm(actual[c][i]), norm(expected_rows[i][j])
                if not _equal(a, e, rel_tol, abs_tol) and report(f"row {i} col {j} ({c}): got {a!r}, expected {e!r}"):
                    return problems + ["... (more mismatches omitted)"]
        return problems

    order_cols, id_col = tie_spec
    # A) 排序键所在列逐位置一致
    for i in range(n):
        for j in order_cols:
            a, e = norm(actual[cols[j]][i]), norm(expected_rows[i][j])
            if not _equal(a, e, rel_tol, abs_tol) and report(f"[sort key] row {i} col {j} ({cols[j]}): got {a!r}, expected {e!r}"):
                return problems + ["... (more mismatches omitted)"]
    # B) 按唯一标识列对齐后整行比较
    order = sorted(range(n_actual), key=lambda i: norm(actual[cols[id_col]][i]))
    exp_sorted = sorted(expected_rows, key=lambda r: norm(r[id_col]))
    for k in range(n):
        i = order[k]
        for j, c in enumerate(cols):
            a, e = norm(actual[c][i]), norm(exp_sorted[k][j])
            if not _equal(a, e, rel_tol, abs_tol) and report(f"[by id] id={norm(exp_sorted[k][id_col])!r} col {j} ({c}): got {a!r}, expected {e!r}"):
                return problems + ["... (more mismatches omitted)"]
    return problems


def load_official(path: str, q: int):
    with open(path) as f:
        data = json.load(f)
    return data["answers"][str(q)]["rows"]
