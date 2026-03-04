"""Performance review rules for Databricks notebooks.

Detects performance anti-patterns in SQL and PySpark code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass
class ReviewFinding:
    """A single review finding."""

    rule_id: str
    category: str  # performance, standards, anti-pattern
    severity: str  # critical, high, medium, low, info
    cell_index: int
    message: str
    suggestion: str = ""
    line_number: int | None = None
    code_snippet: str = ""


def extract_code_snippet(
    content: str,
    match: re.Match[str] | None,
    context_lines: int = 2,
    max_lines: int = 5,
) -> str:
    """Extract a code snippet around a regex match with context.

    Args:
        content: The full cell content.
        match: The regex match object.
        context_lines: Number of lines of context before and after the match.
        max_lines: Maximum total lines to return.

    Returns:
        The extracted snippet, or empty string if extraction fails.
    """
    if not match:
        return ""

    lines = content.splitlines()
    if not lines:
        return ""

    # Find the line number of the match start
    match_start = match.start()
    char_count = 0
    match_line = 0
    for i, line in enumerate(lines):
        char_count += len(line) + 1  # +1 for newline
        if char_count > match_start:
            match_line = i
            break

    # Calculate range with context
    start = max(0, match_line - context_lines)
    end = min(len(lines), match_line + context_lines + 1)

    # Cap at max_lines
    if end - start > max_lines:
        end = start + max_lines

    return "\n".join(lines[start:end])


# ------------------------------------------------------------------
# Performance rules
# ------------------------------------------------------------------

_PERFORMANCE_RULES: list[dict[str, Any]] = [
    {
        "id": "PERF001",
        "name": "Full table scan without filter",
        "pattern": re.compile(
            r"SELECT\s+\*\s+FROM\s+\w+(?:\.\w+)*\s*(?:;|\s*$)",
            re.IGNORECASE | re.MULTILINE,
        ),
        "severity": "high",
        "message": "SELECT * without WHERE clause may cause a full table scan.",
        "suggestion": "Add a WHERE clause or select only needed columns to reduce data scanned.",
        "cell_types": {"sql"},
    },
    {
        "id": "PERF001",
        "name": "SELECT * in PySpark SQL string",
        "pattern": re.compile(
            r"""(?:spark\.sql|sql)\(\s*(?:f?["']|f?\"\"\")\s*SELECT\s+\*\s+FROM\s""",
            re.IGNORECASE,
        ),
        "severity": "high",
        "message": "SELECT * without column pruning found in spark.sql() call. This reads all columns from the table.",
        "suggestion": "Specify explicit column names instead of SELECT * for better performance and schema safety.",
        "cell_types": {"python"},
    },
    {
        "id": "PERF002",
        "name": "collect() without limit",
        "pattern": re.compile(r"\.collect\(\s*\)"),
        "severity": "critical",
        "message": ".collect() brings all data to the driver. This can cause OOM errors on large datasets.",
        "suggestion": "Use .limit(N).collect() or .take(N) to limit data brought to the driver.",
        "cell_types": {"python", "scala"},
    },
    {
        "id": "PERF003",
        "name": "toPandas() without limit",
        "pattern": re.compile(r"\.toPandas\(\s*\)"),
        "severity": "critical",
        "message": ".toPandas() brings all data to the driver as a Pandas DataFrame. Can cause OOM.",
        "suggestion": "Use .limit(N).toPandas() or use Pandas API on Spark (pyspark.pandas) instead.",
        "cell_types": {"python"},
    },
    {
        "id": "PERF004",
        "name": "Missing broadcast hint for small table join",
        "pattern": re.compile(
            r"\.join\(\s*(?!.*broadcast)",
            re.IGNORECASE,
        ),
        "severity": "medium",
        "message": "Join without broadcast hint. If one side is small, broadcasting can improve performance.",
        "suggestion": "Consider using F.broadcast(small_df) for joins with a small table (<100MB).",
        "cell_types": {"python", "scala"},
    },
    {
        "id": "PERF005",
        "name": "Repeated DataFrame computation",
        "pattern": re.compile(r"(?:(?:df|result|data)\s*=.*\n){3,}.*\.show\(\)"),
        "severity": "medium",
        "message": "Multiple DataFrame transformations may trigger repeated computation.",
        "suggestion": "Cache intermediate DataFrames with .cache() or .persist() if reused.",
        "cell_types": {"python", "scala"},
    },
    {
        "id": "PERF006",
        "name": "COUNT(*) without filter",
        "pattern": re.compile(
            r"SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+\w+(?:\.\w+)*\s*(?:;|\s*$)",
            re.IGNORECASE | re.MULTILINE,
        ),
        "severity": "medium",
        "message": "COUNT(*) without WHERE scans the entire table.",
        "suggestion": "Add a filter clause or use table statistics (DESCRIBE DETAIL) for approximate counts.",
        "cell_types": {"sql"},
    },
    {
        "id": "PERF007",
        "name": "Non-partition filter in query",
        "pattern": re.compile(
            r"CROSS\s+JOIN",
            re.IGNORECASE,
        ),
        "severity": "high",
        "message": "CROSS JOIN produces a cartesian product — potentially explosive row count.",
        "suggestion": "Replace CROSS JOIN with a filtered JOIN if possible.",
        "cell_types": {"sql"},
    },
    {
        "id": "PERF008",
        "name": "Python UDF usage",
        "pattern": re.compile(r"@udf|udf\(|F\.udf\("),
        "severity": "medium",
        "message": "Python UDFs serialize data to Python, causing significant overhead.",
        "suggestion": "Replace with built-in Spark functions or Pandas UDFs (@pandas_udf) for better performance.",
        "cell_types": {"python"},
    },
    {
        "id": "PERF009",
        "name": "Skew join without hint",
        "pattern": re.compile(
            r"\.join\(.*(?:id|key|customer_id|user_id|account_id)",
            re.IGNORECASE,
        ),
        "severity": "medium",
        "message": "Join on a potentially high-cardinality key without a skew hint.",
        "suggestion": (
            "If data is skewed, add SKEW JOIN hints or use salting: "
            "spark.sql('SELECT /*+ SKEW_JOIN(t, \'id\') */ ...'). "
            "Enable AQE: spark.conf.set('spark.sql.adaptive.enabled', 'true')."
        ),
        "cell_types": {"python", "scala"},
    },
    {
        "id": "PERF010",
        "name": "Write operations inside a loop (small files)",
        "pattern": re.compile(
            r"for\s+\w+\s+in\s+.*:\s*\n(?:.*\n)*?.*(?:\.write\.|saveAsTable|\.save\()",
            re.MULTILINE,
        ),
        "severity": "high",
        "message": "Write operation inside a loop \u2014 likely to create many small files.",
        "suggestion": (
            "Accumulate data and write once outside the loop, or use Delta merge/upsert patterns. "
            "Many small files degrade read performance significantly."
        ),
        "cell_types": {"python", "scala"},
    },
    {
        "id": "PERF011",
        "name": "Streaming writeStream without checkpointing",
        "pattern": re.compile(r"\.writeStream\b"),
        "severity": "high",
        "message": "Structured Streaming writeStream detected \u2014 verify a checkpoint location is configured.",
        "suggestion": (
            "Always set a checkpoint location: "
            ".option('checkpointLocation', '/path/to/checkpoint'). "
            "Without it, the stream cannot recover from failures."
        ),
        "cell_types": {"python", "scala"},
    },
]


def check_performance(
    cell_content: str,
    cell_index: int,
    cell_type: str,
) -> list[ReviewFinding]:
    """Run performance rules against a notebook cell.

    Args:
        cell_content: Content of the cell.
        cell_index: Index of the cell in the notebook.
        cell_type: Type of cell (sql, python, scala, etc.)

    Returns:
        List of findings for this cell.
    """
    findings: list[ReviewFinding] = []

    for rule in _PERFORMANCE_RULES:
        if cell_type not in rule["cell_types"]:
            continue

        pattern: re.Pattern[str] = rule["pattern"]
        match = pattern.search(cell_content)
        if match:
            findings.append(ReviewFinding(
                rule_id=rule["id"],
                category="performance",
                severity=rule["severity"],
                cell_index=cell_index,
                message=rule["message"],
                suggestion=rule["suggestion"],
                code_snippet=extract_code_snippet(cell_content, match),
            ))

    return findings
