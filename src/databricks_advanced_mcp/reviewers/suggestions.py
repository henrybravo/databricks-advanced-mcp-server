"""Optimization suggestions for SQL and PySpark code.

Provides rewrite suggestions for common patterns that can be optimized.
"""

from __future__ import annotations

import re

from databricks_advanced_mcp.reviewers.performance import ReviewFinding, extract_code_snippet


def check_suggestions(
    cell_content: str,
    cell_index: int,
    cell_type: str,
) -> list[ReviewFinding]:
    """Generate optimization suggestions for a notebook cell."""
    findings: list[ReviewFinding] = []

    if cell_type == "sql":
        findings.extend(_sql_suggestions(cell_content, cell_index))

    if cell_type in ("python", "scala"):
        findings.extend(_pyspark_suggestions(cell_content, cell_index, cell_type))

    return findings


def _sql_suggestions(content: str, cell_index: int) -> list[ReviewFinding]:
    """SQL optimization suggestions."""
    findings: list[ReviewFinding] = []

    # OPT001: OPTIMIZE / Z-ORDER hint
    write_match = re.search(r"(?:MERGE\s+INTO|INSERT\s+(?:INTO|OVERWRITE))", content, re.IGNORECASE)
    if write_match:
        if not re.search(r"OPTIMIZE|ZORDER|Z-ORDER", content, re.IGNORECASE):
            findings.append(ReviewFinding(
                rule_id="OPT001",
                category="optimization",
                severity="info",
                cell_index=cell_index,
                message="Write operation without OPTIMIZE/Z-ORDER — consider adding after bulk writes.",
                suggestion=(
                    "Run OPTIMIZE table_name ZORDER BY (frequently_filtered_columns) "
                    "after bulk writes to improve read performance."
                ),
                code_snippet=extract_code_snippet(content, write_match),
            ))

    # OPT002: Predicate pushdown hint
    cast_match = re.search(r"WHERE.*\bCAST\b.*\bAS\b", content, re.IGNORECASE)
    if cast_match:
        findings.append(ReviewFinding(
            rule_id="OPT002",
            category="optimization",
            severity="medium",
            cell_index=cell_index,
            message="CAST in WHERE clause may prevent predicate pushdown.",
            suggestion=(
                "Move CAST operations out of filter predicates. "
                "Store data in the correct type, or filter before casting."
            ),
            code_snippet=extract_code_snippet(content, cast_match),
        ))

    # OPT003: DISTINCT with ORDER BY
    distinct_match = re.search(r"SELECT\s+DISTINCT.*ORDER\s+BY", content, re.IGNORECASE | re.DOTALL)
    if distinct_match:
        findings.append(ReviewFinding(
            rule_id="OPT003",
            category="optimization",
            severity="low",
            cell_index=cell_index,
            message="DISTINCT with ORDER BY — both are expensive operations.",
            suggestion="Consider if both are necessary. GROUP BY may be more efficient than DISTINCT.",
            code_snippet=extract_code_snippet(content, distinct_match),
        ))

    # OPT004: Nested subqueries
    subquery_matches = list(re.finditer(r"\(\s*SELECT\b", content, re.IGNORECASE))
    if len(subquery_matches) >= 3:
        findings.append(ReviewFinding(
            rule_id="OPT004",
            category="optimization",
            severity="medium",
            cell_index=cell_index,
            message=f"Deeply nested subqueries ({len(subquery_matches)} levels) — may hurt readability and performance.",
            suggestion="Refactor using CTEs (WITH clause) for better readability and optimizer hints.",
            code_snippet=extract_code_snippet(content, subquery_matches[0]),
        ))

    # OPT005: Partition pruning — filter on partition column
    partition_match = re.search(r"FROM\s+\S+\s+WHERE\s+.*(?:date|dt|year|month|partition)", content, re.IGNORECASE)
    if partition_match:
        if not re.search(r"=|>=|<=|BETWEEN|IN\s*\(", content, re.IGNORECASE):
            findings.append(ReviewFinding(
                rule_id="OPT005",
                category="optimization",
                severity="medium",
                cell_index=cell_index,
                message="Possible partition column in WHERE but no equality/range filter for pruning.",
                suggestion="Use equality (=) or range (BETWEEN) filters on partition columns for optimal pruning.",
                code_snippet=extract_code_snippet(content, partition_match),
            ))

    return findings


def _pyspark_suggestions(
    content: str,
    cell_index: int,
    cell_type: str,
) -> list[ReviewFinding]:
    """PySpark optimization suggestions."""
    findings: list[ReviewFinding] = []

    # OPT010: groupBy().count() vs approx_count_distinct
    groupby_match = re.search(r"\.groupBy\(.*\)\.count\(\)", content)
    if groupby_match:
        findings.append(ReviewFinding(
            rule_id="OPT010",
            category="optimization",
            severity="info",
            cell_index=cell_index,
            message="groupBy().count() — consider if approximate count is sufficient.",
            suggestion="For large datasets, approx_count_distinct() is much faster than exact count.",
            code_snippet=extract_code_snippet(content, groupby_match),
        ))

    # OPT011: withColumn in a loop
    withcol_match = re.search(r"for\s+\w+\s+in\s+.*:\s*\n\s*.*\.withColumn\(", content)
    if withcol_match:
        findings.append(ReviewFinding(
            rule_id="OPT011",
            category="optimization",
            severity="high",
            cell_index=cell_index,
            message="withColumn() inside a loop creates a new DataFrame per iteration — very slow.",
            suggestion=(
                "Use select() with a list of column expressions instead:\n"
                "  df.select([F.col(c).alias(new_name) for c in columns])"
            ),
            code_snippet=extract_code_snippet(content, withcol_match),
        ))

    # OPT012: repartition vs coalesce
    repart_match = re.search(r"\.repartition\(\s*1\s*\)", content)
    if repart_match:
        findings.append(ReviewFinding(
            rule_id="OPT012",
            category="optimization",
            severity="medium",
            cell_index=cell_index,
            message=".repartition(1) causes a full shuffle. Use .coalesce(1) to reduce partitions without shuffle.",
            suggestion="Replace .repartition(1) with .coalesce(1) when reducing to fewer partitions.",
            code_snippet=extract_code_snippet(content, repart_match),
        ))

    # OPT013: Cache pattern
    cache_match = re.search(r"\.cache\(\)", content)
    if cache_match:
        if not re.search(r"\.unpersist\(\)", content):
            findings.append(ReviewFinding(
                rule_id="OPT013",
                category="optimization",
                severity="low",
                cell_index=cell_index,
                message=".cache() without .unpersist() — cached data stays in memory.",
                suggestion="Call .unpersist() when cached DataFrame is no longer needed to free memory.",
                code_snippet=extract_code_snippet(content, cache_match),
            ))

    # OPT014: Use of display() without limit
    display_match = re.search(r"\bdisplay\s*\(\s*\w+\s*\)(?!\s*#)", content)
    if display_match:
        findings.append(ReviewFinding(
            rule_id="OPT014",
            category="optimization",
            severity="info",
            cell_index=cell_index,
            message="display() on a large DataFrame can be slow.",
            suggestion="Use display(df.limit(100)) to preview large DataFrames efficiently.",
            code_snippet=extract_code_snippet(content, display_match),
        ))

    return findings
