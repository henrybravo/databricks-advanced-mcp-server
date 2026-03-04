"""Notebook parser for Databricks notebooks.

Parses notebook source (Databricks export format) into cells, classifies
cell types, and extracts table references from SQL and PySpark code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

from databricks_advanced_mcp.parsers.sql_parser import TableReference


class CellType(StrEnum):
    """Types of notebook cells."""

    SQL = "sql"
    PYTHON = "python"
    SCALA = "scala"
    R = "r"
    MARKDOWN = "markdown"
    MAGIC = "magic"
    UNKNOWN = "unknown"


@dataclass
class NotebookCell:
    """A single cell from a notebook."""

    index: int
    content: str
    cell_type: CellType
    language: str = ""


@dataclass
class NotebookParseResult:
    """Result of parsing a notebook."""

    cells: list[NotebookCell] = field(default_factory=list)
    default_language: str = "python"
    table_references: list[TableReference] = field(default_factory=list)


# Magic command patterns
_MAGIC_PATTERN = re.compile(r"^\s*%(\w+)\s*(.*)", re.MULTILINE | re.DOTALL)
_MAGIC_SQL_PATTERN = re.compile(r"^\s*%sql\s*(.*)", re.MULTILINE | re.DOTALL)

# PySpark table reference patterns
_SPARK_TABLE_PATTERNS: list[re.Pattern[str]] = [
    # spark.table("catalog.schema.table") or spark.table('table')
    re.compile(r"""spark\.table\(\s*["\']([^"\']+)["\']\s*\)"""),
    # spark.read.table("catalog.schema.table")
    re.compile(r"""spark\.read\.table\(\s*["\']([^"\']+)["\']\s*\)"""),
    # DeltaTable.forName(spark, "catalog.schema.table")
    re.compile(r"""DeltaTable\.forName\(\s*\w+\s*,\s*["\']([^"\']+)["\']\s*\)"""),
    # spark.read.format("delta").load("path") — extract path as reference
    re.compile(r"""spark\.read\.format\(\s*["\']delta["\']\s*\)\.load\(\s*["\']([^"\']+)["\']\s*\)"""),
    # spark.sql("SELECT ... FROM table")
    re.compile(r"""spark\.sql\(\s*(?:f?["\']|f?\"\"\")(.*?)(?:["\']|\"\"\")\s*\)""", re.DOTALL),
]

# Write patterns (PySpark)
_SPARK_WRITE_PATTERNS: list[re.Pattern[str]] = [
    # .write.saveAsTable("catalog.schema.table")
    re.compile(r"""\.write(?:\.\w+\([^)]*\))*\.saveAsTable\(\s*["\']([^"\']+)["\']\s*\)"""),
    # .write.insertInto("catalog.schema.table")
    re.compile(r"""\.write(?:\.\w+\([^)]*\))*\.insertInto\(\s*["\']([^"\']+)["\']\s*\)"""),
    # .writeTo("catalog.schema.table")
    re.compile(r"""\.writeTo\(\s*["\']([^"\']+)["\']\s*\)"""),
]

# DLT-specific patterns
_DLT_READ_PATTERNS: list[re.Pattern[str]] = [
    # dlt.read("table_name")
    re.compile(r"""dlt\.read\(\s*["\']([^"\']+)["\']\s*\)"""),
    # dlt.read_stream("table_name")
    re.compile(r"""dlt\.read_stream\(\s*["\']([^"\']+)["\']\s*\)"""),
    # spark.read.table("table") in DLT context
    re.compile(r"""spark\.readStream\.table\(\s*["\']([^"\']+)["\']\s*\)"""),
]


def parse_notebook(source: str, default_language: str = "python") -> NotebookParseResult:
    """Parse a Databricks notebook source into cells.

    Databricks notebooks use `# COMMAND ----------` as cell separators
    (for Python/Scala/R) or `-- COMMAND ----------` for SQL.

    Args:
        source: Raw notebook source code.
        default_language: Default language of the notebook.

    Returns:
        NotebookParseResult with parsed cells and extracted table references.
    """
    result = NotebookParseResult(default_language=default_language)

    # Split into cells using Databricks cell separator
    cell_separator = re.compile(r"#\s*COMMAND\s*-{5,}|--\s*COMMAND\s*-{5,}")
    raw_cells = cell_separator.split(source)

    for i, raw_content in enumerate(raw_cells):
        content = raw_content.strip()
        if not content:
            continue

        cell_type, language, cleaned_content = _classify_cell(content, default_language)

        cell = NotebookCell(
            index=i,
            content=cleaned_content,
            cell_type=cell_type,
            language=language,
        )
        result.cells.append(cell)

    # Extract table references from all cells
    result.table_references = extract_table_references(result.cells)

    return result


def _classify_cell(content: str, default_language: str) -> tuple[CellType, str, str]:
    """Classify a cell's type based on magic commands or default language.

    Returns (cell_type, language, cleaned_content).
    """
    # Check for magic commands
    magic_match = _MAGIC_PATTERN.match(content)
    if magic_match:
        magic_cmd = magic_match.group(1).lower()
        magic_content = magic_match.group(2).strip()

        magic_to_type = {
            "sql": (CellType.SQL, "sql"),
            "python": (CellType.PYTHON, "python"),
            "scala": (CellType.SCALA, "scala"),
            "r": (CellType.R, "r"),
            "md": (CellType.MARKDOWN, "markdown"),
            "sh": (CellType.MAGIC, "shell"),
            "fs": (CellType.MAGIC, "dbutils"),
            "pip": (CellType.MAGIC, "pip"),
            "run": (CellType.MAGIC, "run"),
        }

        if magic_cmd in magic_to_type:
            cell_type, lang = magic_to_type[magic_cmd]
            return cell_type, lang, magic_content

    # Check for markdown cells (starting with # MAGIC %md)
    if content.startswith("# MAGIC %md"):
        cleaned = re.sub(r"^#\s*MAGIC\s*%md\s*", "", content, flags=re.MULTILINE)
        cleaned = re.sub(r"^#\s*MAGIC\s*", "", cleaned, flags=re.MULTILINE)
        return CellType.MARKDOWN, "markdown", cleaned.strip()

    # Default language
    lang_to_type = {
        "python": CellType.PYTHON,
        "sql": CellType.SQL,
        "scala": CellType.SCALA,
        "r": CellType.R,
    }

    return lang_to_type.get(default_language, CellType.UNKNOWN), default_language, content


def extract_table_references(cells: list[NotebookCell]) -> list[TableReference]:
    """Extract table references from notebook cells.

    Combines SQL parsing results with PySpark regex extraction.
    """
    from databricks_advanced_mcp.parsers.sql_parser import parse_sql

    references: list[TableReference] = []

    for cell in cells:
        if cell.cell_type == CellType.SQL:
            # Parse SQL cells with sqlglot
            sql_result = parse_sql(cell.content)
            references.extend(sql_result.tables)

        elif cell.cell_type == CellType.PYTHON:
            # Extract PySpark table references via regex
            references.extend(_extract_pyspark_references(cell.content))

            # Also check for inline SQL in spark.sql() calls
            for match in _SPARK_TABLE_PATTERNS[-1].finditer(cell.content):
                sql_snippet = match.group(1)
                # Only parse if it looks like SQL (contains SELECT, INSERT, etc.)
                if re.search(r"\b(SELECT|INSERT|CREATE|MERGE|UPDATE|DELETE)\b", sql_snippet, re.IGNORECASE):
                    sql_result = parse_sql(sql_snippet)
                    references.extend(sql_result.tables)

        elif cell.cell_type == CellType.MARKDOWN:
            continue  # Skip markdown cells

    # Deduplicate
    seen: set[tuple[str, str]] = set()
    unique: list[TableReference] = []
    for ref in references:
        key = (ref.fqn, ref.reference_type)
        if key not in seen:
            seen.add(key)
            unique.append(ref)

    return unique


def _extract_pyspark_references(code: str) -> list[TableReference]:
    """Extract table references from PySpark code using regex patterns."""
    references: list[TableReference] = []

    # Read patterns (all except the last spark.sql pattern)
    for pattern in _SPARK_TABLE_PATTERNS[:-1]:
        for match in pattern.finditer(code):
            table_str = match.group(1)
            ref = _parse_table_string(table_str, reference_type="reads_from")
            if ref:
                references.append(ref)

    # DLT read patterns
    for pattern in _DLT_READ_PATTERNS:
        for match in pattern.finditer(code):
            table_str = match.group(1)
            ref = _parse_table_string(table_str, reference_type="reads_from")
            if ref:
                references.append(ref)

    # Write patterns
    for pattern in _SPARK_WRITE_PATTERNS:
        for match in pattern.finditer(code):
            table_str = match.group(1)
            ref = _parse_table_string(table_str, reference_type="writes_to")
            if ref:
                references.append(ref)

    return references


def _parse_table_string(table_str: str, reference_type: str = "reads_from") -> TableReference | None:
    """Parse a table string like 'catalog.schema.table' into a TableReference."""
    if not table_str or table_str.startswith("/"):
        return None  # Skip paths

    parts = table_str.split(".")
    if len(parts) == 3:
        return TableReference(
            catalog=parts[0], schema=parts[1], table=parts[2], reference_type=reference_type
        )
    elif len(parts) == 2:
        return TableReference(schema=parts[0], table=parts[1], reference_type=reference_type)
    elif len(parts) == 1:
        return TableReference(table=parts[0], reference_type=reference_type)
    return None
