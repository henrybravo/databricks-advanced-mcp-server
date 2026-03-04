"""SQL parser for extracting table and column references.

Uses sqlglot with Databricks dialect to parse SQL statements and extract
fully-qualified table references and column lineage.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


@dataclass
class TableReference:
    """A reference to a table found in SQL or code."""

    catalog: str | None = None
    schema: str | None = None
    table: str = ""
    alias: str | None = None
    reference_type: str = "reads_from"  # reads_from | writes_to

    @property
    def fqn(self) -> str:
        """Return the fully-qualified name (catalog.schema.table)."""
        parts = [p for p in [self.catalog, self.schema, self.table] if p]
        return ".".join(parts)


@dataclass
class ColumnReference:
    """A reference to a column found in SQL."""

    table_fqn: str = ""
    column: str = ""
    context: str = ""  # select, where, join, group_by, order_by


@dataclass
class SQLParseResult:
    """Result of parsing a SQL statement."""

    tables: list[TableReference] = field(default_factory=list)
    columns: list[ColumnReference] = field(default_factory=list)
    statement_type: str = ""
    errors: list[str] = field(default_factory=list)


def parse_sql(sql: str, default_catalog: str = "", default_schema: str = "") -> SQLParseResult:
    """Parse a SQL statement and extract table/column references.

    Args:
        sql: SQL statement to parse.
        default_catalog: Default catalog for unqualified table names.
        default_schema: Default schema for unqualified table names.

    Returns:
        SQLParseResult with extracted references.
    """
    result = SQLParseResult()

    try:
        parsed = sqlglot.parse(sql, read="databricks")
    except sqlglot.errors.ParseError as e:
        result.errors.append(f"SQL parse error: {e}")
        return result

    for statement in parsed:
        if statement is None:
            continue

        # Determine statement type
        result.statement_type = type(statement).__name__

        # Extract table references
        _extract_tables(statement, result, default_catalog, default_schema)

        # Extract column references
        _extract_columns(statement, result)

    return result


def _extract_tables(
    statement: exp.Expression,
    result: SQLParseResult,
    default_catalog: str,
    default_schema: str,
) -> None:
    """Extract table references from a parsed SQL statement."""
    # Determine write targets (INSERT INTO, CREATE TABLE AS, MERGE INTO)
    write_targets: set[str] = set()

    for insert in statement.find_all(exp.Insert):
        table = insert.find(exp.Table)
        if table:
            write_targets.add(_table_fqn(table))

    for create in statement.find_all(exp.Create):
        table = create.find(exp.Table)
        if table:
            write_targets.add(_table_fqn(table))

    for merge in statement.find_all(exp.Merge):
        table = merge.find(exp.Table)
        if table:
            write_targets.add(_table_fqn(table))

    # Extract all table references
    for table in statement.find_all(exp.Table):
        table_name = table.name
        if not table_name:
            continue

        catalog = table.catalog or default_catalog or None
        schema = table.db or default_schema or None

        fqn = _table_fqn(table)
        ref_type = "writes_to" if fqn in write_targets else "reads_from"

        ref = TableReference(
            catalog=catalog,
            schema=schema,
            table=table_name,
            alias=table.alias,
            reference_type=ref_type,
        )
        # Avoid duplicates
        already = any(
            existing.fqn == ref.fqn and existing.reference_type == ref.reference_type
            for existing in result.tables
        )
        if not already:
            result.tables.append(ref)


def _extract_columns(statement: exp.Expression, result: SQLParseResult) -> None:
    """Extract column references from a parsed SQL statement."""
    for column in statement.find_all(exp.Column):
        col_name = column.name
        if not col_name:
            continue

        # Determine context
        context = "unknown"
        parent = column.parent
        while parent:
            if isinstance(parent, exp.Where):
                context = "where"
                break
            if isinstance(parent, exp.Join):
                context = "join"
                break
            if isinstance(parent, exp.Group):
                context = "group_by"
                break
            if isinstance(parent, exp.Order):
                context = "order_by"
                break
            if isinstance(parent, exp.Select):
                context = "select"
                break
            parent = parent.parent

        table_name = column.table or ""
        result.columns.append(
            ColumnReference(
                table_fqn=table_name,
                column=col_name,
                context=context,
            )
        )


def _table_fqn(table: exp.Table) -> str:
    """Build a simple FQN string from a sqlglot Table expression."""
    parts = [p for p in [table.catalog, table.db, table.name] if p]
    return ".".join(parts)


def extract_table_names(sql: str) -> list[str]:
    """Quick helper to extract just table names from SQL.

    Returns a deduplicated list of table FQN strings.
    """
    result = parse_sql(sql)
    return list({ref.fqn for ref in result.tables})
