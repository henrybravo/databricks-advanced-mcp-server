"""DLT pipeline parser.

Extracts source/target table references from DLT pipeline definitions
and associated notebook code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from databricks_advanced_mcp.parsers.sql_parser import TableReference


@dataclass
class DLTPipelineInfo:
    """Parsed information about a DLT pipeline."""

    pipeline_id: str
    name: str
    target_schema: str | None = None
    target_catalog: str | None = None
    source_tables: list[TableReference] = field(default_factory=list)
    target_tables: list[TableReference] = field(default_factory=list)
    notebook_paths: list[str] = field(default_factory=list)


# DLT decorator patterns in notebook code
_DLT_TABLE_PATTERN = re.compile(
    r"""@dlt\.table\s*\((?:[^)]*name\s*=\s*["\']([^"\']+)["\'])?""",
    re.MULTILINE,
)

_DLT_VIEW_PATTERN = re.compile(
    r"""@dlt\.view\s*\((?:[^)]*name\s*=\s*["\']([^"\']+)["\'])?""",
    re.MULTILINE,
)

_DLT_READ_PATTERN = re.compile(
    r"""(?:dlt\.read|dlt\.read_stream)\(\s*["\']([^"\']+)["\']\s*\)"""
)

_SPARK_READ_STREAM_TABLE = re.compile(
    r"""spark\.readStream\.table\(\s*["\']([^"\']+)["\']\s*\)"""
)


def parse_dlt_pipeline_config(pipeline_config: dict[str, Any]) -> DLTPipelineInfo:
    """Parse a DLT pipeline API response into structured info.

    Args:
        pipeline_config: Raw pipeline definition from the Pipelines API.

    Returns:
        DLTPipelineInfo with extracted metadata and notebook paths.
    """
    pipeline_id = pipeline_config.get("pipeline_id", "")
    name = pipeline_config.get("name", "")
    target_schema = pipeline_config.get("target", pipeline_config.get("schema"))
    target_catalog = pipeline_config.get("catalog")

    notebook_paths: list[str] = []
    libraries = pipeline_config.get("libraries", [])
    for lib in libraries:
        if isinstance(lib, dict):
            notebook = lib.get("notebook", {})
            if isinstance(notebook, dict) and "path" in notebook:
                notebook_paths.append(notebook["path"])

    return DLTPipelineInfo(
        pipeline_id=pipeline_id,
        name=name,
        target_schema=target_schema,
        target_catalog=target_catalog,
        notebook_paths=notebook_paths,
    )


def extract_dlt_references_from_code(
    code: str,
    target_catalog: str | None = None,
    target_schema: str | None = None,
) -> tuple[list[TableReference], list[TableReference]]:
    """Extract DLT source and target table references from notebook code.

    Args:
        code: Notebook source code to analyze.
        target_catalog: Pipeline target catalog for qualifying output tables.
        target_schema: Pipeline target schema for qualifying output tables.

    Returns:
        Tuple of (source_tables, target_tables).
    """
    source_tables: list[TableReference] = []
    target_tables: list[TableReference] = []

    # Extract target tables from @dlt.table decorators
    for match in _DLT_TABLE_PATTERN.finditer(code):
        table_name = match.group(1)
        if not table_name:
            # Try to get the function name as the table name
            func_match = re.search(r"def\s+(\w+)\s*\(", code[match.end() :])
            if func_match:
                table_name = func_match.group(1)
        if table_name:
            target_tables.append(
                TableReference(
                    catalog=target_catalog,
                    schema=target_schema,
                    table=table_name,
                    reference_type="writes_to",
                )
            )

    # Extract target views from @dlt.view decorators
    for match in _DLT_VIEW_PATTERN.finditer(code):
        view_name = match.group(1)
        if not view_name:
            func_match = re.search(r"def\s+(\w+)\s*\(", code[match.end() :])
            if func_match:
                view_name = func_match.group(1)
        if view_name:
            target_tables.append(
                TableReference(
                    catalog=target_catalog,
                    schema=target_schema,
                    table=view_name,
                    reference_type="writes_to",
                )
            )

    # Extract source tables from dlt.read / dlt.read_stream
    for match in _DLT_READ_PATTERN.finditer(code):
        table_str = match.group(1)
        parts = table_str.split(".")
        if len(parts) == 3:
            source_tables.append(
                TableReference(catalog=parts[0], schema=parts[1], table=parts[2], reference_type="reads_from")
            )
        elif len(parts) == 2:
            source_tables.append(
                TableReference(schema=parts[0], table=parts[1], reference_type="reads_from")
            )
        else:
            source_tables.append(
                TableReference(
                    catalog=target_catalog,
                    schema=target_schema,
                    table=table_str,
                    reference_type="reads_from",
                )
            )

    # Extract from spark.readStream.table
    for match in _SPARK_READ_STREAM_TABLE.finditer(code):
        table_str = match.group(1)
        parts = table_str.split(".")
        if len(parts) == 3:
            source_tables.append(
                TableReference(catalog=parts[0], schema=parts[1], table=parts[2], reference_type="reads_from")
            )
        elif len(parts) == 2:
            source_tables.append(
                TableReference(schema=parts[0], table=parts[1], reference_type="reads_from")
            )
        else:
            source_tables.append(
                TableReference(table=table_str, reference_type="reads_from")
            )

    return source_tables, target_tables
