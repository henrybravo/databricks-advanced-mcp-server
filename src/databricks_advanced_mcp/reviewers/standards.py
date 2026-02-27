"""Coding standards review rules for Databricks notebooks.

Checks for documentation, naming conventions, hardcoded credentials,
and structural best practices.
"""

from __future__ import annotations

import re

from databricks_advanced_mcp.reviewers.performance import ReviewFinding, extract_code_snippet


# ------------------------------------------------------------------
# Standards rules
# ------------------------------------------------------------------

def check_standards(
    cell_content: str,
    cell_index: int,
    cell_type: str,
) -> list[ReviewFinding]:
    """Run coding standards rules against a notebook cell."""
    findings: list[ReviewFinding] = []

    if cell_type in ("python", "scala"):
        findings.extend(_check_python_standards(cell_content, cell_index, cell_type))

    if cell_type == "sql":
        findings.extend(_check_sql_standards(cell_content, cell_index))

    # Universal checks
    findings.extend(_check_credentials(cell_content, cell_index, cell_type))

    return findings


def _check_python_standards(
    content: str,
    cell_index: int,
    cell_type: str,
) -> list[ReviewFinding]:
    """Check Python/Scala code standards."""
    findings: list[ReviewFinding] = []

    # STD001: Missing docstring for function definitions
    func_pattern = re.compile(r"def\s+\w+\s*\(")
    docstring_pattern = re.compile(r'def\s+\w+\s*\([^)]*\)\s*(?:->.*)?:\s*\n\s*(?:\"\"\"|\'\'\')' )
    
    func_match = func_pattern.search(content)
    if func_match and not docstring_pattern.search(content):
        findings.append(ReviewFinding(
            rule_id="STD001",
            category="standards",
            severity="low",
            cell_index=cell_index,
            message="Function definition without docstring.",
            suggestion="Add a docstring to document the function's purpose, parameters, and return value.",
            code_snippet=extract_code_snippet(content, func_match),
        ))

    # STD002: Wildcard imports
    wildcard_match = re.search(r"from\s+\S+\s+import\s+\*", content)
    if wildcard_match:
        findings.append(ReviewFinding(
            rule_id="STD002",
            category="standards",
            severity="medium",
            cell_index=cell_index,
            message="Wildcard import (import *) detected.",
            suggestion="Import specific names to improve readability and avoid namespace conflicts.",
            code_snippet=extract_code_snippet(content, wildcard_match),
        ))

    # STD003: Hardcoded paths
    path_match = re.search(r'["\'](?:dbfs:/|/mnt/|abfss://|s3://|gs://)[^"\']+["\']', content)
    if path_match:
        findings.append(ReviewFinding(
            rule_id="STD003",
            category="standards",
            severity="medium",
            cell_index=cell_index,
            message="Hardcoded storage path detected.",
            suggestion="Use configuration variables or Unity Catalog volumes instead of hardcoded paths.",
            code_snippet=extract_code_snippet(content, path_match),
        ))

    # STD004: Magic commands in production code  
    magic_match = re.search(r"^\s*%(?:sh|pip|fs)\b", content, re.MULTILINE)
    if magic_match:
        findings.append(ReviewFinding(
            rule_id="STD004",
            category="standards",
            severity="low",
            cell_index=cell_index,
            message="Magic command (%sh, %pip, %fs) used — not recommended in production notebooks.",
            suggestion="Use dbutils or cluster libraries/init scripts for production workflows.",
            code_snippet=extract_code_snippet(content, magic_match),
        ))

    # STD005: print() for debugging
    print_match = re.search(r"\bprint\s*\(", content)
    if print_match:
        findings.append(ReviewFinding(
            rule_id="STD005",
            category="standards",
            severity="info",
            cell_index=cell_index,
            message="print() statement found — may be leftover debug output.",
            suggestion="Use logging module or remove print statements in production code.",
            code_snippet=extract_code_snippet(content, print_match),
        ))

    return findings


def _check_sql_standards(content: str, cell_index: int) -> list[ReviewFinding]:
    """Check SQL coding standards."""
    findings: list[ReviewFinding] = []

    # STD010: SELECT * usage
    select_star_match = re.search(r"SELECT\s+\*", content, re.IGNORECASE)
    if select_star_match:
        findings.append(ReviewFinding(
            rule_id="STD010",
            category="standards",
            severity="low",
            cell_index=cell_index,
            message="SELECT * used — consider specifying column names explicitly.",
            suggestion="List specific columns to improve readability and reduce data transfer.",
            code_snippet=extract_code_snippet(content, select_star_match),
        ))

    # STD011: Missing schema qualification
    unqualified_match = re.search(
        r"(?:FROM|JOIN|INTO)\s+(?![\w]+\.[\w]+)(\w+)(?:\s|;|$)",
        content,
        re.IGNORECASE,
    )
    if unqualified_match:
        findings.append(ReviewFinding(
            rule_id="STD011",
            category="standards",
            severity="medium",
            cell_index=cell_index,
            message="Unqualified table name — may resolve differently across environments.",
            suggestion="Use fully-qualified table names (catalog.schema.table) for clarity and portability.",
            code_snippet=extract_code_snippet(content, unqualified_match),
        ))

    return findings


def _check_credentials(
    content: str,
    cell_index: int,
    cell_type: str,
) -> list[ReviewFinding]:
    """Check for hardcoded credentials or secrets."""
    findings: list[ReviewFinding] = []

    credential_patterns = [
        (r'(?:password|passwd|pwd)\s*=\s*["\'][^"\']+["\']', "Hardcoded password detected."),
        (r'(?:token|api_key|apikey|secret)\s*=\s*["\'][^"\']+["\']', "Hardcoded token/API key detected."),
        (r'(?:dapi)[a-f0-9]{32,}', "Possible Databricks personal access token detected."),
        (r'(?:Bearer\s+)[a-zA-Z0-9\-._~+/]+=*', "Hardcoded Bearer token detected."),
    ]

    for pattern, message in credential_patterns:
        cred_match = re.search(pattern, content, re.IGNORECASE)
        if cred_match:
            findings.append(ReviewFinding(
                rule_id="STD020",
                category="standards",
                severity="critical",
                cell_index=cell_index,
                message=message,
                suggestion="Use Databricks secrets (dbutils.secrets.get()) or environment variables instead.",
                code_snippet=extract_code_snippet(content, cred_match),
            ))
            break  # One credential finding per cell is enough

    return findings
