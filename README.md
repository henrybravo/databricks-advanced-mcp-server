# Databricks Advanced MCP Server

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![MCP](https://img.shields.io/badge/MCP-compatible-purple.svg)](https://modelcontextprotocol.io)

An advanced [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server that gives AI assistants deep visibility into your Databricks workspace - dependency scanning, impact analysis, notebook review, job/pipeline operations, SQL execution, and table metadata inspection.

## Features

| Domain | What it does |
|---|---|
| **SQL Execution** | Run SQL queries against Databricks SQL warehouses with configurable result limits |
| **Table Information** | Inspect table metadata, schemas, column details, row counts, and storage info |
| **Dependency Scanning** | Scan notebooks, jobs, and DLT pipelines to build a workspace dependency graph (DAG) |
| **Impact Analysis** | Predict downstream breakage from column drops, schema changes, or pipeline failures |
| **Notebook Review** | Detect performance anti-patterns, coding standard violations, and suggest optimizations |
| **Job & Pipeline Ops** | List jobs/pipelines, get run status with error diagnostics, trigger reruns |

## Quick Start

### Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — fast Python package manager
- A **Databricks workspace** with a SQL warehouse
- A Databricks **personal access token**

> **Other auth methods:** The Databricks SDK supports [unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth.html) — if you don't set `DATABRICKS_TOKEN`, it will fall back to Azure CLI, managed identity, or `.databrickscfg`. The `.env` setup below uses a PAT for simplicity.
>
> **Don't have a Databricks workspace yet?** See [`infra/INSTALL.md`](infra/INSTALL.md) for a one-command Azure deployment using Bicep.

### 1. Clone and install

```bash
git clone https://github.com/henrybravo/databricks-advanced-mcp-server.git
cd databricks-advanced-mcp-server
```

Create and activate a virtual environment:

**Windows (PowerShell)**
```powershell
uv venv .venv
.\.venv\Scripts\Activate.ps1
uv pip install -e .
```

**macOS / Linux**
```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e .
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` with your Databricks credentials:

```dotenv
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi_your_token
DATABRICKS_WAREHOUSE_ID=your_warehouse_id

# Optional (defaults shown)
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
```

### 3. Add to your IDE

Create `.vscode/mcp.json` in your project to register the MCP server with VS Code / GitHub Copilot.

#### Option A: Virtual environment activated (recommended)

If you installed into a local `.venv` (as shown above), point directly to the Python interpreter. This is the most reliable approach - it doesn't require the `databricks-mcp` command to be on your system PATH.

**Windows**
```jsonc
{
  "servers": {
    "databricks-mcp": {
      "type": "stdio",
      "command": "${workspaceFolder}/.venv/Scripts/python.exe",
      "args": ["-m", "databricks_advanced_mcp.server"],
      "envFile": "${workspaceFolder}/.env"
    }
  }
}
```

**macOS / Linux**
```jsonc
{
  "servers": {
    "databricks-mcp": {
      "type": "stdio",
      "command": "${workspaceFolder}/.venv/bin/python",
      "args": ["-m", "databricks_advanced_mcp.server"],
      "envFile": "${workspaceFolder}/.env"
    }
  }
}
```

#### Option B: Global / PATH install

If you installed the package globally (e.g., `uv pip install .` without a venv, or via `pipx`), the `databricks-mcp` CLI command is available on your PATH. In this case you can use the simpler config - but you must pass env vars inline since there's no `envFile` relative to the project:

```jsonc
{
  "servers": {
    "databricks-mcp": {
      "type": "stdio",
      "command": "databricks-mcp",
      "env": {
        "DATABRICKS_HOST": "https://adb-xxxx.azuredatabricks.net",
        "DATABRICKS_TOKEN": "dapi_your_token",
        "DATABRICKS_WAREHOUSE_ID": "your_warehouse_id"
      }
    }
  }
}
```

### 4. Start using

Once configured, your AI assistant can call any of the tools below. Try prompts like:

- *"List all tables in the `analytics` schema"*
- *"Review the notebook at `/Users/me/etl_pipeline` for performance issues"*
- *"What would break if I drop the `customer_id` column from `main.sales.orders`?"*
- *"Show me the status of job 12345"*

## MCP Tools

| Tool | Description |
|---|---|
| `execute_query` | Execute SQL against a Databricks SQL warehouse |
| `get_table_info` | Get table metadata — columns, row count, properties, storage |
| `list_tables` | List tables in a catalog.schema |
| `scan_notebook` | Scan a notebook for table/column references |
| `scan_jobs` | Scan all jobs for table dependencies |
| `scan_dlt_pipelines` | Scan all DLT pipelines for source/target tables |
| `build_dependency_graph` | Build the full workspace dependency graph |
| `get_table_dependencies` | Get upstream/downstream dependencies for a table |
| `refresh_graph` | Invalidate and rebuild the dependency graph cache |
| `analyze_impact` | Analyze impact of column drop / schema change / pipeline failure |
| `review_notebook` | Review a notebook for issues, anti-patterns, and optimizations |
| `list_jobs` | List jobs with status and schedule info |
| `get_job_status` | Get detailed job run status with error diagnostics |
| `list_pipelines` | List DLT pipelines with state and update status |
| `get_pipeline_status` | Get pipeline update details with event log |
| `trigger_rerun` | Trigger a job rerun (requires confirmation) |

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABRICKS_HOST` | Yes | — | Workspace URL (e.g., `https://adb-xxx.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Yes | — | Personal access token or service principal token |
| `DATABRICKS_WAREHOUSE_ID` | Yes | — | SQL warehouse ID for query execution |
| `DATABRICKS_CATALOG` | No | `main` | Default catalog for unqualified table names |
| `DATABRICKS_SCHEMA` | No | `default` | Default schema for unqualified table names |

## Infrastructure (Optional)

If you need to provision a new Azure Databricks workspace, the `infra/` directory contains:

- **`main.bicep`** — Azure Bicep template (Premium SKU, Unity Catalog enabled)
- **`deploy.ps1`** — One-command PowerShell deployment script
- **`INSTALL.md`** — Detailed step-by-step deployment guide

```bash
cd infra
./deploy.ps1 -ResourceGroupName rg-databricks-mcp -Location eastus2
```

## Development

```bash
# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest

# Lint
uv run ruff check src/ tests/

# Type check
uv run mypy src/
```

## Architecture

```
src/databricks_advanced_mcp/
├── server.py          # FastMCP server + CLI entry point
├── config.py          # Pydantic settings from env vars
├── client.py          # Databricks SDK client factory
├── tools/             # MCP tool implementations
│   ├── sql_executor.py
│   ├── table_info.py
│   ├── dependency_scanner.py
│   ├── impact_analysis.py
│   ├── notebook_reviewer.py
│   └── job_pipeline_ops.py
├── parsers/           # Code parsing engines
│   ├── sql_parser.py       # sqlglot-based SQL extraction
│   ├── notebook_parser.py  # Databricks notebook cell parsing
│   └── dlt_parser.py       # DLT pipeline definition parsing
├── graph/             # Dependency graph
│   ├── models.py      # Node, Edge, DependencyGraph data models
│   ├── builder.py     # Graph builder (orchestrates scans)
│   └── cache.py       # In-memory graph cache with TTL
└── reviewers/         # Notebook review rule engines
    ├── performance.py # Performance anti-patterns
    ├── standards.py   # Coding standards checks
    └── suggestions.py # Optimization suggestions
```

## License

[MIT](LICENSE)
