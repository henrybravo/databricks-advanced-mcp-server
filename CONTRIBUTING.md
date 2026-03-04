# Contributing to Databricks Advanced MCP Server

Thank you for your interest in contributing! This guide covers everything you need to get started.

## Development Setup

### Prerequisites
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (fast Python package manager)
- A Databricks workspace + SQL warehouse (for integration tests)

### 1. Fork & clone

```bash
git clone https://github.com/<your-fork>/databricks-advanced-mcp-server.git
cd databricks-advanced-mcp-server
```

### 2. Create a virtual environment

```bash
uv venv .venv
source .venv/bin/activate        # macOS / Linux
# .\.venv\Scripts\Activate.ps1  # Windows PowerShell
```

### 3. Install with dev dependencies

```bash
uv pip install -e ".[dev]"
uv pip install pytest-cov
```

### 4. Set up environment variables

```bash
cp .env.example .env
# Edit .env with your Databricks credentials
```

### 5. Run the checks

```bash
# Tests (excluding live integration tests)
uv run pytest tests/ --ignore=tests/test_workspace_ops_live.py -v

# Tests with coverage
uv run pytest tests/ --cov=src/databricks_advanced_mcp --cov-report=term-missing --ignore=tests/test_workspace_ops_live.py

# Lint
uv run ruff check src/ tests/

# Type check
uv run mypy src/
```

---

## Project Structure

```
src/databricks_advanced_mcp/
├── server.py          # FastMCP server + CLI entry point
├── config.py          # Pydantic settings (env vars)
├── client.py          # Databricks SDK client factory
├── tools/             # MCP tool implementations (one file per domain)
├── parsers/           # Code parsing (SQL, notebook cells, DLT)
├── graph/             # Dependency graph (models, builder, cache)
└── reviewers/         # Notebook review rule engines
```

Adding a new **MCP tool**:
1. Add a function decorated with `@mcp.tool()` inside the appropriate `tools/*.py` file's `register()` function.
2. Register the module in `tools/__init__.py` if creating a new file.
3. Add a test in `tests/test_tools_integration.py`.
4. Document the tool in `README.md` (MCP Tools table + example prompt).

Adding a new **reviewer rule**:
1. Add a rule to `_PERFORMANCE_RULES` in `reviewers/performance.py`, or add a check in `reviewers/suggestions.py` / `reviewers/standards.py`.
2. Use a unique `id` following the existing naming scheme (`PERF###`, `OPT###`, `STD###`).
3. Add a test case in `tests/test_reviewers.py`.

---

## Pull Request Checklist

Before submitting a PR, please make sure:

- [ ] All existing tests pass: `uv run pytest tests/ --ignore=tests/test_workspace_ops_live.py`
- [ ] New functionality is covered by tests
- [ ] Ruff linting passes: `uv run ruff check src/ tests/`
- [ ] Type check passes: `uv run mypy src/`
- [ ] README updated if new tools, config options, or reviewer rules were added
- [ ] Commit messages are descriptive and follow conventional commits (`feat:`, `fix:`, `docs:`, etc.)

---

## Wanted Features

Looking for something to contribute? Here are areas where help would be especially valuable:

### Notebook Reviewer
- [ ] MLflow: detect missing `mlflow.end_run()`, `mlflow.log_params()`, `mlflow.log_metrics()`
- [ ] Delta: detect missing liquid clustering (`CLUSTER BY`) on new tables
- [ ] Delta: detect missing Z-ORDER on high-cardinality join/filter columns
- [ ] Streaming: detect `foreachBatch` without checkpointing
- [ ] Cost: detect cluster/warehouse usage patterns that imply waste

### Dependency Graph
- [ ] Event-driven graph refresh via Databricks job webhooks
- [ ] Export graph as Mermaid / DOT / JSON for external visualization
- [ ] Support for non-notebook Python/SQL task dependencies

### Auth & Security
- [ ] OAuth M2M (service principal) setup guide and example
- [ ] Least-privilege UC grants reference for each tool category
- [ ] Per-tool read-only mode flag

### Deployment
- [ ] `app.yaml` for Databricks Apps hosting
- [ ] Docker image and `docker-compose.yml` for local hosting
- [ ] Helm chart for Kubernetes deployment

### Testing
- [ ] Mock Databricks client fixture for unit testing tools
- [ ] Snapshot-based tests for notebook reviewer rules

---

## Code Style

- **Python**: follow [PEP 8](https://peps.python.org/pep-0008/); enforced by `ruff`
- **Type annotations**: all public functions must be typed; enforced by `mypy`
- **Docstrings**: use Google-style docstrings for all public functions
- **Line length**: 120 characters (configured in `pyproject.toml`)

---

## Reporting Issues

Please use [GitHub Issues](https://github.com/henrybravo/databricks-advanced-mcp-server/issues) to report bugs or request features.

When reporting a bug, include:
- Python version (`python --version`)
- Package version (`pip show databricks-advanced-mcp`)
- Databricks cloud (Azure / AWS / GCP) and workspace version
- Minimal reproduction steps and full error traceback

---

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
