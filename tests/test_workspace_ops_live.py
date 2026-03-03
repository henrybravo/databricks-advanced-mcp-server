"""Live integration test for workspace_ops tools against real Databricks.

Loads credentials from .env, exercises all three tools (create_notebook,
create_job, workspace_upload) with real API calls, then cleans up.

Usage:
    python tests/test_workspace_ops_live.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import traceback

from dotenv import load_dotenv

load_dotenv()

from databricks.sdk import WorkspaceClient
from fastmcp import FastMCP

# Ensure the source tree is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from databricks_advanced_mcp.tools.workspace_ops import register


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _call(mcp: FastMCP, tool_name: str, args: dict) -> dict:
    import asyncio
    result = asyncio.run(mcp.call_tool(tool_name, args))
    text = result.content[0].text
    return json.loads(text)


def _print_result(label: str, result: dict) -> None:
    status = "OK" if "error" not in result else "FAIL"
    print(f"  [{status}] {label}")
    print(f"         {json.dumps(result, indent=2)[:500]}")


# ------------------------------------------------------------------
# Cleanup helper
# ------------------------------------------------------------------

_created_jobs: list[int] = []
_created_paths: list[str] = []


def _cleanup():
    """Best-effort cleanup of resources created during the test."""
    print("\n--- Cleanup ---")
    client = WorkspaceClient(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
    )
    for job_id in _created_jobs:
        try:
            client.jobs.delete(job_id)
            print(f"  Deleted job {job_id}")
        except Exception as e:
            print(f"  Failed to delete job {job_id}: {e}")
    for path in _created_paths:
        try:
            client.workspace.delete(path)
            print(f"  Deleted workspace object {path}")
        except Exception as e:
            print(f"  Failed to delete {path}: {e}")


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

def main() -> None:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env")
        sys.exit(1)

    print(f"Testing against: {host}\n")

    mcp = FastMCP("live-test")
    register(mcp)

    test_prefix = "/Shared/_mcp_integration_test"
    all_passed = True

    try:
        # -------------------------------------------------------
        # 1. create_notebook — preview
        # -------------------------------------------------------
        print("1. create_notebook (preview)")
        r = _call(mcp, "create_notebook", {
            "path": f"{test_prefix}/test_nb",
            "language": "PYTHON",
        })
        _print_result("Preview returns action=preview", r)
        assert r["action"] == "preview", f"Expected preview, got {r}"

        # -------------------------------------------------------
        # 2. create_notebook — actual
        # -------------------------------------------------------
        print("\n2. create_notebook (confirm)")
        nb_path = f"{test_prefix}/test_nb"
        r = _call(mcp, "create_notebook", {
            "path": nb_path,
            "language": "PYTHON",
            "content": "# MCP integration test\nprint('hello from MCP')",
            "confirm": True,
        })
        _print_result("Notebook created", r)
        if "error" in r:
            all_passed = False
        else:
            _created_paths.append(nb_path)

        # -------------------------------------------------------
        # 3. create_notebook — invalid language
        # -------------------------------------------------------
        print("\n3. create_notebook (invalid language)")
        r = _call(mcp, "create_notebook", {
            "path": f"{test_prefix}/bad_nb",
            "language": "JAVASCRIPT",
        })
        _print_result("Returns error for bad language", r)
        assert "error" in r, f"Expected error, got {r}"

        # -------------------------------------------------------
        # 4. create_job — preview
        # -------------------------------------------------------
        print("\n4. create_job (preview)")
        r = _call(mcp, "create_job", {
            "name": "mcp-integration-test-job",
            "notebook_path": nb_path,
        })
        _print_result("Preview returns action=preview", r)
        assert r["action"] == "preview", f"Expected preview, got {r}"

        # -------------------------------------------------------
        # 5. create_job — actual
        # -------------------------------------------------------
        print("\n5. create_job (confirm)")
        r = _call(mcp, "create_job", {
            "name": "mcp-integration-test-job",
            "notebook_path": nb_path,
            "confirm": True,
        })
        _print_result("Job created", r)
        if "error" in r:
            all_passed = False
        elif r.get("job_id"):
            _created_jobs.append(int(r["job_id"]))

        # -------------------------------------------------------
        # 6. workspace_upload — preview
        # -------------------------------------------------------
        print("\n6. workspace_upload (preview)")
        # Create a temp file to upload
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write("# Uploaded via MCP integration test\nprint('uploaded')\n")
            tmp_path = f.name

        try:
            r = _call(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": f"{test_prefix}/uploaded_script",
            })
            _print_result("Upload preview", r)
            assert r["action"] == "preview", f"Expected preview, got {r}"

            # ---------------------------------------------------
            # 7. workspace_upload — actual
            # ---------------------------------------------------
            print("\n7. workspace_upload (confirm)")
            upload_path = f"{test_prefix}/uploaded_script"
            r = _call(mcp, "workspace_upload", {
                "local_path": tmp_path,
                "workspace_path": upload_path,
                "overwrite": True,
                "confirm": True,
            })
            _print_result("File uploaded", r)
            if "error" in r:
                all_passed = False
            else:
                _created_paths.append(upload_path)
        finally:
            os.unlink(tmp_path)

        # -------------------------------------------------------
        # 8. workspace_upload — file not found
        # -------------------------------------------------------
        print("\n8. workspace_upload (file not found)")
        r = _call(mcp, "workspace_upload", {
            "local_path": "/this/does/not/exist.py",
            "workspace_path": f"{test_prefix}/nowhere",
        })
        _print_result("Error for missing file", r)
        assert "error" in r, f"Expected error, got {r}"

    except Exception:
        all_passed = False
        traceback.print_exc()
    finally:
        _cleanup()

    print("\n" + "=" * 50)
    if all_passed:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
