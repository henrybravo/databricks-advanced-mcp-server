"""Job and pipeline operations MCP tools.

Provides tools for listing jobs/pipelines, checking run status,
triggering reruns, and diagnosing failures.
"""

from __future__ import annotations

import json
import re
from typing import Any

from fastmcp import FastMCP

from databricks_advanced_mcp.client import get_workspace_client
from databricks_advanced_mcp.config import get_settings

# ------------------------------------------------------------------
# Diagnostic engine
# ------------------------------------------------------------------

_ERROR_PATTERNS: list[dict[str, Any]] = [
    {
        "pattern": re.compile(r"(?:schema|column).*(?:not found|missing|does not exist)", re.IGNORECASE),
        "cause": "schema_drift",
        "suggestion": "Check for upstream schema changes. Run impact analysis to identify affected assets.",
    },
    {
        "pattern": re.compile(r"(?:out of memory|OOM|java\.lang\.OutOfMemoryError)", re.IGNORECASE),
        "cause": "resource_contention",
        "suggestion": "Increase driver/executor memory, reduce data volume, or optimize the query.",
    },
    {
        "pattern": re.compile(r"(?:timeout|timed out|deadline exceeded)", re.IGNORECASE),
        "cause": "timeout",
        "suggestion": "Increase timeout settings, optimize the query, or check for resource contention.",
    },
    {
        "pattern": re.compile(r"(?:permission denied|access denied|unauthorized|forbidden)", re.IGNORECASE),
        "cause": "permission_error",
        "suggestion": "Review table/resource ACLs and service principal permissions.",
    },
    {
        "pattern": re.compile(r"(?:connection refused|network|unreachable|DNS)", re.IGNORECASE),
        "cause": "connectivity",
        "suggestion": "Check network connectivity, firewall rules, and endpoint availability.",
    },
    {
        "pattern": re.compile(r"(?:concurrent.*(?:update|write)|conflict|optimistic locking)", re.IGNORECASE),
        "cause": "concurrency_conflict",
        "suggestion": "Implement retry logic or schedule jobs to avoid concurrent writes to the same table.",
    },
    {
        "pattern": re.compile(r"(?:FileNotFoundException|path does not exist|No such file)", re.IGNORECASE),
        "cause": "missing_data",
        "suggestion": "Verify upstream data has been produced. Check storage paths and mount points.",
    },
    {
        "pattern": re.compile(r"(?:ParseException|AnalysisException|syntax error)", re.IGNORECASE),
        "cause": "query_error",
        "suggestion": "Fix the SQL syntax or semantic error in the query/notebook.",
    },
]


def _diagnose_error(error_message: str) -> dict[str, str]:
    """Classify an error message and suggest a fix."""
    for entry in _ERROR_PATTERNS:
        if entry["pattern"].search(error_message):
            return {
                "probable_cause": entry["cause"],
                "suggestion": entry["suggestion"],
            }
    return {
        "probable_cause": "unknown",
        "suggestion": "Review the full error message and stack trace for details.",
    }


# ------------------------------------------------------------------
# MCP Tool registration
# ------------------------------------------------------------------

def register(mcp: FastMCP) -> None:
    """Register job and pipeline operations tools."""

    @mcp.tool()
    def list_jobs(name_filter: str = "") -> str:
        """List all Databricks jobs with status and schedule.

        Args:
            name_filter: Optional name substring to filter jobs (case-insensitive).

        Returns:
            JSON list of jobs with id, name, schedule, and last run status.
        """
        client = get_workspace_client()

        try:
            jobs = list(client.jobs.list())
        except Exception as e:
            return json.dumps({"error": f"Failed to list jobs: {e}"})

        results: list[dict[str, Any]] = []
        for job in jobs:
            job_name = job.settings.name if job.settings else ""

            if name_filter and name_filter.lower() not in (job_name or "").lower():
                continue

            schedule = None
            if job.settings and job.settings.schedule:
                schedule = {
                    "cron": job.settings.schedule.quartz_cron_expression,
                    "timezone": job.settings.schedule.timezone_id,
                    "paused": getattr(job.settings.schedule, "pause_status", None),
                }

            results.append({
                "job_id": str(job.job_id),
                "name": job_name,
                "schedule": schedule,
                "creator": getattr(job, "creator_user_name", None),
            })

        return json.dumps({
            "job_count": len(results),
            "jobs": results,
        }, indent=2)

    @mcp.tool()
    def get_job_status(job_id: str) -> str:
        """Get detailed run status and errors for a specific job.

        Args:
            job_id: The Databricks job ID.

        Returns:
            JSON with run status, duration, task-level status, and error diagnostics.
        """
        client = get_workspace_client()

        # Always fetch job definition for metadata
        job_meta: dict[str, Any] = {"job_id": job_id}
        try:
            job_detail = client.jobs.get(int(job_id))
            if job_detail.settings:
                job_meta["name"] = job_detail.settings.name
                if job_detail.settings.schedule:
                    job_meta["schedule"] = {
                        "cron": job_detail.settings.schedule.quartz_cron_expression,
                        "timezone": job_detail.settings.schedule.timezone_id,
                    }
                defined_tasks = job_detail.settings.tasks or []
                job_meta["defined_task_count"] = len(defined_tasks)
                job_meta["defined_tasks"] = [
                    t.task_key for t in defined_tasks
                ]
        except Exception:
            pass  # Non-fatal — proceed with run info

        try:
            runs = list(client.jobs.list_runs(job_id=int(job_id), limit=1))
        except Exception as e:
            return json.dumps({"error": f"Failed to get runs for job {job_id}: {e}"})

        if not runs:
            job_meta["message"] = "No runs found for this job."
            return json.dumps(job_meta, indent=2)

        run = runs[0]
        run_state = run.state

        state_info: dict[str, Any] = {}
        if run_state:
            state_info = {
                "life_cycle_state": str(run_state.life_cycle_state) if run_state.life_cycle_state else None,
                "result_state": str(run_state.result_state) if run_state.result_state else None,
                "state_message": run_state.state_message,
            }

        # Task-level details — extract from the run object
        tasks_info: list[dict[str, Any]] = []
        run_tasks = run.tasks or []

        # If run.tasks is empty, try fetching the full run object
        if not run_tasks and run.run_id:
            try:
                full_run = client.jobs.get_run(run.run_id)
                run_tasks = full_run.tasks or []
            except Exception:
                pass

        for task in run_tasks:
            task_state = task.state
            task_info: dict[str, Any] = {
                "task_key": task.task_key,
            }
            if task_state:
                task_info["life_cycle_state"] = (
                    str(task_state.life_cycle_state) if task_state.life_cycle_state else None
                )
                task_info["result_state"] = (
                    str(task_state.result_state) if task_state.result_state else None
                )

                # Diagnose errors
                if task_state.state_message and task_state.result_state and "FAILED" in str(task_state.result_state):
                    task_info["error_message"] = task_state.state_message

                    # Try to fetch the detailed run output for this task
                    task_run_id = getattr(task, "run_id", None)
                    if task_run_id:
                        try:
                            output = client.jobs.get_run_output(task_run_id)
                            if output.error:
                                task_info["error_detail"] = output.error
                            if output.error_trace:
                                # Truncate trace to first 2000 chars
                                task_info["error_trace"] = output.error_trace[:2000]
                        except Exception:
                            pass

                    task_info["diagnosis"] = _diagnose_error(
                        task_info.get("error_detail") or task_info.get("error_message", "")
                    )

            tasks_info.append(task_info)

        # Duration
        duration_ms = None
        if run.start_time and run.end_time:
            duration_ms = run.end_time - run.start_time

        result: dict[str, Any] = {
            **job_meta,
            "run_id": str(run.run_id),
            "state": state_info,
            "duration_ms": duration_ms,
            "start_time": run.start_time,
            "end_time": run.end_time,
            "task_count": len(tasks_info),
            "tasks": tasks_info,
        }

        # Top-level diagnosis if the run failed
        error_source = ""
        if tasks_info:
            # Use the first failed task's detailed error for diagnosis
            for ti in tasks_info:
                if ti.get("error_detail") or ti.get("error_message"):
                    error_source = ti.get("error_detail") or ti.get("error_message", "")
                    break
        if not error_source and run_state and run_state.state_message:
            error_source = run_state.state_message

        if error_source and run_state and run_state.result_state and "FAILED" in str(run_state.result_state):
            result["diagnosis"] = _diagnose_error(error_source)

        return json.dumps(result, indent=2)

    @mcp.tool()
    def list_pipelines(name_filter: str = "") -> str:
        """List all DLT pipelines with state and latest update status.

        Args:
            name_filter: Optional name substring to filter pipelines.

        Returns:
            JSON list of pipelines with id, name, state, and latest update.
        """
        client = get_workspace_client()

        try:
            pipelines = list(client.pipelines.list_pipelines())
        except Exception as e:
            return json.dumps({"error": f"Failed to list pipelines: {e}"})

        results: list[dict[str, Any]] = []
        for ps in pipelines:
            name = ps.name or ""
            if name_filter and name_filter.lower() not in name.lower():
                continue

            results.append({
                "pipeline_id": ps.pipeline_id,
                "name": name,
                "state": str(ps.state) if ps.state else None,
                "creator": getattr(ps, "creator_user_name", None),
            })

        return json.dumps({
            "pipeline_count": len(results),
            "pipelines": results,
        }, indent=2)

    @mcp.tool()
    def get_pipeline_status(pipeline_id: str) -> str:
        """Get detailed status and recent events for a DLT pipeline.

        Args:
            pipeline_id: The DLT pipeline ID.

        Returns:
            JSON with pipeline state, latest update details, and error diagnostics.
        """
        client = get_workspace_client()

        try:
            pipeline = client.pipelines.get(pipeline_id)
        except Exception as e:
            return json.dumps({"error": f"Failed to get pipeline {pipeline_id}: {e}"})

        state = str(pipeline.state) if pipeline.state else None
        name = pipeline.name or ""

        # Get latest update
        latest_update: dict[str, Any] | None = None
        if pipeline.latest_updates:
            for update in pipeline.latest_updates:
                latest_update = {
                    "update_id": update.update_id,
                    "state": str(update.state) if update.state else None,
                    "creation_time": getattr(update, "creation_time", None),
                }
                break  # Just the latest

        # Get recent events (errors)
        events: list[dict[str, Any]] = []
        try:
            event_list = client.pipelines.list_pipeline_events(
                pipeline_id=pipeline_id,
            )
            for event in event_list:
                event_info: dict[str, Any] = {
                    "id": event.id,
                    "event_type": event.event_type,
                    "timestamp": getattr(event, "timestamp", None),
                    "level": str(event.level) if event.level else None,
                }
                if event.error and event.error.exceptions:
                    error_msg = "; ".join(
                        ex.message or "" for ex in event.error.exceptions if ex.message
                    )
                    event_info["error_message"] = error_msg
                    event_info["diagnosis"] = _diagnose_error(error_msg)

                events.append(event_info)
                if len(events) >= 10:
                    break
        except Exception:
            pass  # Events are optional

        result: dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "name": name,
            "state": state,
            "latest_update": latest_update,
            "recent_events": events,
        }

        return json.dumps(result, indent=2)

    @mcp.tool()
    def trigger_rerun(
        job_id: str,
        confirm: bool = False,
    ) -> str:
        """Trigger a rerun of a failed or completed job.

        This is a MUTATING operation. When confirm=False (default),
        returns a preview of what would happen. Set confirm=True to
        actually trigger the rerun.

        Args:
            job_id: The Databricks job ID to rerun.
            confirm: Set to True to actually trigger the rerun.

        Returns:
            JSON with rerun preview or confirmation.
        """
        client = get_workspace_client()

        # Get latest run
        try:
            runs = list(client.jobs.list_runs(job_id=int(job_id), limit=1))
        except Exception as e:
            return json.dumps({"error": f"Failed to get runs for job {job_id}: {e}"})

        if not runs:
            return json.dumps({
                "error": f"No runs found for job {job_id}.",
            })

        latest_run = runs[0]
        run_id = latest_run.run_id

        if not confirm:
            state_msg = ""
            if latest_run.state:
                state_msg = (
                    str(latest_run.state.result_state)
                    if latest_run.state.result_state
                    else str(latest_run.state.life_cycle_state)
                )

            return json.dumps({
                "action": "preview",
                "message": f"Would trigger rerun of job {job_id}, latest run {run_id}.",
                "latest_run_status": state_msg,
                "warning": "Set confirm=True to actually trigger the rerun.",
            }, indent=2)

        # Actually trigger the rerun
        try:
            if run_id is None:
                return json.dumps({"error": "No run ID found for latest run."})
            repair_run = client.jobs.repair_run(run_id=run_id, rerun_all_failed_tasks=True)
            return json.dumps({
                "action": "triggered",
                "job_id": job_id,
                "original_run_id": str(run_id),
                "repair_run_id": str(repair_run.repair_id) if repair_run.repair_id else None,
                "message": "Rerun triggered successfully.",
            }, indent=2)
        except Exception as e:
            # If repair fails, try run_now instead
            try:
                new_run = client.jobs.run_now(job_id=int(job_id))
                return json.dumps({
                    "action": "triggered",
                    "job_id": job_id,
                    "new_run_id": str(new_run.run_id),
                    "message": "New run triggered (repair was not possible).",
                    "repair_error": str(e),
                }, indent=2)
            except Exception as e2:
                return json.dumps({
                    "error": f"Failed to trigger rerun: {e2}",
                    "repair_error": str(e),
                })

    @mcp.tool()
    def trigger_job_run(
        job_id: str,
        job_parameters: str = "{}",
        confirm: bool = False,
    ) -> str:
        """Trigger a new run of a Databricks job (run now).

        Unlike trigger_rerun (which repairs the latest failed run), this
        tool starts a fresh new run of the job — useful for scheduled jobs,
        manual triggers, or testing with different parameters.

        This is a MUTATING operation. When confirm=False (default), returns
        a preview. Set confirm=True to actually trigger the run.

        Args:
            job_id: The Databricks job ID to run.
            job_parameters: JSON string of key-value pairs to pass as job
                parameters, e.g. '{"env": "prod", "date": "2026-03-04"}'.
                Defaults to empty dict (use job defaults).
            confirm: Set to True to actually trigger the run.

        Returns:
            JSON with run preview or the new run_id and a direct link.
        """
        client = get_workspace_client()

        # Parse job_parameters
        try:
            params: dict[str, str] = json.loads(job_parameters) if job_parameters else {}
        except Exception:
            return json.dumps({"error": f"Invalid job_parameters JSON: {job_parameters}"})

        # Fetch job metadata for the preview
        job_meta: dict[str, Any] = {"job_id": job_id}
        try:
            job_detail = client.jobs.get(int(job_id))
            if job_detail.settings:
                job_meta["name"] = job_detail.settings.name
        except Exception:
            pass

        if not confirm:
            return json.dumps({
                "action": "preview",
                "job_id": job_id,
                "job_name": job_meta.get("name"),
                "job_parameters": params,
                "message": f"Would trigger a new run of job {job_id}.",
                "warning": "Set confirm=True to actually trigger the run.",
            }, indent=2)

        # Trigger the run
        try:
            kwargs: dict[str, Any] = {}
            if params:
                kwargs["notebook_params"] = params
            new_run = client.jobs.run_now(job_id=int(job_id), **kwargs)
            run_id = new_run.run_id
            host = get_settings().databricks_host.rstrip("/")
            run_url = f"{host}/#job/{job_id}/run/{run_id}" if run_id else None
            return json.dumps({
                "action": "triggered",
                "job_id": job_id,
                "job_name": job_meta.get("name"),
                "run_id": str(run_id),
                "run_url": run_url,
                "job_parameters": params,
                "message": "New job run triggered successfully.",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to trigger job run: {e}"})
