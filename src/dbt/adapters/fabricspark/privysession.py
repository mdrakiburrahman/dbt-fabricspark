"""Privy connection method (experimental) — Azure Relay transport for Spark SQL.

Sends ``spark.sql(...)`` statements to a Fabric notebook running
``privy.RelayServer`` over an Azure Relay Hybrid Connection, instead of the
Livy REST API.

Every statement is sent with ``mode="inprocess"``: privy's default
``mode="subprocess"`` spawns a fresh, isolated Python interpreter with no
Fabric notebook context, while ``mode="inprocess"`` executes inside the
``RelayServer``'s own already-running interpreter — the same kernel the
Fabric notebook cell is running in — so the notebook's pre-existing ``spark``
session global is visible. Without ``inprocess``, ``spark`` would be
undefined.

This is a spike: no lakehouse schema-detection or high-concurrency multi-REPL
support. Concurrent dbt threads do run in parallel — privy captures
stdout/stderr per thread and dispatches inprocess calls on a thread pool, so
statements execute simultaneously against the notebook's shared ``spark``
session (setting ``PRIVY_SERIALIZE_INPROCESS=1`` forces them back to
one-at-a-time). Transient listener disconnects are retried against the same
remote job, while ambiguous submits are deduplicated by request id. A
transient failure on a poll's long-poll HTTP request is likewise ambiguous —
the job may have already finished server-side and only the relay's response
was lost — so it is first resolved with an immediate, non-blocking status
re-check of the same job instead of sleeping and starting a brand new long
poll; only a genuinely inconclusive re-check falls back to that sleep.

The notebook run is never cancelled by this module (not even on process
exit) — an interprocess-locked ownership cache under the user's standard
cache directory lets separate dbt invocations reuse the same run without
silently crossing campaign or Relay-target ownership. Cancelling it is
entirely up to the caller.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livy_backend import coerce_time_columns
from dbt.adapters.fabricspark.livysession import get_headers

try:
    import fcntl
except ImportError:  # pragma: no cover - Fabric adapter CI/runtime is POSIX
    fcntl = None  # type: ignore

logger = AdapterLogger("Microsoft Fabric-Spark")

_NUMBERS = DECIMALS + (int, float)

_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

# Fast, short-timeout probe used purely to check "is anything listening on the
# relay right now" — deliberately much shorter than the timeout used for real
# query execution below.
_PROBE_HTTP_TIMEOUT_S = 20.0
_PROBE_TIMEOUT_S = 10.0

# Fallback exec timeout when credentials.statement_timeout == 0 ("no timeout"
# for Livy's polling loop). privy's wire protocol needs a finite number, so a
# generous one week stands in for "effectively unbounded".
_UNBOUNDED_TIMEOUT_S = 7 * 24 * 3600.0

# Fabric Job Scheduler statuses for the RunNotebook job instance we trigger.
# https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/get-item-job-instance
_JOB_TERMINAL_STATUSES = {"Completed", "Failed", "Cancelled", "Deduped"}
_JOB_FAILURE_STATUSES = {"Failed", "Cancelled"}
_JOB_ACTIVE_STATUSES = {"NotStarted", "InProgress", "Queued", "Running"}
_JOB_SUBMISSION_CLOCK_SKEW = dt.timedelta(seconds=30)
_JOB_HISTORY_MAX_PAGES = 20
_JOB_RECONCILE_TIMEOUT_S = 30.0
_JOB_RECONCILE_POLL_S = 1.0
_JOB_LOCK_TIMEOUT_S = 60.0
_JOB_LOCK_POLL_S = 0.05
_FABRIC_SKILL_HEADER = "x-ms-fabric-skill"
_FABRIC_SKILL_VALUE = "spark-cli"
_PRIVY_EXECUTION_SPAN_PREFIX = "PRIVY_EXECUTION_SPAN "
_PRIVY_TIMING_NEGATIVE_CLAMP_MS = 5
_PRIVY_TRANSIENT_HTTP_STATUSES = frozenset({404, 408, 429, 500, 502, 503, 504})
_PRIVY_CONTROL_GRACE_S = 60.0
_PRIVY_POLL_BACKOFF_MIN_S = 0.25
_PRIVY_POLL_BACKOFF_MAX_S = 5.0
_PRIVY_MAX_WARM_CONNECTIONS = 64

# A 504/408/etc. on a poll's long-poll HTTP request is inherently ambiguous —
# the relay may have dropped the response to an already-finished job. Rather
# than sleeping ``connect_timeout`` seconds and starting a brand new
# ``DEFAULT_POLL_WAIT_S``-long poll, we immediately re-check the same job with
# a non-blocking (``wait_s=0``) status fetch. It is a plain read (same
# request/job id, no resubmission), so it is safe to try before falling back
# to the normal retry backoff.
_PRIVY_STATUS_PROBE_WAIT_S = 0.0

# Sentinel returned by ``_quick_status_probe`` when the immediate recheck
# could not resolve the ambiguity (job still running, or the probe itself hit
# a transient relay error) — signals ``_control_call`` to fall back to its
# normal sleep-then-retry loop.
_PROBE_INCONCLUSIVE = object()


@dataclass(frozen=True)
class _NotebookJobRef:
    workspace_id: str
    notebook_id: str
    target_id: str
    job_instance_id: str
    correlation_token: str


class PrivyTransportRetryError(DbtDatabaseError):
    pass


class PrivyNotebookSubmissionAmbiguousError(DbtRuntimeError):
    def __init__(self, evidence: Dict[str, Any]) -> None:
        self.evidence = evidence
        super().__init__(
            "Fabric notebook submission remained unattributable; POST was not retried "
            "and no job was adopted or cancelled. "
            f"ambiguity_evidence={json.dumps(evidence, sort_keys=True)}"
        )


class _PrivyOwnershipBudgetExceeded(DbtRuntimeError):
    def __init__(self, stage: str) -> None:
        self.stage = stage
        super().__init__(f"Privy notebook ownership budget expired during {stage}.")


def _import_relay_client() -> Any:
    try:
        from privy import RelayClient
    except ImportError as exc:
        raise DbtRuntimeError(
            "method=privy requires the `privy` package, which should be bundled with "
            "dbt-fabricspark. Try reinstalling dbt-fabricspark, or `pip install privy` "
            "directly if this is a stripped-down/offline environment."
        ) from exc
    return RelayClient


def _build_relay_client(credentials: FabricSparkCredentials, http_timeout_s: float) -> Any:
    RelayClient = _import_relay_client()
    return RelayClient(
        namespace=credentials.privy_relay_namespace,
        path=credentials.privy_relay_path,
        keyrule=credentials.privy_relay_keyrule,
        key=credentials.privy_relay_key,
        http_timeout_s=http_timeout_s,
    )


def _query_timeout_s(credentials: FabricSparkCredentials) -> float:
    if credentials.statement_timeout and credentials.statement_timeout > 0:
        return float(credentials.statement_timeout)
    return _UNBOUNDED_TIMEOUT_S


def _relay_error_status(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    return int(status_code) if status_code is not None else None


def _is_transient_relay_error(exc: Exception) -> bool:
    status_code = _relay_error_status(exc)
    if status_code is not None:
        return status_code in _PRIVY_TRANSIENT_HTTP_STATUSES
    return isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ),
    )


def _relay_error_label(exc: Exception) -> str:
    status_code = _relay_error_status(exc)
    return f"HTTP {status_code}" if status_code is not None else type(exc).__name__


def _parse_notebook_ids(notebook_url: Optional[str]) -> Tuple[str, str]:
    """Extract (workspaceId, notebookId) GUIDs from a Fabric notebook browser URL.

    e.g. ``https://<host>/groups/<workspaceId>/synapsenotebooks/<notebookId>``.
    """
    ids = _UUID_RE.findall(notebook_url or "")
    if len(ids) < 2:
        raise ValueError(
            f"expected to find 2 GUIDs (workspace, notebook) in privy_notebook_url, "
            f"found {len(ids)}: {notebook_url!r}"
        )
    return ids[0], ids[1]


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _job_scheduler_headers(credentials: FabricSparkCredentials) -> Dict[str, str]:
    headers = dict(get_headers(credentials))
    headers[_FABRIC_SKILL_HEADER] = _FABRIC_SKILL_VALUE
    return headers


def _job_scheduler_request(
    method: str,
    url: str,
    credentials: FabricSparkCredentials,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, str]] = None,
    timeout_s: Optional[float] = None,
) -> requests.Response:
    request_timeout = float(credentials.http_timeout)
    if timeout_s is not None:
        if timeout_s <= 0:
            raise DbtRuntimeError("Fabric Job Scheduler request budget is exhausted.")
        request_timeout = min(request_timeout, timeout_s)
    kwargs: Dict[str, Any] = {
        "headers": _job_scheduler_headers(credentials),
        "timeout": request_timeout,
    }
    if json_body is not None:
        kwargs["json"] = json_body
    if params is not None:
        kwargs["params"] = params
    return requests.request(method, url, **kwargs)


def _response_error_code(response: requests.Response) -> Optional[str]:
    try:
        payload = response.json()
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    error_code = payload.get("errorCode")
    if not error_code and isinstance(payload.get("error"), dict):
        error_code = payload["error"].get("errorCode")
    return _safe_diagnostic_code(error_code)


def _safe_diagnostic_code(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value):
        return None
    return value


def _raise_job_response_error(
    operation: str,
    response: requests.Response,
    *,
    parameter_submission: bool = False,
) -> None:
    error_code = _response_error_code(response)
    detail = f"HTTP {response.status_code}"
    if error_code:
        detail += f", errorCode={error_code}"
    if parameter_submission:
        raise DbtRuntimeError(
            f"Fabric rejected the parameterized notebook submission ({detail}); "
            "the adapter failed closed and a parameterless fallback was not attempted."
        )
    raise DbtRuntimeError(f"Fabric Job Scheduler {operation} failed ({detail}).")


def _require_job_response(
    operation: str,
    response: requests.Response,
    expected_statuses: Sequence[int],
    *,
    parameter_submission: bool = False,
) -> None:
    if response.status_code not in expected_statuses:
        _raise_job_response_error(
            operation,
            response,
            parameter_submission=parameter_submission,
        )


def _continuation_token(payload: Dict[str, Any]) -> Optional[str]:
    token = payload.get("continuationToken")
    if isinstance(token, str) and token:
        return token
    continuation_uri = payload.get("continuationUri")
    if not isinstance(continuation_uri, str) or not continuation_uri:
        return None
    values = parse_qs(urlparse(continuation_uri).query).get("continuationToken")
    return values[0] if values else None


def _list_item_job_instances(
    credentials: FabricSparkCredentials,
    workspace_id: str,
    notebook_id: str,
    *,
    timeout_s: Optional[float] = None,
) -> List[Dict[str, Any]]:
    url = f"{credentials.endpoint}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances"
    jobs: List[Dict[str, Any]] = []
    continuation_token: Optional[str] = None
    seen_tokens = set()
    deadline = time.monotonic() + timeout_s if timeout_s is not None else None
    for _ in range(_JOB_HISTORY_MAX_PAGES):
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0:
            raise DbtRuntimeError("Fabric Job Scheduler history budget is exhausted.")
        params = (
            {"continuationToken": continuation_token} if continuation_token is not None else None
        )
        response = _job_scheduler_request(
            "GET",
            url,
            credentials,
            params=params,
            timeout_s=remaining,
        )
        remaining = _remaining_budget(deadline)
        if remaining is not None and remaining <= 0:
            raise DbtRuntimeError("Fabric Job Scheduler history budget expired after a page.")
        _require_job_response("history lookup", response, (200,))
        try:
            payload = response.json()
        except (ValueError, TypeError) as exc:
            raise DbtRuntimeError(
                "Fabric Job Scheduler history lookup returned invalid JSON."
            ) from exc
        if (
            not isinstance(payload, dict)
            or "value" not in payload
            or not isinstance(payload["value"], list)
        ):
            raise DbtRuntimeError(
                "Fabric Job Scheduler history lookup returned an invalid result shape."
            )
        jobs.extend(job for job in payload.get("value", []) if isinstance(job, dict))
        continuation_token = _continuation_token(payload)
        if continuation_token is None:
            return jobs
        if continuation_token in seen_tokens:
            raise DbtRuntimeError(
                "Fabric Job Scheduler history pagination repeated a continuation token."
            )
        seen_tokens.add(continuation_token)
    raise DbtRuntimeError(f"Fabric Job Scheduler history exceeded {_JOB_HISTORY_MAX_PAGES} pages.")


def _remaining_budget(deadline: Optional[float]) -> Optional[float]:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _job_started_at(job: Dict[str, Any]) -> Optional[dt.datetime]:
    value = job.get("startTimeUtc")
    if not isinstance(value, str):
        return None
    try:
        parsed = _parse_utc_iso(value)
    except ValueError:
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _matching_job_instances(
    jobs: Sequence[Dict[str, Any]],
    *,
    notebook_id: str,
    statuses: Optional[Sequence[str]] = None,
    not_before: Optional[dt.datetime] = None,
    not_after: Optional[dt.datetime] = None,
    excluded_job_ids: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    allowed_statuses = set(statuses) if statuses is not None else None
    excluded_ids = set(excluded_job_ids or ())
    matches: Dict[str, Dict[str, Any]] = {}
    for job in jobs:
        if job.get("itemId") != notebook_id:
            continue
        if job.get("jobType") != "RunNotebook":
            continue
        if allowed_statuses is not None and job.get("status") not in allowed_statuses:
            continue
        job_instance_id = job.get("id")
        if not isinstance(job_instance_id, str) or _UUID_RE.fullmatch(job_instance_id) is None:
            continue
        if job_instance_id in excluded_ids:
            continue
        if not_before is not None or not_after is not None:
            started_at = _job_started_at(job)
            if started_at is None:
                continue
            if not_before is not None and started_at < not_before:
                continue
            if not_after is not None and started_at > not_after:
                continue
        matches[job_instance_id] = job
    return list(matches.values())


def _job_ref_from_instance(
    job: Dict[str, Any],
    *,
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    correlation_token: str,
) -> _NotebookJobRef:
    return _NotebookJobRef(
        workspace_id=workspace_id,
        notebook_id=notebook_id,
        target_id=target_id,
        job_instance_id=str(job["id"]),
        correlation_token=correlation_token,
    )


def _resolve_job_correlation_token(
    credentials: FabricSparkCredentials,
    workspace_id: str,
    notebook_id: str,
) -> str:
    if credentials.privy_campaign_correlation_token:
        return credentials.privy_campaign_correlation_token
    target = ":".join(
        (
            workspace_id,
            notebook_id,
            credentials.privy_relay_namespace or "",
            credentials.privy_relay_path or "",
        )
    )
    digest = hashlib.sha256(target.encode("utf-8")).hexdigest()[:24]
    return f"dbt-fabricspark-{digest}"


def _job_target_id(credentials: FabricSparkCredentials) -> str:
    target = "\0".join(
        (
            credentials.privy_relay_namespace or "",
            credentials.privy_relay_path or "",
        )
    )
    return hashlib.sha256(target.encode("utf-8")).hexdigest()


def _notebook_run_body(
    credentials: FabricSparkCredentials,
    *,
    correlation_token: str,
) -> Dict[str, Any]:
    return {
        "parameters": [
            {
                "name": "relay_namespace",
                "value": credentials.privy_relay_namespace,
                "type": "Text",
            },
            {
                "name": "relay_path",
                "value": credentials.privy_relay_path,
                "type": "Text",
            },
            {
                "name": "relay_key_rule",
                "value": credentials.privy_relay_keyrule,
                "type": "Text",
            },
            {
                "name": "relay_key",
                "value": credentials.privy_relay_key,
                "type": "Text",
            },
            {
                "name": "max_workers",
                "value": credentials.privy_max_workers,
                "type": "Integer",
            },
            {
                "name": "listener_connections",
                "value": credentials.privy_listener_connections,
                "type": "Integer",
            },
            {
                "name": "serialize_inprocess",
                "value": credentials.privy_serialize_inprocess,
                "type": "Boolean",
            },
            {
                "name": "campaign_correlation_token",
                "value": correlation_token,
                "type": "Text",
            },
        ]
    }


def _job_instance_id_from_response(response: requests.Response) -> Optional[str]:
    location = response.headers.get("Location", "")
    if isinstance(location, str):
        path_parts = [part for part in urlparse(location).path.split("/") if part]
        if (
            len(path_parts) >= 3
            and path_parts[-3:-1] == ["jobs", "instances"]
            and _UUID_RE.fullmatch(path_parts[-1])
        ):
            return path_parts[-1]
    return None


def _response_request_id(response: Optional[requests.Response]) -> Optional[str]:
    if response is None:
        return None
    for name, value in response.headers.items():
        if name.casefold() in {"x-ms-request-id", "request-id"}:
            return _safe_uuid(value)
    return None


def _safe_uuid(value: Any) -> Optional[str]:
    if not isinstance(value, str) or _UUID_RE.fullmatch(value) is None:
        return None
    return value


def _job_ambiguity_evidence(job: Dict[str, Any]) -> Dict[str, Any]:
    started_at = _job_started_at(job)
    return {
        "job_instance_id": _safe_uuid(job.get("id")),
        "root_activity_id": _safe_uuid(job.get("rootActivityId")),
        "status": _safe_diagnostic_code(job.get("status")),
        "start_time_utc": started_at.isoformat() if started_at is not None else None,
    }


def _request_exception_label(exc: BaseException) -> str:
    return type(exc).__name__


def _trigger_notebook_run(
    credentials: FabricSparkCredentials,
) -> _NotebookJobRef:
    """Reuse the owned singleton run or submit one parameterized notebook job."""
    try:
        workspace_id, notebook_id = _parse_notebook_ids(credentials.privy_notebook_url)
    except ValueError as exc:
        raise DbtRuntimeError(
            "Could not parse workspace/notebook IDs from privy_notebook_url; "
            "refusing to auto-start without an exact Job Scheduler target."
        ) from exc

    correlation_token = _resolve_job_correlation_token(
        credentials,
        workspace_id,
        notebook_id,
    )
    target_id = _job_target_id(credentials)
    ownership_deadline = time.monotonic() + _JOB_LOCK_TIMEOUT_S
    with _notebook_scope_lock(workspace_id, notebook_id, deadline=ownership_deadline):
        with _notebook_job_lock(
            workspace_id,
            notebook_id,
            target_id,
            deadline=ownership_deadline,
        ):
            return _trigger_notebook_run_locked(
                credentials,
                workspace_id,
                notebook_id,
                target_id,
                correlation_token,
                ownership_deadline,
            )


def _trigger_notebook_run_locked(
    credentials: FabricSparkCredentials,
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    correlation_token: str,
    ownership_deadline: float,
) -> _NotebookJobRef:
    cached_ref = _read_cached_job_ref(
        workspace_id,
        notebook_id,
        target_id,
        deadline=ownership_deadline,
    )
    try:
        history = _list_item_job_instances(credentials, workspace_id, notebook_id)
    except requests.exceptions.RequestException as exc:
        raise DbtRuntimeError(
            "Could not inspect Fabric notebook job history "
            f"({_request_exception_label(exc)}); refusing to submit without duplicate checks."
        ) from None

    owned_active_ref = _owned_active_job_ref(
        history,
        cached_ref=cached_ref,
        workspace_id=workspace_id,
        notebook_id=notebook_id,
        target_id=target_id,
        correlation_token=correlation_token,
    )
    if owned_active_ref is not None:
        logger.info(
            f"Reusing owned active Fabric notebook job {owned_active_ref.job_instance_id}."
        )
        return owned_active_ref

    if cached_ref is not None:
        _clear_cached_job_ref(cached_ref, deadline=ownership_deadline)

    url = (
        f"{credentials.endpoint}/workspaces/{workspace_id}/items/{notebook_id}"
        f"/jobs/execute/instances?beta=false"
    )
    logger.info(f"Privy relay not responding; triggering Fabric notebook run: POST {url}")
    body = _notebook_run_body(credentials, correlation_token=correlation_token)
    prior_job_ids = {str(job["id"]) for job in history if isinstance(job.get("id"), str)}
    submission_started_at = _utc_now()
    response: Optional[requests.Response] = None
    ambiguous_error: Optional[str] = None
    try:
        response = _job_scheduler_request(
            "POST",
            url,
            credentials,
            json_body=body,
        )
    except requests.exceptions.RequestException as exc:
        ambiguous_error = _request_exception_label(exc)
        exception_response = getattr(exc, "response", None)
        if isinstance(exception_response, requests.Response):
            response = exception_response

    if ambiguous_error is not None:
        logger.warning(
            "Fabric notebook submission outcome is ambiguous "
            f"({ambiguous_error}); reconciling through job history without retrying POST."
        )
        return _reconcile_ambiguous_notebook_submission(
            credentials,
            workspace_id,
            notebook_id,
            target_id,
            correlation_token,
            submission_started_at,
            prior_job_ids,
            ambiguity_kind=ambiguous_error,
            response=response,
            ownership_deadline=ownership_deadline,
        )

    if response is None:  # pragma: no cover - defensive
        raise DbtRuntimeError("Fabric notebook submission produced no response.")
    if response.status_code == 408 or 500 <= response.status_code <= 599:
        logger.warning(
            "Fabric notebook submission returned an ambiguous "
            f"HTTP {response.status_code}; reconciling through job history without retrying POST."
        )
        return _reconcile_ambiguous_notebook_submission(
            credentials,
            workspace_id,
            notebook_id,
            target_id,
            correlation_token,
            submission_started_at,
            prior_job_ids,
            ambiguity_kind=f"HTTP {response.status_code}",
            response=response,
            ownership_deadline=ownership_deadline,
        )
    _require_job_response(
        "parameterized notebook submission", response, (200, 202), parameter_submission=True
    )
    job_instance_id = _job_instance_id_from_response(response)
    if job_instance_id is None:
        logger.warning(
            "Fabric accepted the parameterized notebook submission without a usable "
            "job ID; reconciling through job history without retrying POST."
        )
        return _reconcile_ambiguous_notebook_submission(
            credentials,
            workspace_id,
            notebook_id,
            target_id,
            correlation_token,
            submission_started_at,
            prior_job_ids,
            ambiguity_kind="accepted_without_job_id",
            response=response,
            ownership_deadline=ownership_deadline,
        )

    job_ref = _NotebookJobRef(
        workspace_id=workspace_id,
        notebook_id=notebook_id,
        target_id=target_id,
        job_instance_id=job_instance_id,
        correlation_token=correlation_token,
    )
    _cache_job_ref(job_ref, deadline=ownership_deadline)
    logger.info(
        f"Notebook run triggered (HTTP {response.status_code}). "
        f"Job instance: {job_instance_id}. Waiting for the Privy relay to come up..."
    )
    return job_ref


def _owned_active_job_ref(
    history: Sequence[Dict[str, Any]],
    *,
    cached_ref: Optional[_NotebookJobRef],
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    correlation_token: str,
) -> Optional[_NotebookJobRef]:
    if cached_ref is not None and (
        cached_ref.workspace_id != workspace_id
        or cached_ref.notebook_id != notebook_id
        or cached_ref.target_id != target_id
    ):
        raise DbtRuntimeError("Privy notebook job ownership cache is scoped to the wrong target.")
    active_jobs = _matching_job_instances(
        history,
        notebook_id=notebook_id,
        statuses=tuple(_JOB_ACTIVE_STATUSES),
    )
    if len(active_jobs) > 1:
        raise DbtRuntimeError(
            "Fabric notebook auto-start found multiple active RunNotebook jobs for the "
            "notebook; singleton ownership is ambiguous, so no job was reused or submitted."
        )
    if len(active_jobs) == 1:
        active_job_id = str(active_jobs[0]["id"])
        if cached_ref is None:
            raise DbtRuntimeError(
                "Fabric notebook auto-start found one active RunNotebook job but no local "
                "ownership record. Refusing to adopt an uncorrelated campaign job."
            )
        if cached_ref.job_instance_id != active_job_id:
            raise DbtRuntimeError(
                "Fabric notebook auto-start found an active RunNotebook job that does not "
                "match the locally owned job ID. Refusing to adopt another campaign's job."
            )
        if cached_ref.correlation_token != correlation_token:
            raise DbtRuntimeError(
                "Fabric notebook auto-start found an active RunNotebook job owned by a "
                "different campaign token. Notebook-wide single-active-run semantics "
                "forbid concurrent campaigns on the same target."
            )
        return cached_ref
    return None


def _reconcile_ambiguous_notebook_submission(
    credentials: FabricSparkCredentials,
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    correlation_token: str,
    submission_started_at: dt.datetime,
    prior_job_ids: Sequence[str],
    *,
    ambiguity_kind: str,
    response: Optional[requests.Response],
    ownership_deadline: float,
) -> _NotebookJobRef:
    # Fabric documents Location as the POST-to-job link. Job history's
    # rootActivityId is server-generated but is not documented as matching any
    # POST response/request header, so timing-only history entries are evidence,
    # never attribution.
    started_tick = time.monotonic()
    deadline = min(
        ownership_deadline,
        started_tick + _JOB_RECONCILE_TIMEOUT_S,
    )
    effective_budget_seconds = max(0.0, deadline - started_tick)
    location_job_instance_id = (
        _job_instance_id_from_response(response) if response is not None else None
    )
    evidence: Dict[str, Any] = {
        "schema_version": 1,
        "ambiguity_kind": ambiguity_kind,
        "workspace_id": workspace_id,
        "notebook_id": notebook_id,
        "target_id": target_id,
        "campaign_correlation_token": correlation_token,
        "submission_started_at": submission_started_at.isoformat(),
        "budget_seconds": effective_budget_seconds,
        "server_request_id": _response_request_id(response),
        "location_job_instance_id": location_job_instance_id,
        "post_retried": False,
        "cancel_attempted": False,
    }
    if location_job_instance_id is not None:
        return _reconcile_location_job_instance(
            credentials,
            workspace_id,
            notebook_id,
            target_id,
            correlation_token,
            location_job_instance_id,
            deadline,
            evidence,
        )

    last_error: Optional[str] = None
    observed_jobs: Dict[str, Dict[str, Any]] = {}
    while True:
        remaining = _remaining_budget(deadline)
        if remaining is None or remaining <= 0:
            break
        try:
            history = _list_item_job_instances(
                credentials,
                workspace_id,
                notebook_id,
                timeout_s=remaining,
            )
        except (requests.exceptions.RequestException, DbtRuntimeError) as exc:
            last_error = _request_exception_label(exc)
        else:
            matches = _matching_job_instances(
                history,
                notebook_id=notebook_id,
                not_before=submission_started_at - _JOB_SUBMISSION_CLOCK_SKEW,
                not_after=_utc_now() + _JOB_SUBMISSION_CLOCK_SKEW,
                excluded_job_ids=prior_job_ids,
            )
            for job in matches:
                job_id = _safe_uuid(job.get("id"))
                if job_id is not None:
                    observed_jobs[job_id] = _job_ambiguity_evidence(job)
            last_error = None

        remaining = _remaining_budget(deadline)
        if remaining is None or remaining <= 0:
            break
        time.sleep(min(_JOB_RECONCILE_POLL_S, remaining))

    evidence["elapsed_seconds"] = max(0.0, time.monotonic() - started_tick)
    evidence["last_history_error"] = last_error
    evidence["observed_new_jobs"] = list(observed_jobs.values())
    raise PrivyNotebookSubmissionAmbiguousError(evidence) from None


def _reconcile_location_job_instance(
    credentials: FabricSparkCredentials,
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    correlation_token: str,
    job_instance_id: str,
    deadline: float,
    evidence: Dict[str, Any],
) -> _NotebookJobRef:
    last_error: Optional[str] = None
    while True:
        remaining = _remaining_budget(deadline)
        if remaining is None or remaining <= 0:
            evidence["last_detail_error"] = last_error
            evidence["elapsed_seconds"] = evidence["budget_seconds"]
            raise PrivyNotebookSubmissionAmbiguousError(evidence) from None
        try:
            job = _get_job_instance_status(
                credentials,
                workspace_id,
                notebook_id,
                job_instance_id,
                timeout_s=remaining,
            )
        except (requests.exceptions.RequestException, DbtRuntimeError) as exc:
            last_error = _request_exception_label(exc)
        else:
            detail_evidence = _job_ambiguity_evidence(job)
            evidence["location_job_detail"] = detail_evidence
            remaining_after_detail = _remaining_budget(deadline)
            if remaining_after_detail is None or remaining_after_detail <= 0:
                evidence["location_validation"] = "detail_arrived_after_deadline"
                evidence["elapsed_seconds"] = evidence["budget_seconds"]
                raise PrivyNotebookSubmissionAmbiguousError(evidence) from None
            if (
                job.get("id") != job_instance_id
                or job.get("itemId") != notebook_id
                or job.get("jobType") != "RunNotebook"
            ):
                evidence["location_validation"] = "mismatch"
                evidence["elapsed_seconds"] = max(
                    0.0,
                    evidence["budget_seconds"] - (_remaining_budget(deadline) or 0.0),
                )
                raise PrivyNotebookSubmissionAmbiguousError(evidence) from None
            job_ref = _job_ref_from_instance(
                job,
                workspace_id=workspace_id,
                notebook_id=notebook_id,
                target_id=target_id,
                correlation_token=correlation_token,
            )
            try:
                _cache_job_ref(job_ref, deadline=deadline)
            except _PrivyOwnershipBudgetExceeded as exc:
                evidence["cache_persistence"] = "budget_exhausted"
                evidence["cache_budget_stage"] = exc.stage
                evidence["elapsed_seconds"] = evidence["budget_seconds"]
                raise PrivyNotebookSubmissionAmbiguousError(evidence) from None
            logger.info(
                "Reconciled the ambiguous Fabric notebook submission through its "
                f"server-returned Location job ID {job_ref.job_instance_id}."
            )
            return job_ref

        remaining = _remaining_budget(deadline)
        if remaining is None or remaining <= 0:
            continue
        time.sleep(min(_JOB_RECONCILE_POLL_S, remaining))


def _get_job_instance_status(
    credentials: FabricSparkCredentials,
    workspace_id: str,
    item_id: str,
    job_instance_id: str,
    *,
    timeout_s: Optional[float] = None,
) -> Dict[str, Any]:
    """GET .../items/{itemId}/jobs/instances/{jobInstanceId} — the run's live status."""
    url = (
        f"{credentials.endpoint}/workspaces/{workspace_id}/items/{item_id}"
        f"/jobs/instances/{job_instance_id}"
    )
    response = _job_scheduler_request("GET", url, credentials, timeout_s=timeout_s)
    _require_job_response("status poll", response, (200,))
    try:
        payload = response.json()
    except (ValueError, TypeError) as exc:
        raise DbtRuntimeError("Fabric Job Scheduler status poll returned invalid JSON.") from exc
    if not isinstance(payload, dict):
        raise DbtRuntimeError("Fabric Job Scheduler status poll returned an invalid result shape.")
    return payload


# Process and filesystem cache of the last correlated notebook job this machine
# triggered or reconciled. Only nonsecret IDs and the nonsecret correlation
# token are persisted; notebook parameter bodies and Relay credentials are not.
_JOB_CACHE_FILENAME = "privy-notebook-job.json"
_JOB_CACHE_VERSION = 2
_job_refs_by_target: Dict[str, _NotebookJobRef] = {}
_job_thread_locks: Dict[str, threading.Lock] = {}
_job_thread_locks_guard = threading.Lock()


def _job_cache_path() -> str:
    cache_root = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"),
        ".cache",
    )
    cache_dir = os.path.join(cache_root, "dbt-fabricspark")
    os.makedirs(cache_dir, mode=0o700, exist_ok=True)
    return os.path.join(cache_dir, _JOB_CACHE_FILENAME)


def _job_ref_cache_key(workspace_id: str, notebook_id: str, target_id: str) -> str:
    raw_key = "\0".join((workspace_id, notebook_id, target_id))
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def _thread_lock_for(path: str) -> threading.Lock:
    with _job_thread_locks_guard:
        lock = _job_thread_locks.get(path)
        if lock is None:
            lock = threading.Lock()
            _job_thread_locks[path] = lock
        return lock


@contextmanager
def _interprocess_file_lock(
    path: str,
    *,
    deadline: Optional[float] = None,
) -> Iterator[None]:
    if fcntl is None:
        raise DbtRuntimeError(
            "Privy notebook auto-start requires POSIX file locking; "
            "set privy_auto_start_notebook: false on unsupported platforms."
        )
    effective_deadline = (
        deadline if deadline is not None else time.monotonic() + _JOB_LOCK_TIMEOUT_S
    )
    thread_lock = _thread_lock_for(path)
    remaining = _require_remaining_budget(effective_deadline, "thread lock acquisition")
    if not thread_lock.acquire(timeout=remaining):
        raise _PrivyOwnershipBudgetExceeded("thread lock acquisition")
    lock_file = None
    file_locked = False
    try:
        _require_remaining_budget(effective_deadline, "file lock open")
        lock_file = open(path, "a+")
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                file_locked = True
                break
            except BlockingIOError:
                remaining = _require_remaining_budget(
                    effective_deadline,
                    "interprocess lock acquisition",
                )
                time.sleep(min(_JOB_LOCK_POLL_S, remaining))
        yield
    finally:
        try:
            if lock_file is not None and file_locked:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            if lock_file is not None:
                lock_file.close()
            thread_lock.release()


def _require_remaining_budget(deadline: float, stage: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _PrivyOwnershipBudgetExceeded(stage)
    return remaining


@contextmanager
def _notebook_scope_lock(
    workspace_id: str,
    notebook_id: str,
    *,
    deadline: Optional[float] = None,
) -> Iterator[None]:
    raw_key = "\0".join((workspace_id, notebook_id))
    key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    with _interprocess_file_lock(
        f"{_job_cache_path()}.{key}.notebook.lock",
        deadline=deadline,
    ):
        yield


@contextmanager
def _notebook_job_lock(
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    *,
    deadline: Optional[float] = None,
) -> Iterator[None]:
    key = _job_ref_cache_key(workspace_id, notebook_id, target_id)
    with _interprocess_file_lock(
        f"{_job_cache_path()}.{key}.target.lock",
        deadline=deadline,
    ):
        yield


@contextmanager
def _job_cache_mapping_lock(*, deadline: Optional[float] = None) -> Iterator[None]:
    with _interprocess_file_lock(
        f"{_job_cache_path()}.mapping.lock",
        deadline=deadline,
    ):
        yield


def _empty_job_ref_mapping() -> Dict[str, Any]:
    return {"version": _JOB_CACHE_VERSION, "entries": {}}


def _load_job_ref_mapping_unlocked() -> Dict[str, Any]:
    try:
        with open(_job_cache_path()) as f:
            data = json.load(f)
    except FileNotFoundError:
        return _empty_job_ref_mapping()
    except (OSError, ValueError, TypeError) as exc:
        raise DbtRuntimeError(
            "Privy notebook job ownership cache is unreadable; refusing auto-start."
        ) from exc

    if isinstance(data, dict) and {
        "workspace_id",
        "notebook_id",
        "job_instance_id",
    }.issubset(data):
        return _empty_job_ref_mapping()
    if (
        not isinstance(data, dict)
        or data.get("version") != _JOB_CACHE_VERSION
        or not isinstance(data.get("entries"), dict)
    ):
        raise DbtRuntimeError(
            "Privy notebook job ownership cache has an unsupported shape; refusing auto-start."
        )
    return data


def _write_job_ref_mapping_unlocked(
    mapping: Dict[str, Any],
    *,
    deadline: Optional[float] = None,
) -> None:
    path = _job_cache_path()
    temp_path = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        _check_optional_budget(deadline, "cache file create")
        fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            _check_optional_budget(deadline, "cache write")
            json.dump(mapping, f, sort_keys=True)
            f.flush()
            _check_optional_budget(deadline, "cache fsync")
            os.fsync(f.fileno())
        _check_optional_budget(deadline, "cache replace")
        os.replace(temp_path, path)
    except _PrivyOwnershipBudgetExceeded:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise
    except OSError as exc:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise DbtRuntimeError(
            "Could not atomically persist Privy notebook job ownership; "
            "refusing to continue without an interprocess-safe cache."
        ) from exc


def _check_optional_budget(deadline: Optional[float], stage: str) -> None:
    if deadline is not None:
        _require_remaining_budget(deadline, stage)


def _job_ref_from_cache_entry(
    entry: Any,
    *,
    workspace_id: str,
    notebook_id: str,
    target_id: str,
) -> _NotebookJobRef:
    if not isinstance(entry, dict):
        raise DbtRuntimeError("Privy notebook job ownership entry is invalid.")
    try:
        job_ref = _NotebookJobRef(
            workspace_id=str(entry["workspace_id"]),
            notebook_id=str(entry["notebook_id"]),
            target_id=str(entry["target_id"]),
            job_instance_id=str(entry["job_instance_id"]),
            correlation_token=str(entry["correlation_token"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DbtRuntimeError("Privy notebook job ownership entry is incomplete.") from exc
    if (
        job_ref.workspace_id != workspace_id
        or job_ref.notebook_id != notebook_id
        or job_ref.target_id != target_id
        or _UUID_RE.fullmatch(job_ref.job_instance_id) is None
        or not job_ref.correlation_token
    ):
        raise DbtRuntimeError("Privy notebook job ownership entry failed validation.")
    return job_ref


def _read_cached_job_ref(
    workspace_id: str,
    notebook_id: str,
    target_id: str,
    *,
    deadline: Optional[float] = None,
) -> Optional[_NotebookJobRef]:
    key = _job_ref_cache_key(workspace_id, notebook_id, target_id)
    with _job_cache_mapping_lock(deadline=deadline):
        _check_optional_budget(deadline, "cache read")
        mapping = _load_job_ref_mapping_unlocked()
        _check_optional_budget(deadline, "cache read completion")
        entry = mapping["entries"].get(key)
    if entry is None:
        _job_refs_by_target.pop(key, None)
        return None
    job_ref = _job_ref_from_cache_entry(
        entry,
        workspace_id=workspace_id,
        notebook_id=notebook_id,
        target_id=target_id,
    )
    _job_refs_by_target[key] = job_ref
    return job_ref


def _cache_job_ref(
    job_ref: _NotebookJobRef,
    *,
    deadline: Optional[float] = None,
) -> None:
    key = _job_ref_cache_key(
        job_ref.workspace_id,
        job_ref.notebook_id,
        job_ref.target_id,
    )
    entry = {
        "workspace_id": job_ref.workspace_id,
        "notebook_id": job_ref.notebook_id,
        "target_id": job_ref.target_id,
        "job_instance_id": job_ref.job_instance_id,
        "correlation_token": job_ref.correlation_token,
    }
    _check_optional_budget(deadline, "cache mapping lock")
    with _job_cache_mapping_lock(deadline=deadline):
        _check_optional_budget(deadline, "cache mapping read")
        mapping = _load_job_ref_mapping_unlocked()
        mapping["entries"][key] = entry
        _write_job_ref_mapping_unlocked(mapping, deadline=deadline)
    _job_refs_by_target[key] = job_ref


def _clear_cached_job_ref(
    job_ref: _NotebookJobRef,
    *,
    deadline: Optional[float] = None,
) -> None:
    key = _job_ref_cache_key(
        job_ref.workspace_id,
        job_ref.notebook_id,
        job_ref.target_id,
    )
    with _job_cache_mapping_lock(deadline=deadline):
        _check_optional_budget(deadline, "cache clear read")
        mapping = _load_job_ref_mapping_unlocked()
        current = mapping["entries"].get(key)
        if isinstance(current, dict) and (
            current.get("job_instance_id") == job_ref.job_instance_id
            and current.get("correlation_token") == job_ref.correlation_token
        ):
            del mapping["entries"][key]
            _write_job_ref_mapping_unlocked(mapping, deadline=deadline)
    cached = _job_refs_by_target.get(key)
    if cached == job_ref:
        _job_refs_by_target.pop(key, None)


def _probe(client: Any) -> bool:
    try:
        result = client.run_python("1", mode="inprocess", timeout_s=_PROBE_TIMEOUT_S)
        return bool(result.ok)
    except Exception as exc:  # noqa: BLE001 — any failure just means "not ready yet"
        logger.debug(f"Privy relay probe failed ({_relay_error_label(exc)}).")
        return False


def _wait_for_relay(
    probe_client: Any,
    credentials: FabricSparkCredentials,
    job_ref: Optional[_NotebookJobRef] = None,
) -> None:
    deadline = time.time() + credentials.privy_ready_timeout
    attempt = 0
    while True:
        attempt += 1
        if _probe(probe_client):
            logger.info(f"Privy relay responded after {attempt} attempt(s).")
            return

        # Job status is a much stronger signal than another silent relay
        # probe: it tells us whether the notebook run is still starting up
        # (queued/in-progress — normal, keep waiting), or has already ended
        # (failed/cancelled/completed without ever starting the relay — no
        # amount of extra waiting will help, fail fast instead).
        if job_ref is not None:
            job_instance_id = job_ref.job_instance_id
            try:
                job = _get_job_instance_status(
                    credentials,
                    job_ref.workspace_id,
                    job_ref.notebook_id,
                    job_instance_id,
                )
            except (requests.exceptions.RequestException, DbtRuntimeError) as exc:
                logger.debug(
                    "Could not fetch notebook job status "
                    f"({_request_exception_label(exc)}); continuing the bounded readiness wait."
                )
            else:
                status = job.get("status", "Unknown")
                logger.info(f"Notebook job {job_instance_id} status: {status}")
                if status in _JOB_FAILURE_STATUSES:
                    failure_reason = job.get("failureReason")
                    error_code = None
                    if isinstance(failure_reason, dict):
                        error_code = _safe_diagnostic_code(failure_reason.get("errorCode"))
                    error_detail = f" errorCode={error_code}." if error_code else ""
                    _clear_cached_job_ref(job_ref)
                    raise DbtRuntimeError(
                        f"Fabric notebook run {status} before the Privy relay came up "
                        f"(job {job_instance_id}).{error_detail} "
                        f"Check the notebook run history in the Fabric portal for details."
                    )
                if status in _JOB_TERMINAL_STATUSES:
                    # e.g. "Completed"/"Deduped" — the run this job represents
                    # is over (or was superseded), yet the relay never came up.
                    # Stop polling this particular job instance (it won't
                    # change anymore) but keep waiting on the relay itself in
                    # case another run is what's actually serving it.
                    logger.warning(
                        f"Notebook job {job_instance_id} reached status={status} but the "
                        f"Privy relay never responded. If this persists, verify the notebook "
                        f"cell actually reaches `RelayServer(...).serve_forever()`."
                    )
                    _clear_cached_job_ref(job_ref)
                    job_ref = None

        if time.time() >= deadline:
            raise DbtRuntimeError(
                f"Timed out after {credentials.privy_ready_timeout}s waiting for the "
                f"Privy relay/notebook to respond. Check the Fabric notebook run history, "
                f"or set `privy_auto_start_notebook: false` and start the notebook manually. "
                f"Override the timeout with `privy_ready_timeout: <seconds>` in your profile."
            )
        time.sleep(credentials.poll_wait)


def _validate_responding_relay_ownership(
    credentials: FabricSparkCredentials,
) -> _NotebookJobRef:
    try:
        workspace_id, notebook_id = _parse_notebook_ids(credentials.privy_notebook_url)
    except ValueError as exc:
        raise DbtRuntimeError(
            "Could not parse workspace/notebook IDs from privy_notebook_url; "
            "refusing to reuse an unowned relay target."
        ) from exc
    correlation_token = _resolve_job_correlation_token(
        credentials,
        workspace_id,
        notebook_id,
    )
    target_id = _job_target_id(credentials)
    ownership_deadline = time.monotonic() + _JOB_LOCK_TIMEOUT_S
    with _notebook_scope_lock(workspace_id, notebook_id, deadline=ownership_deadline):
        with _notebook_job_lock(
            workspace_id,
            notebook_id,
            target_id,
            deadline=ownership_deadline,
        ):
            cached_ref = _read_cached_job_ref(
                workspace_id,
                notebook_id,
                target_id,
                deadline=ownership_deadline,
            )
            try:
                history = _list_item_job_instances(credentials, workspace_id, notebook_id)
            except requests.exceptions.RequestException as exc:
                raise DbtRuntimeError(
                    "Could not verify ownership of the responding Privy relay "
                    f"({_request_exception_label(exc)})."
                ) from None
            owned_ref = _owned_active_job_ref(
                history,
                cached_ref=cached_ref,
                workspace_id=workspace_id,
                notebook_id=notebook_id,
                target_id=target_id,
                correlation_token=correlation_token,
            )
            if owned_ref is None:
                raise DbtRuntimeError(
                    "The Privy relay is responding, but Fabric history has no active "
                    "adapter-owned RunNotebook job. Set privy_auto_start_notebook: false "
                    "for a manually managed listener."
                )
            return owned_ref


def _ensure_notebook_ready(exec_client: Any, credentials: FabricSparkCredentials) -> None:
    """Probe the relay and, if needed, reuse-or-trigger a notebook run.

    Auto-start enforces one adapter-owned active job per notebook and Relay
    target, then submits at most once under an interprocess lock. Nothing is
    ever cancelled here or on process exit; cancelling the notebook run is
    entirely up to the caller.
    """
    probe_client = _build_relay_client(credentials, http_timeout_s=_PROBE_HTTP_TIMEOUT_S)
    if _probe(probe_client):
        if credentials.privy_auto_start_notebook:
            _validate_responding_relay_ownership(credentials)
        logger.debug("Privy relay already responding; reusing the existing notebook run.")
        _warm_exec_client(exec_client, credentials)
        return

    job_ref: Optional[_NotebookJobRef] = None
    if credentials.privy_auto_start_notebook:
        job_ref = _trigger_notebook_run(credentials)
    else:
        logger.info(
            "Privy relay not responding and privy_auto_start_notebook is False; "
            "waiting for it to be started manually."
        )

    _wait_for_relay(probe_client, credentials, job_ref)
    _warm_exec_client(exec_client, credentials)


def _warm_exec_client(exec_client: Any, credentials: FabricSparkCredentials) -> None:
    warmup = getattr(exec_client, "warmup", None)
    if not callable(warmup):
        return
    connection_count = min(
        _PRIVY_MAX_WARM_CONNECTIONS,
        max(1, int(credentials.privy_max_workers) // 2),
    )
    warmed = warmup(connection_count, timeout_s=_PROBE_HTTP_TIMEOUT_S)
    logger.info(f"Privy relay prewarmed {warmed} HTTPS connection(s).")


class PrivyConnectionManager:
    """Builds, health-checks and (if needed) triggers the Fabric notebook for a
    shared, process-wide Privy ``RelayClient``.

    Unlike Livy, Privy calls are stateless HTTP POSTs — there is no session or
    REPL to acquire per dbt thread, so a single client per unique
    (namespace, path) target is shared across all threads. A per-key lock
    ensures only the first caller for a given target does the
    health-check/auto-start dance; later callers (including other dbt
    threads) reuse the already-verified client.
    """

    _clients: Dict[str, Any] = {}
    _ready: Dict[str, bool] = {}
    _locks: Dict[str, threading.Lock] = {}
    _registry_lock = threading.Lock()

    @classmethod
    def _lock_for(cls, key: str) -> threading.Lock:
        with cls._registry_lock:
            lock = cls._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                cls._locks[key] = lock
            return lock

    @classmethod
    def connect(cls, credentials: FabricSparkCredentials) -> Any:
        key = credentials.unique_field
        lock = cls._lock_for(key)
        with lock:
            client = cls._clients.get(key)
            if client is None:
                http_timeout_s = _query_timeout_s(credentials) + 30.0
                client = _build_relay_client(credentials, http_timeout_s=http_timeout_s)
                cls._clients[key] = client
            if not cls._ready.get(key):
                _ensure_notebook_ready(client, credentials)
                cls._ready[key] = True
        return client

    @classmethod
    def disconnect(cls) -> None:
        """No persistent resources to release — Privy calls are stateless HTTP.

        The notebook run itself is never cancelled here or on process exit —
        the filesystem job cache lets later, separate dbt invocations keep
        reusing it. Cancel it yourself when you're done (Fabric portal, or
        POST .../jobs/instances/{id}/cancel).
        """


_NODE_ID_RE = re.compile(r'"node_id"\s*:\s*"([^"]+)"')
_REQUEST_SCOPE_RE = re.compile(r"\W+")


def _node_id_for(sql: str) -> Optional[str]:
    match = _NODE_ID_RE.search(sql[:1024])
    return match.group(1) if match else None


def _job_group_for(sql: str) -> str:
    """Derive a Spark job-group id from dbt's query comment."""
    return _node_id_for(sql) or "dbt"


def _request_scope_for(request_id: str) -> str:
    scope = _REQUEST_SCOPE_RE.sub("_", request_id)
    if not scope:
        return "_"
    return f"_{scope}" if scope[0].isdigit() else scope


def _scheduler_pool_for_request_id(request_id: str) -> str:
    return f"privy_{_request_scope_for(request_id)[-24:]}"


_MODEL_POOL_KEY_RE = re.compile(r"\W+")
_MAX_MODEL_POOL_KEY_LEN = 40
_MODEL_POOL_HASH_LEN = 8

# Guards the two module-level pool-name registries below. They are
# process-wide (shared by every PrivyConnectionWrapper/thread in one dbt
# invocation) by design: a model can be retried from a different thread or a
# fresh wrapper instance, and it must land back in the exact same scheduler
# pool every time for a pre-declared allocation-file weight to keep applying.
_pool_registry_lock = threading.Lock()
# node_id -> resolved pool name, so retries of the same model are pool-stable.
_pool_name_by_node: Dict[str, str] = {}
# pool name -> the first node_id that claimed it, so an auto-derived name
# never silently collides with a different model's pool (whether that pool
# came from another auto-derivation or from an explicit override).
_pool_owner_by_name: Dict[str, str] = {}


def _model_key_for_node(node_id: str) -> str:
    """Strip a dbt unique_id's ``<resource_type>.<package>.`` prefix.

    dbt unique_ids for models are ``model.<package>.<name>`` (optionally
    ``.v<version>`` for versioned models); the remainder is a short,
    human-/operator-readable model name that stays identical across every
    run and retry of that model. Falls back to the whole node_id for any
    non-standard shape (fewer than 3 dot-segments).
    """
    parts = node_id.split(".")
    return ".".join(parts[2:]) if len(parts) > 2 else node_id


def _sanitize_pool_key(value: str) -> str:
    key = _MODEL_POOL_KEY_RE.sub("_", value).strip("_")
    if not key:
        key = "_"
    if key[0].isdigit():
        key = f"_{key}"
    return key[:_MAX_MODEL_POOL_KEY_LEN]


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:_MODEL_POOL_HASH_LEN]


def _scheduler_pool_for_node(
    node_id: Optional[str],
    request_id: str,
    pool_priority_map: Optional[Dict[str, str]] = None,
) -> str:
    """Resolve the deterministic ``spark.scheduler.pool`` name for a job.

    Statements with no dbt ``node_id`` (ad hoc SQL that doesn't carry a
    model's query-comment header) keep the pre-existing per-request
    isolation scheme unchanged -- each such request still gets its own,
    never-reused pool -- because there is no stable identity to key a
    shared pool off of, and that isolation is what prevents
    ``spark.scheduler.pool``/job-group/local-property bleed across
    concurrent ``inprocess`` jobs (see the pool-isolation-per-request
    feature this builds on).

    Statements that DO carry a ``node_id`` instead get a *model-stable*
    pool name: every execution/retry of the same model lands in the same
    pool name, so an operator can pre-declare that exact name's weight in a
    ``spark.scheduler.allocation.file`` and have it actually take effect --
    Spark only honors a configured weight for pool names it saw when that
    file was parsed at SparkContext start-up; any other name silently gets
    a fresh, default-weight-1 pool (which is exactly what a fresh
    per-request UUID name always was).

    ``pool_priority_map`` is the opt-in model -> pool-name override carried
    on ``FabricSparkCredentials.privy_pool_priority_map`` (looked up first
    by the full node_id, then by its bare model name). Overrides are
    honored verbatim -- including two different models deliberately
    sharing one pool -- and are never hash-suffixed. Auto-derived names are
    hash-suffixed only when they would otherwise collide with a
    *different* node_id's pool (whether that pool came from another
    auto-derivation or from an override), so two distinct, long model
    names that truncate to the same prefix can never bleed into each
    other's weight.
    """
    if not node_id:
        return _scheduler_pool_for_request_id(request_id)

    with _pool_registry_lock:
        cached = _pool_name_by_node.get(node_id)
        if cached is not None:
            return cached

        model_key = _model_key_for_node(node_id)
        override = None
        if pool_priority_map:
            override = pool_priority_map.get(node_id) or pool_priority_map.get(model_key)

        if override:
            pool_name = override
        else:
            pool_name = f"privy_{_sanitize_pool_key(model_key)}"
            existing_owner = _pool_owner_by_name.get(pool_name)
            if existing_owner is not None and existing_owner != node_id:
                pool_name = f"{pool_name}_{_short_hash(node_id)}"

        _pool_owner_by_name.setdefault(pool_name, node_id)
        _pool_name_by_node[node_id] = pool_name
        return pool_name


def _utc_iso(value: dt.datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_utc_iso(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _duration_ms(start_tick: float, end_tick: float) -> int:
    return int(round((end_tick - start_tick) * 1000))


def _datetime_delta_ms(started_at: dt.datetime, completed_at: dt.datetime) -> int:
    return int(round((completed_at - started_at).total_seconds() * 1000))


def _clamp_tiny_negative_ms(value_ms: int) -> int:
    if -_PRIVY_TIMING_NEGATIVE_CLAMP_MS <= value_ms < 0:
        return 0
    return value_ms


_PRIVY_DISPATCH_SOURCE = """
def __privy_dispatch_v2(
    __privy_spark,
    __privy_sql,
    __privy_marker,
    __privy_request_id,
    __privy_node_id,
    __privy_group,
    __privy_description,
    __privy_pool,
    __privy_span_prefix,
    __privy_timeout_s,
):
    import datetime as __privy_datetime
    import json as __privy_json
    import threading as __privy_threading
    import time as __privy_time

    def __privy_iso(__privy_value):
        return __privy_value.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    __privy_globals = globals()
    __privy_registry_lock = __privy_globals.setdefault(
        "__privy_request_registry_lock_v2", __privy_threading.RLock()
    )
    __privy_registry = __privy_globals.setdefault("__privy_request_registry_v2", {})
    __privy_now = __privy_time.monotonic()
    with __privy_registry_lock:
        for __privy_key, __privy_old_entry in list(__privy_registry.items()):
            __privy_completed_tick = __privy_old_entry.get("completed_tick")
            if (
                __privy_completed_tick is not None
                and __privy_now - __privy_completed_tick > 3600.0
            ):
                __privy_registry.pop(__privy_key, None)
        __privy_entry = __privy_registry.get(__privy_request_id)
        if __privy_entry is None:
            __privy_entry = {
                "event": __privy_threading.Event(),
                "sql": __privy_sql,
                "payload": None,
                "error": None,
                "completed_tick": None,
            }
            __privy_registry[__privy_request_id] = __privy_entry
            __privy_owner = True
        else:
            if __privy_entry["sql"] != __privy_sql:
                raise RuntimeError(
                    "Privy request id was reused for different SQL: " + __privy_request_id
                )
            __privy_owner = False

    if __privy_owner:
        __privy_started_at = __privy_datetime.datetime.now(__privy_datetime.timezone.utc)
        __privy_started_tick = __privy_time.perf_counter()
        __privy_payload = None
        __privy_error = None
        try:
            __privy_spark.sparkContext.setJobGroup(
                __privy_group, __privy_description, True
            )
            __privy_spark.sparkContext.setLocalProperty(
                "spark.scheduler.pool", __privy_pool
            )
            __privy_spark.sparkContext.setLocalProperty(
                "openivm.request_id", __privy_request_id
            )
            __privy_spark.sparkContext.setLocalProperty(
                "openivm.node_id", __privy_node_id
            )
            __privy_df = __privy_spark.sql(__privy_sql)
            __privy_fields = [
                {
                    "name": __privy_field.name,
                    "type": __privy_field.dataType.simpleString(),
                    "nullable": __privy_field.nullable,
                }
                for __privy_field in __privy_df.schema.fields
            ]
            __privy_rows = (
                [list(__privy_row) for __privy_row in __privy_df.collect()]
                if __privy_fields
                else []
            )
            __privy_payload = {
                "data": __privy_rows,
                "schema": {"fields": __privy_fields},
            }
        except BaseException as __privy_exc:
            __privy_error = (type(__privy_exc).__name__, str(__privy_exc))
            raise
        finally:
            __privy_completed_at = __privy_datetime.datetime.now(
                __privy_datetime.timezone.utc
            )
            __privy_span = {
                "request_id": __privy_request_id,
                "node_id": __privy_node_id,
                "server_started_at": __privy_iso(__privy_started_at),
                "server_completed_at": __privy_iso(__privy_completed_at),
                "server_duration_ms": int(
                    round(
                        (__privy_time.perf_counter() - __privy_started_tick) * 1000
                    )
                ),
            }
            if __privy_payload is not None:
                __privy_payload["execution_span"] = __privy_span
            try:
                print(
                    __privy_span_prefix
                    + __privy_json.dumps(__privy_span, sort_keys=True)
                )
            finally:
                try:
                    for __privy_prop in (
                        "spark.jobGroup.id",
                        "spark.job.description",
                        "spark.job.interruptOnCancel",
                        "spark.scheduler.pool",
                        "openivm.request_id",
                        "openivm.node_id",
                    ):
                        __privy_spark.sparkContext.setLocalProperty(__privy_prop, None)
                finally:
                    with __privy_registry_lock:
                        __privy_entry["payload"] = __privy_payload
                        __privy_entry["error"] = __privy_error
                        __privy_entry["completed_tick"] = __privy_time.monotonic()
                        __privy_entry["event"].set()
    else:
        if not __privy_entry["event"].wait(
            timeout=max(1.0, float(__privy_timeout_s) + 60.0)
        ):
            raise TimeoutError(
                "Timed out waiting for in-flight Privy request "
                + __privy_request_id
            )
        if __privy_entry["error"] is not None:
            __privy_error_type, __privy_error_message = __privy_entry["error"]
            raise RuntimeError(
                "Original Privy request failed: "
                + __privy_error_type
                + ": "
                + __privy_error_message
            )
        __privy_payload = __privy_entry["payload"]
        if __privy_payload is None:
            raise RuntimeError(
                "Completed Privy request has no payload: " + __privy_request_id
            )
        __privy_span = __privy_payload.get("execution_span")
        if __privy_span is not None:
            print(
                __privy_span_prefix
                + __privy_json.dumps(__privy_span, sort_keys=True)
            )

    print(__privy_marker)
    print(__privy_json.dumps(__privy_payload, default=str))
    print(__privy_marker)
"""


def _build_exec_snippet(
    sql: str,
    marker: str,
    request_id: Optional[str] = None,
    timeout_s: float = _UNBOUNDED_TIMEOUT_S,
    pool_priority_map: Optional[Dict[str, str]] = None,
) -> str:
    """Build the Python snippet run (inprocess) on the notebook side.

    Runs ``spark.sql(sql)``, serializes the result into the same
    ``{"data": [...], "schema": {"fields": [...]}}`` shape Livy's statement
    API returns, and prints it between two copies of a unique marker so the
    client can find it even if the query itself prints other output.

    ``inprocess`` mode shares the notebook kernel's thread-local Spark
    properties, so without an explicit ``setJobGroup`` every job inherits the
    description Fabric set on its own start-up cell and is unattributable in
    the Spark UI.

    DDL/DML statements are executed eagerly by ``spark.sql`` and expose no
    output schema; collecting them would only round-trip an empty list.

    Completed request ids remain cached briefly in the notebook interpreter,
    so retrying a submit whose Relay response was lost returns the original
    result without executing ``spark.sql`` again.

    The job group is cleared via ``setLocalProperty(..., None)`` rather than
    ``clearJobGroup()`` because some Fabric runtimes do not expose the latter.
    """
    request_id = request_id or marker
    node_id = _node_id_for(sql)
    pool_literal = repr(_scheduler_pool_for_node(node_id, request_id, pool_priority_map))
    sql_literal = repr(sql)
    marker_literal = repr(marker)
    request_id_literal = repr(request_id)
    node_id_literal = repr(node_id)
    group_literal = repr(_job_group_for(sql))
    description_literal = repr(" ".join(sql.split())[:400])
    span_prefix_literal = repr(_PRIVY_EXECUTION_SPAN_PREFIX)
    timeout_literal = repr(timeout_s)
    return (
        "if '__privy_dispatch_v2' not in globals():\n"
        f"    exec({_PRIVY_DISPATCH_SOURCE!r}, globals())\n"
        "globals()['__privy_dispatch_v2'](\n"
        "    spark,\n"
        f"    {sql_literal},\n"
        f"    {marker_literal},\n"
        f"    {request_id_literal},\n"
        f"    {node_id_literal},\n"
        f"    {group_literal},\n"
        f"    {description_literal},\n"
        f"    {pool_literal},\n"
        f"    {span_prefix_literal},\n"
        f"    {timeout_literal},\n"
        ")\n"
    )


def _extract_marked_json(stdout: str, marker: str) -> Dict[str, Any]:
    lines = stdout.splitlines()
    marker_lines = [idx for idx, line in enumerate(lines) if line == marker]
    if not marker_lines:
        raise DbtDatabaseError(
            f"Privy response is missing the result marker; stdout={stdout[-2000:]!r}"
        )
    if len(marker_lines) < 2:
        raise DbtDatabaseError(
            f"Privy response is missing the closing result marker; stdout={stdout[-2000:]!r}"
        )
    raw = "\n".join(lines[marker_lines[0] + 1 : marker_lines[1]]).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DbtDatabaseError(
            f"Could not parse Privy result JSON ({exc}); raw={raw[:2000]!r}"
        ) from exc


def _extract_execution_span(stdout: Optional[str]) -> Optional[Dict[str, Any]]:
    for line in reversed((stdout or "").splitlines()):
        if not line.startswith(_PRIVY_EXECUTION_SPAN_PREFIX):
            continue
        raw = line[len(_PRIVY_EXECUTION_SPAN_PREFIX) :].strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.debug(f"Could not parse Privy execution span JSON ({exc}); raw={raw[:2000]!r}")
            return None
    return None


def _log_execution_span(span: Optional[Dict[str, Any]]) -> None:
    if span is None:
        return
    logger.info(f"{_PRIVY_EXECUTION_SPAN_PREFIX}{json.dumps(span, sort_keys=True)}")


def _enrich_execution_span(
    server_span: Optional[Dict[str, Any]],
    request_id: str,
    node_id: Optional[str],
    client_submitted_at: dt.datetime,
    client_completed_at: dt.datetime,
    client_duration_ms: int,
) -> Dict[str, Any]:
    span: Dict[str, Any] = dict(server_span or {})
    span.setdefault("request_id", request_id)
    span.setdefault("node_id", node_id)
    span["client_submitted_at"] = _utc_iso(client_submitted_at)
    span["client_completed_at"] = _utc_iso(client_completed_at)
    span["client_duration_ms"] = client_duration_ms

    server_started_at = _parse_utc_iso(span.get("server_started_at"))
    if server_started_at is not None:
        span["relay_wait_before_server_ms"] = _clamp_tiny_negative_ms(
            _datetime_delta_ms(client_submitted_at, server_started_at)
        )

    server_completed_at = _parse_utc_iso(span.get("server_completed_at"))
    if server_completed_at is not None:
        span["relay_return_after_server_ms"] = _datetime_delta_ms(
            server_completed_at, client_completed_at
        )

    return span


class PrivyConnectionWrapper:
    """Connection wrapper for the privy (Azure Relay) connection method.

    Deliberately duck-types the same surface as ``FabricSparkConnectionWrapper``
    (see ``connections.py``) without importing/inheriting from it — mirrors
    how ``LivySessionConnectionWrapper`` avoids a circular import between
    ``connections.py`` and this module.
    """

    def __init__(self, relay_client: Any, credentials: FabricSparkCredentials) -> None:
        self._client = relay_client
        self._timeout_s = _query_timeout_s(credentials)
        self._connect_retries = max(0, int(getattr(credentials, "connect_retries", 0) or 0))
        self._connect_timeout_s = max(0.0, float(getattr(credentials, "connect_timeout", 0) or 0))
        self._pool_priority_map: Dict[str, str] = dict(
            getattr(credentials, "privy_pool_priority_map", None) or {}
        )
        self._rows: Optional[List] = None
        self._schema: Optional[List[Dict[str, Any]]] = None
        self._active_lock = threading.Lock()
        self._active_request: Any = None
        self._active_job_id: Optional[str] = None

    def cursor(self) -> "PrivyConnectionWrapper":
        return self

    def cancel(self) -> None:
        with self._active_lock:
            request = self._active_request
            job_id = self._active_job_id
        if request is None or job_id is None:
            logger.debug("No active Privy job to cancel")
            return
        self._cancel_job(request, job_id)

    def close(self) -> None:
        self._rows = None
        self._schema = None

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Optional[List]:
        return self._rows

    def fetchmany(self, size: Optional[int] = None) -> Optional[List]:
        if self._rows is None:
            return None
        return self._rows if size is None else self._rows[:size]

    def fetchone(self) -> Optional[Any]:
        return self._rows[0] if self._rows else None

    def _control_call(
        self,
        operation: str,
        request_id: str,
        call: Any,
        quick_status_probe: Optional[Any] = None,
    ) -> Any:
        """Invoke ``call`` for ``operation``, retrying on transient relay errors.

        A transient failure (HTTP 504/408/... or a dropped connection) is
        ambiguous: the relay may simply have lost the response to a job that
        already finished server-side. When ``quick_status_probe`` is given
        (only the ``poll`` call site passes one) it is tried first, with no
        sleep — a short, idempotent, non-blocking re-check of the *same*
        job/request id. A terminal result from that probe is used
        immediately instead of sleeping ``connect_timeout`` seconds and
        starting a brand new long poll. Only when the probe is inconclusive
        (job still running, or the probe itself fails transiently) do we
        fall back to the original fixed sleep-then-retry loop.
        """
        retries = 0
        while True:
            try:
                return call()
            except Exception as exc:
                if not _is_transient_relay_error(exc):
                    raise
                if quick_status_probe is not None:
                    probe_outcome = self._quick_status_probe(
                        operation, request_id, exc, quick_status_probe
                    )
                    if probe_outcome is not _PROBE_INCONCLUSIVE:
                        return probe_outcome
                if retries >= self._connect_retries:
                    raise PrivyTransportRetryError(
                        f"Privy relay {operation} failed after {retries + 1} attempt(s) "
                        f"({_relay_error_label(exc)}). The remote request may still be "
                        f"running; refusing to resubmit SQL."
                    ) from None
                retries += 1
                logger.warning(
                    f"Privy relay {operation} got {_relay_error_label(exc)}; retrying the "
                    f"same request/job ({retries}/{self._connect_retries}) in "
                    f"{self._connect_timeout_s:g}s. request_id={request_id}"
                )
                if self._connect_timeout_s:
                    time.sleep(self._connect_timeout_s)

    def _quick_status_probe(
        self,
        operation: str,
        request_id: str,
        exc: Exception,
        probe: Any,
    ) -> Any:
        """Try one non-blocking status re-check after a transient ``exc``.

        Returns the probe's ``(state, result)`` outcome once the job is no
        longer ``running``, or ``_PROBE_INCONCLUSIVE`` if the job is still
        running or the probe itself raised a transient relay error (in which
        case the caller falls back to its normal sleep-then-retry loop).
        Non-transient probe errors propagate — this never widens the set of
        exceptions treated as retryable.
        """
        try:
            outcome = probe()
        except Exception as probe_exc:
            if not _is_transient_relay_error(probe_exc):
                raise
            logger.debug(
                f"Privy relay {operation} got {_relay_error_label(exc)}; the immediate "
                f"status re-check also failed ({_relay_error_label(probe_exc)}), falling "
                f"back to the normal retry backoff. request_id={request_id}"
            )
            return _PROBE_INCONCLUSIVE
        state, _ = outcome
        if state == "running":
            return _PROBE_INCONCLUSIVE
        logger.info(
            f"Privy relay {operation} got {_relay_error_label(exc)}, but an immediate "
            f"status re-check found the job already finished; using that result instead "
            f"of sleeping {self._connect_timeout_s:g}s and starting another long poll. "
            f"request_id={request_id}"
        )
        return outcome

    def _cancel_job(self, request: Any, job_id: str) -> None:
        attempts = min(self._connect_retries, 2) + 1
        for attempt in range(attempts):
            try:
                self._client.cancel(request, job_id)
                return
            except Exception as exc:
                if not _is_transient_relay_error(exc) or attempt == attempts - 1:
                    logger.warning(
                        f"Could not cancel Privy job {job_id}: {_relay_error_label(exc)}"
                    )
                    return
                if self._connect_timeout_s:
                    time.sleep(min(self._connect_timeout_s, 1.0))

    def _run_python(self, code: str, request_id: str) -> Any:
        job_methods = ("submit", "poll", "cancel")
        if not all(callable(getattr(self._client, name, None)) for name in job_methods):
            return self._client.run_python(
                code,
                mode="inprocess",
                timeout_s=self._timeout_s,
            )

        from privy.protocol import DEFAULT_POLL_WAIT_S, ExecRequest

        request = ExecRequest(
            kind="python",
            code=code,
            mode="inprocess",
            timeout_s=self._timeout_s,
            request_id=request_id,
        )
        job_id: Optional[str] = None
        deadline = (
            time.monotonic()
            + self._timeout_s
            + max(
                _PRIVY_CONTROL_GRACE_S,
                self._connect_retries * self._connect_timeout_s,
            )
        )
        backoff_s = _PRIVY_POLL_BACKOFF_MIN_S
        try:
            job_id = self._control_call(
                "submit",
                request_id,
                lambda: self._client.submit(request),
            )
            with self._active_lock:
                self._active_request = request
                self._active_job_id = job_id

            while True:
                poll_started = time.monotonic()
                state, result = self._control_call(
                    "poll",
                    request_id,
                    lambda: self._client.poll(
                        request,
                        job_id,
                        wait_s=DEFAULT_POLL_WAIT_S,
                    ),
                    quick_status_probe=lambda: self._client.poll(
                        request,
                        job_id,
                        wait_s=_PRIVY_STATUS_PROBE_WAIT_S,
                    ),
                )
                if state != "running":
                    return result
                if time.monotonic() >= deadline:
                    raise DbtDatabaseError(
                        f"Privy query exceeded statement_timeout={self._timeout_s:g}s; "
                        f"increase `statement_timeout` in profiles.yml if the query is "
                        f"expected to run longer."
                    )
                if time.monotonic() - poll_started < 1.0:
                    time.sleep(backoff_s)
                    backoff_s = min(backoff_s * 2, _PRIVY_POLL_BACKOFF_MAX_S)
                else:
                    backoff_s = _PRIVY_POLL_BACKOFF_MIN_S
        except BaseException:
            if job_id is not None:
                self._cancel_job(request, job_id)
            raise
        finally:
            with self._active_lock:
                if self._active_job_id == job_id:
                    self._active_request = None
                    self._active_job_id = None

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        sql = sql.strip()
        if sql.endswith(";"):
            sql = sql[:-1]
        if bindings is not None:
            fixed_bindings = tuple(self._fix_binding(b) for b in bindings)
            sql = sql % fixed_bindings

        node_id = _node_id_for(sql)
        request_id = uuid.uuid4().hex
        marker = f"__PRIVY_RESULT_{request_id}__"
        code = _build_exec_snippet(
            sql,
            marker,
            request_id=request_id,
            timeout_s=self._timeout_s,
            pool_priority_map=self._pool_priority_map,
        )
        logger.debug(f"Submitting to Privy relay (inprocess): {sql}")
        client_submitted_at = dt.datetime.now(dt.timezone.utc)
        client_submitted_tick = time.perf_counter()
        try:
            result = self._run_python(code, request_id)
        except Exception:
            client_completed_tick = time.perf_counter()
            client_completed_at = dt.datetime.now(dt.timezone.utc)
            _log_execution_span(
                _enrich_execution_span(
                    server_span=None,
                    request_id=request_id,
                    node_id=node_id,
                    client_submitted_at=client_submitted_at,
                    client_completed_at=client_completed_at,
                    client_duration_ms=_duration_ms(client_submitted_tick, client_completed_tick),
                )
            )
            raise
        client_completed_tick = time.perf_counter()
        client_completed_at = dt.datetime.now(dt.timezone.utc)
        client_duration_ms = _duration_ms(client_submitted_tick, client_completed_tick)
        execution_span = _extract_execution_span(result.stdout)
        logged_execution_span = _enrich_execution_span(
            server_span=execution_span,
            request_id=request_id,
            node_id=node_id,
            client_submitted_at=client_submitted_at,
            client_completed_at=client_completed_at,
            client_duration_ms=client_duration_ms,
        )

        if not result.ok:
            _log_execution_span(logged_execution_span)
            timeout_note = " (timed out)" if result.timed_out else ""
            raise DbtDatabaseError(
                f"Error while executing query via Privy{timeout_note}: "
                f"{result.stderr or result.stdout}"
            )

        payload = _extract_marked_json(result.stdout, marker)
        _log_execution_span(
            _enrich_execution_span(
                server_span=payload.get("execution_span") or execution_span,
                request_id=request_id,
                node_id=node_id,
                client_submitted_at=client_submitted_at,
                client_completed_at=client_completed_at,
                client_duration_ms=client_duration_ms,
            )
        )
        self._rows = payload.get("data", [])
        self._schema = payload.get("schema", {}).get("fields", [])
        coerce_time_columns(self._rows, self._schema)

    @property
    def description(
        self,
    ) -> Sequence[Tuple[str, Any, None, None, None, None, bool]]:
        if not self._schema:
            return []
        return [
            (field["name"], field["type"], None, None, None, None, field["nullable"])
            for field in self._schema
        ]

    @classmethod
    def _fix_binding(cls, value: Any) -> Any:
        """Convert complex datatypes to primitives that can be loaded by the Spark driver."""
        if isinstance(value, _NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            escaped = str(value).replace("'", "\\'")
            return f"'{escaped}'"


__all__ = [
    "PrivyConnectionManager",
    "PrivyConnectionWrapper",
    "PrivyTransportRetryError",
]
