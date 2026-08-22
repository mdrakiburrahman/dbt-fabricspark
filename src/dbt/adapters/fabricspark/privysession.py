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

This is a spike: no lakehouse schema-detection, no high-concurrency
multi-REPL support, and no retry/backoff sophistication beyond a simple
health-check + wait loop. Concurrent dbt threads do run in parallel — privy
captures stdout/stderr per thread and dispatches inprocess calls on a thread
pool, so statements execute simultaneously against the notebook's shared
``spark`` session (setting ``PRIVY_SERIALIZE_INPROCESS=1`` forces them back
to one-at-a-time).

The notebook run is never cancelled by this module (not even on process
exit) — a filesystem cache (``privy-notebook-job.json`` in the cwd) lets
separate dbt invocations reuse the same run instead of each starting their
own. Cancelling it is entirely up to the caller.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livy_backend import coerce_time_columns
from dbt.adapters.fabricspark.livysession import get_headers

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
_PRIVY_EXECUTION_SPAN_PREFIX = "PRIVY_EXECUTION_SPAN "
_PRIVY_TIMING_NEGATIVE_CLAMP_MS = 5


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


def _trigger_notebook_run(
    credentials: FabricSparkCredentials,
) -> Optional[Tuple[str, str, str]]:
    """Best-effort trigger of the Fabric notebook via the Job Scheduler API.

    POST .../items/{notebookId}/jobs/instances?jobType=RunNotebook — a 202
    means the run was accepted (the notebook, and eventually its
    ``RelayServer.serve_forever()`` cell, will start). This never polls the
    job to completion here: the job is meant to run forever, so acceptance is
    the only thing checked at trigger time. The job instance id (parsed from
    the ``Location`` header) is returned so the caller can poll its status
    while waiting for the relay — see ``_wait_for_relay``. Failures to trigger
    are logged and swallowed rather than raised — the notebook might already
    be starting (e.g. from a previous invocation or a manual start), so the
    wait loop is the real arbiter.
    """
    try:
        workspace_id, notebook_id = _parse_notebook_ids(credentials.privy_notebook_url)
    except ValueError as exc:
        logger.warning(
            f"Could not parse workspace/notebook id from privy_notebook_url ({exc}). "
            f"Skipping auto-start; will still wait in case the relay is already up."
        )
        return None

    url = (
        f"{credentials.endpoint}/workspaces/{workspace_id}/items/{notebook_id}"
        f"/jobs/instances?jobType=RunNotebook"
    )
    logger.info(f"Privy relay not responding; triggering Fabric notebook run: POST {url}")
    try:
        headers = get_headers(credentials)
        response = requests.post(url, headers=headers, json={}, timeout=credentials.http_timeout)
        if response.status_code in (200, 202):
            location = response.headers.get("Location", "")
            job_instance_id = location.rstrip("/").rsplit("/", 1)[-1] if location else ""
            logger.info(
                f"Notebook run triggered (HTTP {response.status_code}). "
                f"Job instance: {job_instance_id or 'unknown'}. "
                f"Waiting for the Privy relay to come up..."
            )
            if job_instance_id:
                return workspace_id, notebook_id, job_instance_id
            return None
        else:
            logger.warning(
                f"Notebook run trigger returned HTTP {response.status_code}: "
                f"{response.text[:500]}. Will keep waiting in case it's already starting "
                f"(e.g. started manually, or by a previous dbt invocation)."
            )
    except requests.exceptions.RequestException as exc:
        logger.warning(
            f"Failed to trigger notebook run ({exc}). Will keep waiting in case it's "
            f"already starting."
        )
    return None


def _get_job_instance_status(
    credentials: FabricSparkCredentials, workspace_id: str, item_id: str, job_instance_id: str
) -> Dict[str, Any]:
    """GET .../items/{itemId}/jobs/instances/{jobInstanceId} — the run's live status."""
    url = (
        f"{credentials.endpoint}/workspaces/{workspace_id}/items/{item_id}"
        f"/jobs/instances/{job_instance_id}"
    )
    headers = get_headers(credentials)
    response = requests.get(url, headers=headers, timeout=credentials.http_timeout)
    response.raise_for_status()
    return response.json()


# Filesystem cache of the last notebook job this machine triggered — mirrors
# livysession.py's session-id file, but stores a (workspace, notebook, job
# instance) triple as JSON instead of a bare Livy session id. Lets a fresh
# dbt invocation (a brand new process — dbt spawns one per `debug`/`run`/
# `show` etc.) find and reuse a notebook run a previous invocation already
# triggered instead of firing (and paying for) a new one every single time.
# Nothing here ever cancels the run — that's left entirely to the caller.
_JOB_CACHE_FILENAME = "privy-notebook-job.json"


def _job_cache_path() -> str:
    return os.path.join(os.getcwd(), _JOB_CACHE_FILENAME)


def _read_cached_job_ref() -> Optional[Tuple[str, str, str]]:
    path = _job_cache_path()
    try:
        with open(path) as f:
            data = json.load(f)
        return data["workspace_id"], data["notebook_id"], data["job_instance_id"]
    except Exception as exc:
        logger.debug(f"No usable Privy job cache at {path}: {exc}")
        return None


def _write_cached_job_ref(job_ref: Tuple[str, str, str]) -> None:
    path = _job_cache_path()
    try:
        with open(path, "w") as f:
            json.dump(
                {
                    "workspace_id": job_ref[0],
                    "notebook_id": job_ref[1],
                    "job_instance_id": job_ref[2],
                },
                f,
            )
    except OSError as exc:
        logger.debug(f"Could not write Privy job cache file {path}: {exc}")


def _clear_cached_job_ref() -> None:
    try:
        os.remove(_job_cache_path())
    except OSError:
        pass


def _probe(client: Any) -> bool:
    try:
        result = client.run_python("1", mode="inprocess", timeout_s=_PROBE_TIMEOUT_S)
        return bool(result.ok)
    except Exception as exc:  # noqa: BLE001 — any failure just means "not ready yet"
        logger.debug(f"Privy relay probe failed: {exc}")
        return False


def _wait_for_relay(
    probe_client: Any,
    credentials: FabricSparkCredentials,
    job_ref: Optional[Tuple[str, str, str]] = None,
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
            workspace_id, item_id, job_instance_id = job_ref
            try:
                job = _get_job_instance_status(credentials, workspace_id, item_id, job_instance_id)
                status = job.get("status", "Unknown")
                logger.info(f"Notebook job {job_instance_id} status: {status}")
                if status in _JOB_FAILURE_STATUSES:
                    failure_reason = job.get("failureReason")
                    _clear_cached_job_ref()
                    raise DbtRuntimeError(
                        f"Fabric notebook run {status} before the Privy relay came up "
                        f"(job {job_instance_id}). failureReason={failure_reason}. "
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
                    _clear_cached_job_ref()
                    job_ref = None
            except requests.exceptions.RequestException as exc:
                logger.debug(f"Could not fetch notebook job status: {exc}")

        if time.time() >= deadline:
            raise DbtRuntimeError(
                f"Timed out after {credentials.privy_ready_timeout}s waiting for the "
                f"Privy relay/notebook to respond. Check the Fabric notebook run history, "
                f"or set `privy_auto_start_notebook: false` and start the notebook manually. "
                f"Override the timeout with `privy_ready_timeout: <seconds>` in your profile."
            )
        time.sleep(credentials.poll_wait)


def _ensure_notebook_ready(exec_client: Any, credentials: FabricSparkCredentials) -> None:
    """Probe the relay and, if needed, reuse-or-trigger a notebook run.

    Before triggering a brand-new run, checks the filesystem job cache (see
    ``_read_cached_job_ref``) for a still-active job a previous dbt
    invocation already triggered, and waits on that instead — so back-to-back
    dbt invocations (``debug``, ``run``, ``show``, ...) share one notebook
    session rather than each firing (and paying for) their own. Nothing is
    ever cancelled here or on process exit; cancelling the notebook run is
    entirely up to the caller.
    """
    probe_client = _build_relay_client(credentials, http_timeout_s=_PROBE_HTTP_TIMEOUT_S)
    if _probe(probe_client):
        logger.debug("Privy relay already responding; reusing the existing notebook run.")
        return

    job_ref: Optional[Tuple[str, str, str]] = None
    cached_ref = _read_cached_job_ref()
    if cached_ref is not None:
        try:
            job = _get_job_instance_status(credentials, *cached_ref)
            status = job.get("status", "Unknown")
            if status not in _JOB_TERMINAL_STATUSES:
                logger.info(f"Reusing cached notebook job {cached_ref[2]} (status={status}).")
                job_ref = cached_ref
            else:
                logger.debug(
                    f"Cached notebook job {cached_ref[2]} is terminal ({status}); discarding."
                )
                _clear_cached_job_ref()
        except requests.exceptions.RequestException as exc:
            logger.debug(f"Could not check cached notebook job status ({exc}); discarding cache.")
            _clear_cached_job_ref()

    if job_ref is None:
        if credentials.privy_auto_start_notebook:
            job_ref = _trigger_notebook_run(credentials)
            if job_ref is not None:
                _write_cached_job_ref(job_ref)
        else:
            logger.info(
                "Privy relay not responding and privy_auto_start_notebook is False; "
                "waiting for it to be started manually."
            )

    _wait_for_relay(probe_client, credentials, job_ref)


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


def _build_exec_snippet(sql: str, marker: str, request_id: Optional[str] = None) -> str:
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

    The job group is cleared via ``setLocalProperty(..., None)`` rather than
    ``clearJobGroup()`` because some Fabric runtimes do not expose the latter.
    """
    request_id = request_id or marker
    request_scope = _request_scope_for(request_id)
    function_name = f"__privy_exec_{request_scope}"
    payload_name = f"__privy_payload_{request_scope}"
    sql_literal = repr(sql)
    marker_literal = repr(marker)
    request_id_literal = repr(request_id)
    node_id_literal = repr(_node_id_for(sql))
    group_literal = repr(_job_group_for(sql))
    description_literal = repr(" ".join(sql.split())[:400])
    span_prefix_literal = repr(_PRIVY_EXECUTION_SPAN_PREFIX)
    return (
        f"def {function_name}():\n"
        "    import datetime as __privy_datetime\n"
        "    import time as __privy_time\n"
        "    def __privy_iso(__privy_value):\n"
        "        return __privy_value.isoformat(timespec='milliseconds').replace('+00:00', 'Z')\n"
        f"    __privy_request_id = {request_id_literal}\n"
        f"    __privy_node_id = {node_id_literal}\n"
        "    __privy_started_at = __privy_datetime.datetime.now(__privy_datetime.timezone.utc)\n"
        "    __privy_started_tick = __privy_time.perf_counter()\n"
        "    __privy_payload = None\n"
        f"    spark.sparkContext.setJobGroup({group_literal}, {description_literal}, True)\n"
        "    try:\n"
        f"        __privy_df = spark.sql({sql_literal})\n"
        "        __privy_fields = [\n"
        "            {'name': __f.name, 'type': __f.dataType.simpleString(),"
        " 'nullable': __f.nullable}\n"
        "            for __f in __privy_df.schema.fields\n"
        "        ]\n"
        "        __privy_rows = (\n"
        "            [list(__privy_row) for __privy_row in __privy_df.collect()]\n"
        "            if __privy_fields\n"
        "            else []\n"
        "        )\n"
        "        __privy_payload = {'data': __privy_rows, 'schema': {'fields': __privy_fields}}\n"
        "        return __privy_payload\n"
        "    finally:\n"
        "        try:\n"
        "            __privy_completed_at = __privy_datetime.datetime.now(\n"
        "                __privy_datetime.timezone.utc\n"
        "            )\n"
        "            __privy_span = {\n"
        "                'request_id': __privy_request_id,\n"
        "                'node_id': __privy_node_id,\n"
        "                'server_started_at': __privy_iso(__privy_started_at),\n"
        "                'server_completed_at': __privy_iso(__privy_completed_at),\n"
        "                'server_duration_ms': int(\n"
        "                    round((__privy_time.perf_counter() - __privy_started_tick) * 1000)\n"
        "                ),\n"
        "            }\n"
        "            if __privy_payload is not None:\n"
        "                __privy_payload['execution_span'] = __privy_span\n"
        f"            print({span_prefix_literal} + __import__('json').dumps(__privy_span, sort_keys=True))\n"
        "        finally:\n"
        "            for __privy_prop in (\n"
        "                'spark.jobGroup.id',\n"
        "                'spark.job.description',\n"
        "                'spark.job.interruptOnCancel',\n"
        "            ):\n"
        "                spark.sparkContext.setLocalProperty(__privy_prop, None)\n"
        "try:\n"
        f"    {payload_name} = {function_name}()\n"
        f"    print({marker_literal})\n"
        f"    print(__import__('json').dumps({payload_name}, default=str))\n"
        f"    print({marker_literal})\n"
        "finally:\n"
        f"    globals().pop({payload_name!r}, None)\n"
        f"    globals().pop({function_name!r}, None)\n"
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
        self._rows: Optional[List] = None
        self._schema: Optional[List[Dict[str, Any]]] = None

    def cursor(self) -> "PrivyConnectionWrapper":
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")

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
        code = _build_exec_snippet(sql, marker, request_id=request_id)
        logger.debug(f"Submitting to Privy relay (inprocess): {sql}")
        client_submitted_at = dt.datetime.now(dt.timezone.utc)
        client_submitted_tick = time.perf_counter()
        try:
            result = self._client.run_python(code, mode="inprocess", timeout_s=self._timeout_s)
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
]
