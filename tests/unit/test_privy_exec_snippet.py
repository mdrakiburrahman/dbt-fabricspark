import ast
import datetime as dt
import json
import threading
from types import SimpleNamespace

import pytest
from dbt_common.exceptions import DbtDatabaseError

import dbt.adapters.fabricspark.privysession as privysession_module
from dbt.adapters.fabricspark.privysession import (
    PrivyConnectionWrapper,
    _build_exec_snippet,
    _extract_execution_span,
    _extract_marked_json,
    _job_group_for,
)

CTAS = (
    '/* {"app": "dbt", "node_id": "model.insights.fact_machine"} */ '
    "create or replace table dbo.fact_machine as select 1 as a"
)


def test_job_group_uses_node_id_from_query_comment():
    assert _job_group_for(CTAS) == "model.insights.fact_machine"


def test_job_group_falls_back_when_comment_absent():
    assert _job_group_for("select 1") == "dbt"


def test_snippet_sets_and_clears_job_group():
    snippet = _build_exec_snippet(CTAS, "MARKER")
    assert "setJobGroup(" in snippet
    assert "model.insights.fact_machine" in snippet
    assert "finally:" in snippet
    assert "PRIVY_EXECUTION_SPAN " in snippet
    # clearJobGroup() is missing on some Fabric runtimes.
    assert "clearJobGroup" not in snippet
    for prop in (
        "spark.jobGroup.id",
        "spark.job.description",
        "spark.job.interruptOnCancel",
        "openivm.request_id",
        "openivm.node_id",
    ):
        assert prop in snippet


def test_snippet_truncates_long_job_description():
    snippet = _build_exec_snippet("select " + "x" * 5000, "MARKER")
    description = ast.literal_eval(
        snippet.split("setJobGroup(", 1)[1].split(", True)", 1)[0].split(", ", 1)[1]
    )
    assert len(description) <= 400


def _parse_utc_iso(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _run(snippet, fields, rows):
    """Execute the snippet with a stubbed ``spark`` global."""

    class _Field:
        def __init__(self, name):
            self.name = name
            self.nullable = True
            self.dataType = type("_T", (), {"simpleString": staticmethod(lambda: "int")})()

    collected = []

    class _DF:
        schema = type("_S", (), {"fields": [_Field(f) for f in fields]})()

        def collect(self):
            collected.append(True)
            return rows

    class _Ctx:
        def __init__(self):
            self.props = {}

        def setJobGroup(self, group, description, interrupt):
            self.props["spark.jobGroup.id"] = group

        def setLocalProperty(self, key, value):
            self.props[key] = value

    class _Spark:
        def __init__(self):
            self.sparkContext = _Ctx()

        def sql(self, _sql):
            return _DF()

    spark = _Spark()
    out = []
    env = {"spark": spark, "print": out.append}
    exec(snippet, env)  # noqa: S102 - exercising generated code is the point
    stdout = "\n".join(out)
    payload = _extract_marked_json(stdout, "MARKER")
    return payload, collected, spark.sparkContext.props, stdout


def _assert_execution_span(span, request_id, node_id):
    assert span["request_id"] == request_id
    assert span["node_id"] == node_id
    started = _parse_utc_iso(span["server_started_at"])
    completed = _parse_utc_iso(span["server_completed_at"])
    assert started.tzinfo == dt.timezone.utc
    assert completed.tzinfo == dt.timezone.utc
    assert completed >= started
    assert span["server_duration_ms"] >= 0


def _assert_client_execution_span(
    span,
    *,
    request_id,
    node_id,
    client_submitted_at,
    client_completed_at,
    client_duration_ms,
    relay_wait_before_server_ms,
    relay_return_after_server_ms,
):
    assert span["request_id"] == request_id
    assert span["node_id"] == node_id
    assert span["client_submitted_at"] == client_submitted_at
    assert span["client_completed_at"] == client_completed_at
    assert span["client_duration_ms"] == client_duration_ms
    assert span["relay_wait_before_server_ms"] == relay_wait_before_server_ms
    assert span["relay_return_after_server_ms"] == relay_return_after_server_ms
    submitted = _parse_utc_iso(span["client_submitted_at"])
    completed = _parse_utc_iso(span["client_completed_at"])
    assert submitted.tzinfo == dt.timezone.utc
    assert completed.tzinfo == dt.timezone.utc
    assert completed >= submitted


def _install_client_clock(monkeypatch, datetimes, perf_counters):
    class _FakeDateTime(dt.datetime):
        _values = iter(datetimes)

        @classmethod
        def now(cls, tz=None):
            assert tz == dt.timezone.utc
            return next(cls._values)

    perf_counter_values = iter(perf_counters)
    monkeypatch.setattr(privysession_module.dt, "datetime", _FakeDateTime)
    monkeypatch.setattr(
        privysession_module.time, "perf_counter", lambda: next(perf_counter_values)
    )


def _logged_spans(logger):
    return [
        json.loads(message.split(" ", 1)[1])
        for message in logger.info_messages
        if message.startswith("PRIVY_EXECUTION_SPAN ")
    ]


def _privy_stdout(marker=None, payload=None, server_span=None, extra_lines=None):
    lines = list(extra_lines or [])
    if server_span is not None:
        lines.append(f"PRIVY_EXECUTION_SPAN {json.dumps(server_span, sort_keys=True)}")
    if marker is not None:
        lines.append(marker)
    if payload is not None:
        lines.append(json.dumps(payload))
    if marker is not None:
        lines.append(marker)
    return "\n".join(lines)


def test_command_without_output_schema_skips_collect():
    payload, collected, props, stdout = _run(
        _build_exec_snippet(CTAS, "MARKER"), fields=[], rows=[]
    )
    assert payload["data"] == []
    assert payload["schema"] == {"fields": []}
    _assert_execution_span(
        payload["execution_span"],
        request_id="MARKER",
        node_id="model.insights.fact_machine",
    )
    assert _extract_execution_span(stdout) == payload["execution_span"]
    assert collected == []
    assert props["spark.jobGroup.id"] is None
    assert props["openivm.request_id"] is None
    assert props["openivm.node_id"] is None


def test_query_with_output_schema_collects_rows():
    snippet = _build_exec_snippet("select 1 as id", "MARKER")
    payload, collected, props, stdout = _run(snippet, fields=["id"], rows=[[1]])
    assert payload["data"] == [[1]]
    assert payload["schema"]["fields"][0]["name"] == "id"
    _assert_execution_span(payload["execution_span"], request_id="MARKER", node_id=None)
    assert _extract_execution_span(stdout) == payload["execution_span"]
    assert collected == [True]
    assert props["spark.jobGroup.id"] is None
    assert props["openivm.request_id"] is None
    assert props["openivm.node_id"] is None


def test_execute_logs_structured_execution_span(monkeypatch):
    class _Logger:
        def __init__(self):
            self.debug_messages = []
            self.info_messages = []

        def debug(self, message):
            self.debug_messages.append(message)

        def info(self, message):
            self.info_messages.append(message)

    class _Client:
        def run_python(self, code, mode, timeout_s):
            assert mode == "inprocess"
            assert timeout_s == 30.0
            assert "__PRIVY_RESULT_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__" in code
            payload = {
                "data": [[1]],
                "schema": {"fields": [{"name": "id", "type": "int", "nullable": True}]},
                "execution_span": {
                    "request_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "node_id": "model.insights.dim_thing",
                    "server_started_at": "2026-08-22T19:30:00.140Z",
                    "server_completed_at": "2026-08-22T19:30:00.710Z",
                    "server_duration_ms": 570,
                },
            }
            stdout = _privy_stdout(
                marker="__PRIVY_RESULT_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__",
                payload=payload,
                server_span=payload["execution_span"],
            )
            return SimpleNamespace(ok=True, timed_out=False, stdout=stdout, stderr="")

    logger = _Logger()
    monkeypatch.setattr(privysession_module, "logger", logger)
    monkeypatch.setattr(
        privysession_module.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    _install_client_clock(
        monkeypatch,
        datetimes=[
            dt.datetime(2026, 8, 22, 19, 30, 0, 100000, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 8, 22, 19, 30, 0, 760000, tzinfo=dt.timezone.utc),
        ],
        perf_counters=[10.0, 10.66],
    )

    wrapper = PrivyConnectionWrapper(_Client(), SimpleNamespace(statement_timeout=30))
    sql = '/* {"app": "dbt", "node_id": "model.insights.dim_thing"} */ select 1 as id'
    wrapper.execute(sql)

    assert wrapper.fetchall() == [[1]]
    assert wrapper.description == [("id", "int", None, None, None, None, True)]

    span_logs = _logged_spans(logger)
    assert len(span_logs) == 1
    span = span_logs[0]
    _assert_execution_span(
        span,
        request_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        node_id="model.insights.dim_thing",
    )
    _assert_client_execution_span(
        span,
        request_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        node_id="model.insights.dim_thing",
        client_submitted_at="2026-08-22T19:30:00.100Z",
        client_completed_at="2026-08-22T19:30:00.760Z",
        client_duration_ms=660,
        relay_wait_before_server_ms=40,
        relay_return_after_server_ms=50,
    )


def test_execute_logs_best_available_execution_span_on_failure(monkeypatch):
    class _Logger:
        def __init__(self):
            self.debug_messages = []
            self.info_messages = []

        def debug(self, message):
            self.debug_messages.append(message)

        def info(self, message):
            self.info_messages.append(message)

    class _Client:
        def run_python(self, code, mode, timeout_s):
            assert mode == "inprocess"
            assert timeout_s == 30.0
            assert "__PRIVY_RESULT_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb__" in code
            stdout = _privy_stdout(
                server_span={
                    "request_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "node_id": "model.insights.failed_thing",
                    "server_started_at": "2026-08-22T19:31:00.099Z",
                    "server_completed_at": "2026-08-22T19:31:00.250Z",
                    "server_duration_ms": 151,
                },
                extra_lines=["relay failed before payload marker"],
            )
            return SimpleNamespace(ok=False, timed_out=False, stdout=stdout, stderr="relay failed")

    logger = _Logger()
    monkeypatch.setattr(privysession_module, "logger", logger)
    monkeypatch.setattr(
        privysession_module.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
    )
    _install_client_clock(
        monkeypatch,
        datetimes=[
            dt.datetime(2026, 8, 22, 19, 31, 0, 100000, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 8, 22, 19, 31, 0, 400000, tzinfo=dt.timezone.utc),
        ],
        perf_counters=[20.0, 20.3],
    )

    wrapper = PrivyConnectionWrapper(_Client(), SimpleNamespace(statement_timeout=30))
    sql = '/* {"app": "dbt", "node_id": "model.insights.failed_thing"} */ select 1 as id'

    with pytest.raises(DbtDatabaseError) as excinfo:
        wrapper.execute(sql)

    assert "relay failed" in str(excinfo.value)
    span_logs = _logged_spans(logger)
    assert len(span_logs) == 1
    span = span_logs[0]
    _assert_execution_span(
        span,
        request_id="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        node_id="model.insights.failed_thing",
    )
    _assert_client_execution_span(
        span,
        request_id="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        node_id="model.insights.failed_thing",
        client_submitted_at="2026-08-22T19:31:00.100Z",
        client_completed_at="2026-08-22T19:31:00.400Z",
        client_duration_ms=300,
        relay_wait_before_server_ms=0,
        relay_return_after_server_ms=150,
    )


def test_concurrent_snippets_do_not_cross_contaminate_shared_globals():
    thread_count = 4
    snippets = []
    markers = []
    request_ids = []
    node_ids = []
    for idx in range(thread_count):
        marker = f"MARKER_{idx}"
        request_id = f"request_{idx}"
        node_id = f"model.concurrent.value_{idx}"
        sql = f'/* {{"app": "dbt", "node_id": "{node_id}"}} */ select {idx} as value_{idx}'
        snippets.append(_build_exec_snippet(sql, marker, request_id=request_id))
        markers.append(marker)
        request_ids.append(request_id)
        node_ids.append(node_id)

    barrier = threading.Barrier(thread_count)
    overlap_lock = threading.Lock()
    active_overlap = 0
    max_overlap = 0
    observed_props = {}
    cleared_props = {}
    errors = []

    class _PrintCollector:
        def __init__(self):
            self.lines = []
            self.lock = threading.Lock()

        def __call__(self, value):
            with self.lock:
                self.lines.append((threading.get_ident(), value))

    class _Field:
        def __init__(self, name):
            self.name = name
            self.nullable = True
            self.dataType = type("_T", (), {"simpleString": staticmethod(lambda: "int")})()

    class _DF:
        def __init__(self, idx):
            self.schema = type("_S", (), {"fields": [_Field(f"value_{idx}")]})()
            self._idx = idx

        def collect(self):
            return [[self._idx]]

    class _Ctx:
        def __init__(self):
            self._lock = threading.Lock()
            self._props_by_thread = {}

        def setJobGroup(self, group, description, interrupt):
            self.setLocalProperty("spark.jobGroup.id", group)

        def setLocalProperty(self, key, value):
            with self._lock:
                props = self._props_by_thread.setdefault(threading.get_ident(), {})
                props[key] = value

        def current_props(self):
            with self._lock:
                return dict(self._props_by_thread.get(threading.get_ident(), {}))

    class _Spark:
        def __init__(self):
            self.sparkContext = _Ctx()

        def sql(self, sql):
            nonlocal active_overlap, max_overlap
            idx = int(sql.rsplit("value_", 1)[1])
            observed_props[idx] = self.sparkContext.current_props()
            with overlap_lock:
                active_overlap += 1
                max_overlap = max(max_overlap, active_overlap)
            try:
                barrier.wait(timeout=5)
            finally:
                with overlap_lock:
                    active_overlap -= 1
            return _DF(idx)

    collector = _PrintCollector()
    spark = _Spark()
    shared_env = {"spark": spark, "print": collector, "__builtins__": __builtins__}

    def _worker(idx):
        try:
            exec(snippets[idx], shared_env)  # noqa: S102 - shared-globals exec is under test
            cleared_props[idx] = spark.sparkContext.current_props()
        except Exception as exc:  # pragma: no cover - asserted via errors
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(idx,)) for idx in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert max_overlap > 1

    marker_to_idx = {marker: idx for idx, marker in enumerate(markers)}
    outputs = {idx: [] for idx in range(thread_count)}
    thread_output_idx = {}
    pending_lines = {}
    for thread_id, line in collector.lines:
        output_idx = thread_output_idx.get(thread_id)
        if output_idx is None and line in marker_to_idx:
            output_idx = marker_to_idx[line]
            thread_output_idx[thread_id] = output_idx
            outputs[output_idx].extend(pending_lines.pop(thread_id, []))
        elif output_idx is None:
            pending_lines.setdefault(thread_id, []).append(line)
            continue
        if output_idx is None:
            continue
        outputs[output_idx].append(line)

    for idx in range(thread_count):
        stdout = "\n".join(outputs[idx])
        payload = _extract_marked_json(stdout, markers[idx])
        assert payload["data"] == [[idx]]
        assert payload["schema"] == {
            "fields": [{"name": f"value_{idx}", "type": "int", "nullable": True}]
        }
        _assert_execution_span(payload["execution_span"], request_ids[idx], node_ids[idx])
        assert _extract_execution_span(stdout) == payload["execution_span"]
        assert observed_props[idx]["openivm.request_id"] == request_ids[idx]
        assert observed_props[idx]["openivm.node_id"] == node_ids[idx]
        assert observed_props[idx]["spark.jobGroup.id"] == node_ids[idx]
        assert cleared_props[idx]["openivm.request_id"] is None
        assert cleared_props[idx]["openivm.node_id"] is None
        assert cleared_props[idx]["spark.jobGroup.id"] is None

    assert not any(
        key.startswith("__privy_exec_") or key.startswith("__privy_payload_") for key in shared_env
    )
