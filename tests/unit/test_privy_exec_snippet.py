import ast
import datetime as dt
import json
import sys
import threading
from types import SimpleNamespace

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
    for prop in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
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


def test_query_with_output_schema_collects_rows():
    snippet = _build_exec_snippet("select 1 as id", "MARKER")
    payload, collected, props, stdout = _run(snippet, fields=["id"], rows=[[1]])
    assert payload["data"] == [[1]]
    assert payload["schema"]["fields"][0]["name"] == "id"
    _assert_execution_span(payload["execution_span"], request_id="MARKER", node_id=None)
    assert _extract_execution_span(stdout) == payload["execution_span"]
    assert collected == [True]
    assert props["spark.jobGroup.id"] is None


def test_execute_logs_structured_execution_span(monkeypatch):
    class _Logger:
        def __init__(self):
            self.debug_messages = []
            self.info_messages = []

        def debug(self, message):
            self.debug_messages.append(message)

        def info(self, message):
            self.info_messages.append(message)

    class _Field:
        def __init__(self, name):
            self.name = name
            self.nullable = True
            self.dataType = type("_T", (), {"simpleString": staticmethod(lambda: "int")})()

    class _DF:
        schema = type("_S", (), {"fields": [_Field("id")]})()

        def collect(self):
            return [[1]]

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

    class _Client:
        def run_python(self, code, mode, timeout_s):
            assert mode == "inprocess"
            assert timeout_s == 30.0
            out = []
            exec(code, {"spark": _Spark(), "print": out.append})  # noqa: S102
            return SimpleNamespace(ok=True, timed_out=False, stdout="\n".join(out), stderr="")

    logger = _Logger()
    monkeypatch.setattr(privysession_module, "logger", logger)

    wrapper = PrivyConnectionWrapper(_Client(), SimpleNamespace(statement_timeout=30))
    sql = '/* {"app": "dbt", "node_id": "model.insights.dim_thing"} */ select 1 as id'
    wrapper.execute(sql)

    assert wrapper.fetchall() == [[1]]
    assert wrapper.description == [("id", "int", None, None, None, None, True)]

    span_logs = [
        message for message in logger.info_messages if message.startswith("PRIVY_EXECUTION_SPAN ")
    ]
    assert len(span_logs) == 1
    span = json.loads(span_logs[0].split(" ", 1)[1])
    _assert_execution_span(
        span,
        request_id=span["request_id"],
        node_id="model.insights.dim_thing",
    )
    assert len(span["request_id"]) == 32


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

    barrier_line = next(
        number
        for number, line in enumerate(snippets[0].splitlines(), start=1)
        if "__privy_fields = [" in line
    )
    barrier = threading.Barrier(thread_count)
    overlap_lock = threading.Lock()
    active_overlap = 0
    max_overlap = 0
    thread_indices = {}
    errors = []

    class _PrintCollector:
        def __init__(self):
            self.lines = []
            self.lock = threading.Lock()

        def __call__(self, value):
            with self.lock:
                self.lines.append((thread_indices[threading.get_ident()], value))

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
            self.props = {}

        def setJobGroup(self, group, description, interrupt):
            self.props["spark.jobGroup.id"] = group

        def setLocalProperty(self, key, value):
            self.props[key] = value

    class _Spark:
        def __init__(self):
            self.sparkContext = _Ctx()

        def sql(self, sql):
            return _DF(int(sql.rsplit("value_", 1)[1]))

    collector = _PrintCollector()
    shared_env = {"spark": _Spark(), "print": collector, "__builtins__": __builtins__}

    def _worker(idx):
        nonlocal active_overlap, max_overlap
        paused = False
        thread_indices[threading.get_ident()] = idx

        def _trace(frame, event, arg):
            nonlocal active_overlap, max_overlap, paused
            if (
                event == "line"
                and frame.f_code.co_filename == "<string>"
                and frame.f_lineno == barrier_line
                and not paused
            ):
                paused = True
                with overlap_lock:
                    active_overlap += 1
                    max_overlap = max(max_overlap, active_overlap)
                try:
                    barrier.wait(timeout=5)
                finally:
                    with overlap_lock:
                        active_overlap -= 1
            return _trace

        try:
            sys.settrace(_trace)
            exec(snippets[idx], shared_env)  # noqa: S102 - shared-globals exec is under test
        except Exception as exc:  # pragma: no cover - asserted via errors
            errors.append(exc)
        finally:
            sys.settrace(None)

    threads = [threading.Thread(target=_worker, args=(idx,)) for idx in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert max_overlap > 1

    outputs = {idx: [] for idx in range(thread_count)}
    for idx, line in collector.lines:
        outputs[idx].append(line)

    for idx in range(thread_count):
        stdout = "\n".join(outputs[idx])
        payload = _extract_marked_json(stdout, markers[idx])
        assert payload["data"] == [[idx]]
        assert payload["schema"] == {
            "fields": [{"name": f"value_{idx}", "type": "int", "nullable": True}]
        }
        _assert_execution_span(payload["execution_span"], request_ids[idx], node_ids[idx])
        assert _extract_execution_span(stdout) == payload["execution_span"]

    assert not any(
        key.startswith("__privy_exec_") or key.startswith("__privy_payload_") for key in shared_env
    )
