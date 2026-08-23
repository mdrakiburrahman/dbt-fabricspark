import json
import re
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests
from dbt_common.exceptions import DbtDatabaseError
from privy import executor as privy_executor
from privy.client import ExecResult
from privy.protocol import DEFAULT_POLL_WAIT_S, ExecRequest

import dbt.adapters.fabricspark.privysession as privysession
from dbt.adapters.fabricspark.connections import _is_permanent_error, _is_retryable_error
from dbt.adapters.fabricspark.privysession import (
    PrivyConnectionWrapper,
    PrivyTransportRetryError,
    _extract_marked_json,
    _scheduler_pool_for_node,
)


def _credentials(**overrides):
    values = {
        "statement_timeout": 30,
        "connect_retries": 25,
        "connect_timeout": 0,
        "poll_statement_wait": 0.05,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _http_error(status_code):
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://relay.invalid/test"
    response._content = b"transient relay fault"
    return requests.HTTPError(f"HTTP {status_code}", response=response)


def _exec_result(*, stdout="", stderr="", ok=True, timed_out=False):
    return SimpleNamespace(
        ok=ok,
        timed_out=timed_out,
        stdout=stdout,
        stderr=stderr,
    )


class _BarrierRelayServer:
    def __init__(self):
        self._lock = threading.Lock()
        self._first_poll = threading.Barrier(32)
        self._jobs = {}
        self._poll_attempts = {}
        self.submit_count = 0
        self.poll_count = 0
        self.fault_counts = {404: 0, 504: 0}
        self.cancelled = []
        self.poll_waits = []
        self.request_ids = []
        self.scheduler_pools = set()

    def submit(self, request):
        marker_match = re.search(r"__PRIVY_RESULT_([0-9a-f]+)__", request.code)
        node_match = re.search(r"model\.relay\.value_(\d+)", request.code)
        assert marker_match is not None
        assert node_match is not None
        request_id = marker_match.group(1)
        assert request.request_id == request_id
        marker = marker_match.group(0)
        index = int(node_match.group(1))
        node_id = f"model.relay.value_{index}"
        pool = _scheduler_pool_for_node(node_id, request_id)
        assert repr(pool) in request.code
        with self._lock:
            self.submit_count += 1
            self.request_ids.append(request.request_id)
            job_id = f"job-{index}"
            self._jobs[job_id] = (index, marker)
            self.scheduler_pools.add(pool)
        return job_id

    def poll(self, request, job_id, *, wait_s):
        del request
        with self._lock:
            self.poll_count += 1
            self.poll_waits.append(wait_s)
            attempt = self._poll_attempts.get(job_id, 0)
            self._poll_attempts[job_id] = attempt + 1
            index, marker = self._jobs[job_id]

        if attempt == 0:
            self._first_poll.wait(timeout=10)
            if index < 16:
                with self._lock:
                    self.fault_counts[404] += 1
                raise _http_error(404)
            if index < 26:
                with self._lock:
                    self.fault_counts[504] += 1
                raise _http_error(504)

        payload = {
            "data": [[index]],
            "schema": {
                "fields": [
                    {
                        "name": f"value_{index}",
                        "type": "int",
                        "nullable": True,
                    }
                ]
            },
        }
        stdout = "\n".join((marker, json.dumps(payload), marker))
        return "done", _exec_result(stdout=stdout)

    def cancel(self, request, job_id):
        del request
        self.cancelled.append(job_id)
        return _exec_result()


def test_32_requests_recover_16_404s_and_10_504s_without_resubmitting_sql():
    relay = _BarrierRelayServer()
    wrappers = [PrivyConnectionWrapper(relay, _credentials()) for _ in range(32)]
    errors = []

    def run(index):
        try:
            wrappers[index].execute(
                f'/* {{"node_id": "model.relay.value_{index}"}} */ select {index} as value_{index}'
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(index,)) for index in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert relay.fault_counts == {404: 16, 504: 10}
    assert relay.submit_count == 32
    assert relay.poll_count == 58
    assert relay.cancelled == []
    assert len(relay.scheduler_pools) == 32
    assert len(set(relay.request_ids)) == 32
    # Each of the 32 jobs takes one real long poll (wait_s=DEFAULT_POLL_WAIT_S);
    # the 26 that hit a transient 404/504 are recovered by one immediate,
    # non-blocking quick status probe (wait_s=_PRIVY_STATUS_PROBE_WAIT_S) each
    # — never by sleeping and issuing a second full long poll.
    assert relay.poll_waits.count(DEFAULT_POLL_WAIT_S) == 32
    assert relay.poll_waits.count(privysession._PRIVY_STATUS_PROBE_WAIT_S) == 26
    assert set(relay.poll_waits) == {DEFAULT_POLL_WAIT_S, privysession._PRIVY_STATUS_PROBE_WAIT_S}
    assert [wrapper.fetchall() for wrapper in wrappers] == [[[index]] for index in range(32)]


class _Field:
    def __init__(self, name):
        self.name = name
        self.nullable = True
        self.dataType = SimpleNamespace(simpleString=lambda: "int")


class _DataFrame:
    schema = SimpleNamespace(fields=[_Field("value")])

    def collect(self):
        return [[7]]


class _ThreadLocalSparkContext:
    def __init__(self):
        self._local = threading.local()

    def setJobGroup(self, group, description, interrupt):
        del description, interrupt
        self.setLocalProperty("spark.jobGroup.id", group)

    def setLocalProperty(self, key, value):
        props = getattr(self._local, "props", None)
        if props is None:
            props = {}
            self._local.props = props
        props[key] = value

    def current_properties(self):
        return dict(getattr(self._local, "props", {}))


class _CountingSpark:
    def __init__(self):
        self.sparkContext = _ThreadLocalSparkContext()
        self._lock = threading.Lock()
        self.sql_calls = 0
        self.observed_properties = []

    def sql(self, sql):
        del sql
        with self._lock:
            self.sql_calls += 1
            self.observed_properties.append(self.sparkContext.current_properties())
        return _DataFrame()


class _LegacyAmbiguousSubmitRelayServer:
    def __init__(self):
        self.spark = _CountingSpark()
        self._start_barrier = threading.Barrier(20)
        self._lock = threading.Lock()
        self._thread_state = threading.local()
        self._jobs = {}
        self._shared_env = {
            "spark": self.spark,
            "print": self._print,
            "__builtins__": __builtins__,
        }
        self.submit_attempts = 0
        self.transport_faults = 0
        self.requests = []
        self.cancelled = []

    def _print(self, value):
        job_id = self._thread_state.job_id
        with self._lock:
            self._jobs[job_id]["lines"].append(str(value))

    def _worker(self, job_id, code):
        self._thread_state.job_id = job_id
        job = self._jobs[job_id]
        try:
            self._start_barrier.wait(timeout=10)
            exec(code, self._shared_env)  # noqa: S102
        except BaseException as exc:
            job["error"] = exc
        finally:
            job["done"].set()

    def submit(self, request):
        with self._lock:
            self.submit_attempts += 1
            self.requests.append(request)
            attempt = self.submit_attempts
            job_id = f"submit-{attempt}"
            job = {
                "done": threading.Event(),
                "error": None,
                "lines": [],
                "thread": None,
            }
            self._jobs[job_id] = job
        thread = threading.Thread(
            target=self._worker,
            args=(job_id, request.code),
            name=job_id,
        )
        job["thread"] = thread
        thread.start()
        if attempt <= 19:
            self.transport_faults += 1
            raise _http_error(504)
        return job_id

    def poll(self, request, job_id, *, wait_s):
        del request
        job = self._jobs[job_id]
        if not job["done"].wait(timeout=wait_s):
            return "running", _exec_result()
        if job["error"] is not None:
            return "done", _exec_result(stderr=str(job["error"]), ok=False)
        return "done", _exec_result(stdout="\n".join(job["lines"]))

    def cancel(self, request, job_id):
        del request
        self.cancelled.append(job_id)
        return _exec_result()

    def join(self):
        for job in self._jobs.values():
            job["thread"].join(timeout=10)


def test_legacy_server_notebook_dedupe_executes_ambiguous_submit_once():
    relay = _LegacyAmbiguousSubmitRelayServer()
    wrapper = PrivyConnectionWrapper(relay, _credentials())
    wrapper.execute(
        '/* {"node_id": "model.relay.materialized_view"} */ '
        "create materialized view mv as select 7 as value"
    )
    relay.join()

    assert relay.submit_attempts == 20
    assert relay.transport_faults == 19
    assert relay.spark.sql_calls == 1
    assert relay.cancelled == []
    assert len({id(request) for request in relay.requests}) == 1
    assert len({request.request_id for request in relay.requests}) == 1
    assert relay.requests[0].request_id
    assert wrapper.fetchall() == [[7]]
    assert len(relay.spark.observed_properties) == 1
    properties = relay.spark.observed_properties[0]
    assert properties["spark.jobGroup.id"] == "model.relay.materialized_view"
    assert properties["spark.scheduler.pool"].startswith("privy_")
    assert properties["openivm.request_id"]

    for job in relay._jobs.values():
        assert job["error"] is None
        payload = _extract_marked_json(
            "\n".join(job["lines"]),
            re.search(r"__PRIVY_RESULT_[0-9a-f]+__", "\n".join(job["lines"])).group(0),
        )
        assert payload["data"] == [[7]]


class _ProtocolIdempotentRelayServer:
    def __init__(self):
        self.submit_attempts = 0
        self.transport_faults = 0
        self.requests = []
        self.wire_request_ids = []
        self.job_ids = []
        self.poll_waits = []
        self.cancelled = []

    def submit(self, request):
        self.submit_attempts += 1
        self.requests.append(request)
        wire_request = ExecRequest.from_json(replace(request, action="submit").to_json())
        self.wire_request_ids.append(wire_request.request_id)
        response = privy_executor.execute(wire_request)
        assert response.job_id
        self.job_ids.append(response.job_id)
        if self.submit_attempts <= 19:
            self.transport_faults += 1
            raise _http_error(504)
        return response.job_id

    def poll(self, request, job_id, *, wait_s):
        self.poll_waits.append(wait_s)
        response = privy_executor.execute(
            ExecRequest.from_json(
                replace(
                    request,
                    action="poll",
                    job_id=job_id,
                    wait_s=wait_s,
                ).to_json()
            )
        )
        return response.state, ExecResult.from_response(response)

    def cancel(self, request, job_id):
        self.cancelled.append(job_id)
        response = privy_executor.execute(
            ExecRequest.from_json(replace(request, action="cancel", job_id=job_id).to_json())
        )
        return ExecResult.from_response(response)


def test_protocol_idempotency_reuses_one_job_for_19_ambiguous_submit_retries():
    spark = _CountingSpark()
    privy_executor.seed_inprocess_globals({"spark": spark})
    relay = _ProtocolIdempotentRelayServer()
    wrapper = PrivyConnectionWrapper(relay, _credentials())

    wrapper.execute(
        '/* {"node_id": "model.relay.protocol_materialized_view"} */ '
        "create materialized view mv as select 7 as value"
    )

    assert relay.submit_attempts == 20
    assert relay.transport_faults == 19
    assert len({id(request) for request in relay.requests}) == 1
    assert len({request.request_id for request in relay.requests}) == 1
    assert relay.requests[0].request_id
    assert set(relay.wire_request_ids) == {relay.requests[0].request_id}
    assert len(set(relay.job_ids)) == 1
    assert spark.sql_calls == 1
    assert relay.cancelled == []
    assert set(relay.poll_waits) == {DEFAULT_POLL_WAIT_S}
    assert wrapper.fetchall() == [[7]]


class _Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


class _ScriptedPollRelay:
    def __init__(self, clock, running_durations):
        self.clock = clock
        self.running_durations = list(running_durations)
        self.poll_waits = []
        self.request = None

    def submit(self, request):
        self.request = request
        return "job-poll"

    def poll(self, request, job_id, *, wait_s):
        assert request is self.request
        assert job_id == "job-poll"
        self.poll_waits.append(wait_s)
        if self.running_durations:
            self.clock.advance(self.running_durations.pop(0))
            return "running", _exec_result()

        marker = re.search(r"__PRIVY_RESULT_[0-9a-f]+__", request.code).group(0)
        payload = {
            "data": [[1]],
            "schema": {"fields": [{"name": "value", "type": "int", "nullable": True}]},
        }
        return "done", _exec_result(stdout="\n".join((marker, json.dumps(payload), marker)))

    def cancel(self, request, job_id):
        raise AssertionError((request, job_id))


def _use_clock(monkeypatch, clock):
    monkeypatch.setattr(
        privysession,
        "time",
        SimpleNamespace(
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            perf_counter=privysession.time.perf_counter,
            time=privysession.time.time,
        ),
    )


def test_completed_long_poll_does_not_add_client_side_sleep(monkeypatch):
    clock = _Clock()
    relay = _ScriptedPollRelay(clock, running_durations=[1.01])
    _use_clock(monkeypatch, clock)

    wrapper = PrivyConnectionWrapper(relay, _credentials(poll_statement_wait=0.01))
    wrapper.execute("select 1 as value")

    assert relay.poll_waits == [DEFAULT_POLL_WAIT_S, DEFAULT_POLL_WAIT_S]
    assert clock.sleeps == []
    assert wrapper.fetchall() == [[1]]


def test_immediate_legacy_polls_use_bounded_client_backoff(monkeypatch):
    clock = _Clock()
    relay = _ScriptedPollRelay(clock, running_durations=[0.0] * 6)
    _use_clock(monkeypatch, clock)

    wrapper = PrivyConnectionWrapper(relay, _credentials(poll_statement_wait=0.01))
    wrapper.execute("select 1 as value")

    assert set(relay.poll_waits) == {DEFAULT_POLL_WAIT_S}
    assert clock.sleeps == [0.25, 0.5, 1.0, 2.0, 4.0, 5.0]
    assert wrapper.fetchall() == [[1]]


class _AmbiguousPollRelay:
    """Scripts a fixed sequence of outcomes for one submitted job's polls.

    Each scripted outcome is ``("raise", status_code)`` — the long-poll HTTP
    request itself fails transiently (e.g. an Azure Relay 504) — ``("running",
    None)`` — a successful long poll observes the job genuinely still
    running — or ``("done", None)`` — a successful (long or quick) poll finds
    the job finished. Reproduces the runtime-forensics scenario: Spark/the
    remote job actually finished, but the relay's response to the long poll
    was lost, surfacing as a client-side transient HTTP error.
    """

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.poll_waits = []
        self.submit_count = 0
        self.cancelled = []
        self.request = None

    def submit(self, request):
        self.submit_count += 1
        self.request = request
        return "job-ambiguous"

    def poll(self, request, job_id, *, wait_s):
        assert request is self.request
        assert job_id == "job-ambiguous"
        self.poll_waits.append(wait_s)
        kind, detail = self.outcomes.pop(0)
        if kind == "raise":
            raise _http_error(detail)
        if kind == "running":
            return "running", _exec_result()
        marker = re.search(r"__PRIVY_RESULT_[0-9a-f]+__", request.code).group(0)
        payload = {
            "data": [[42]],
            "schema": {"fields": [{"name": "value", "type": "int", "nullable": True}]},
        }
        return "done", _exec_result(stdout="\n".join((marker, json.dumps(payload), marker)))

    def cancel(self, request, job_id):
        self.cancelled.append(job_id)
        return _exec_result()


def test_504_after_job_already_done_uses_quick_probe_not_sleep_and_new_long_poll(monkeypatch):
    """Reproduces the canary forensics: the long poll's HTTP response is lost
    (surfaces as a 504) after Spark already finished the job. The adapter
    must react with one immediate, non-blocking status re-check for the same
    job — not the old fixed ``connect_timeout`` sleep followed by a brand new
    ``DEFAULT_POLL_WAIT_S`` long poll — and must not resubmit the SQL.
    """
    clock = _Clock()
    relay = _AmbiguousPollRelay(
        [
            ("raise", 504),  # long poll's HTTP response lost/ambiguous
            ("done", None),  # but the job had actually already finished
        ]
    )
    _use_clock(monkeypatch, clock)

    wrapper = PrivyConnectionWrapper(relay, _credentials(connect_timeout=10, connect_retries=1))
    wrapper.execute("select 1 as value")

    assert relay.poll_waits == [DEFAULT_POLL_WAIT_S, privysession._PRIVY_STATUS_PROBE_WAIT_S]
    assert clock.sleeps == []
    assert relay.submit_count == 1
    assert relay.cancelled == []
    assert wrapper.fetchall() == [[42]]


def test_504_while_job_still_running_falls_back_to_sleep_then_new_long_poll(monkeypatch):
    """When the quick status probe itself reports the job still running (a
    genuine ambiguity, not a lost response), the adapter must fall back to
    the original fixed sleep-then-retry loop instead of busy-looping.
    """
    clock = _Clock()
    relay = _AmbiguousPollRelay(
        [
            ("raise", 504),  # long poll's HTTP response lost
            ("running", None),  # quick probe: job is genuinely still running
            ("done", None),  # next full long poll: job has since finished
        ]
    )
    _use_clock(monkeypatch, clock)

    wrapper = PrivyConnectionWrapper(relay, _credentials(connect_timeout=10, connect_retries=1))
    wrapper.execute("select 1 as value")

    assert relay.poll_waits == [
        DEFAULT_POLL_WAIT_S,
        privysession._PRIVY_STATUS_PROBE_WAIT_S,
        DEFAULT_POLL_WAIT_S,
    ]
    assert clock.sleeps == [10]
    assert relay.submit_count == 1
    assert relay.cancelled == []
    assert wrapper.fetchall() == [[42]]


def test_504_when_quick_probe_also_fails_falls_back_to_sleep_then_new_long_poll(monkeypatch):
    """When the quick status probe itself hits a transient relay error (the
    relay is genuinely unreachable, not just slow to answer one poll), the
    adapter must fall back to the normal sleep-then-retry loop rather than
    treating the probe failure as fatal or looping without backoff.
    """
    clock = _Clock()
    relay = _AmbiguousPollRelay(
        [
            ("raise", 504),  # long poll's HTTP response lost
            ("raise", 503),  # the immediate status re-check also fails transiently
            ("done", None),  # next full long poll succeeds
        ]
    )
    _use_clock(monkeypatch, clock)

    wrapper = PrivyConnectionWrapper(relay, _credentials(connect_timeout=10, connect_retries=1))
    wrapper.execute("select 1 as value")

    assert relay.poll_waits == [
        DEFAULT_POLL_WAIT_S,
        privysession._PRIVY_STATUS_PROBE_WAIT_S,
        DEFAULT_POLL_WAIT_S,
    ]
    assert clock.sleeps == [10]
    assert relay.submit_count == 1
    assert relay.cancelled == []
    assert wrapper.fetchall() == [[42]]


def test_poll_exhausts_retries_when_quick_probe_never_resolves(monkeypatch):
    """The quick-probe fast path must not weaken the existing bounded-retry
    guarantee: if neither the long polls nor the quick probes ever resolve,
    ``connect_retries`` is still honored and the job is still cancelled
    exactly once, with no duplicate submission.
    """
    clock = _Clock()
    relay = _AmbiguousPollRelay(
        [
            ("raise", 504),  # real long poll #1
            ("raise", 504),  # quick probe #1 (also inconclusive)
            ("raise", 504),  # real long poll #2 (last allowed retry)
            ("raise", 504),  # quick probe #2 (also inconclusive) -> exhausted
        ]
    )
    _use_clock(monkeypatch, clock)

    wrapper = PrivyConnectionWrapper(relay, _credentials(connect_timeout=10, connect_retries=1))
    with pytest.raises(PrivyTransportRetryError):
        wrapper.execute("select 1 as value")

    assert relay.poll_waits == [
        DEFAULT_POLL_WAIT_S,
        privysession._PRIVY_STATUS_PROBE_WAIT_S,
        DEFAULT_POLL_WAIT_S,
        privysession._PRIVY_STATUS_PROBE_WAIT_S,
    ]
    assert clock.sleeps == [10]
    assert relay.submit_count == 1
    assert relay.cancelled == ["job-ambiguous"]


class _CancellableRelay:
    def __init__(self):
        self.poll_started = threading.Event()
        self.cancelled = threading.Event()
        self.cancel_calls = []

    def submit(self, request):
        self.request = request
        return "job-cancel"

    def poll(self, request, job_id, *, wait_s):
        del request, wait_s
        self.poll_started.set()
        self.cancelled.wait(timeout=10)
        return "cancelled", _exec_result(stderr="cancelled", ok=False)

    def cancel(self, request, job_id):
        assert request is self.request
        self.cancel_calls.append(job_id)
        self.cancelled.set()
        return _exec_result()


def test_cancel_targets_the_active_relay_job():
    relay = _CancellableRelay()
    wrapper = PrivyConnectionWrapper(relay, _credentials())
    errors = []

    def run():
        try:
            wrapper.execute("select 1")
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    assert relay.poll_started.wait(timeout=5)
    wrapper.cancel()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert relay.cancel_calls == ["job-cancel"]
    assert len(errors) == 1
    assert isinstance(errors[0], DbtDatabaseError)
    assert "cancelled" in str(errors[0])


class _UnavailableRelay:
    def __init__(self):
        self.submit_count = 0
        self.requests = []

    def submit(self, request):
        self.submit_count += 1
        self.requests.append(request)
        raise _http_error(404)

    def poll(self, request, job_id, *, wait_s):
        raise AssertionError((request, job_id, wait_s))

    def cancel(self, request, job_id):
        raise AssertionError((request, job_id))


def test_exhausted_relay_recovery_cannot_enter_outer_sql_retry_loop():
    relay = _UnavailableRelay()
    wrapper = PrivyConnectionWrapper(
        relay,
        _credentials(connect_retries=2),
    )

    with pytest.raises(PrivyTransportRetryError) as excinfo:
        wrapper.execute("create materialized view mv as select 1")

    assert relay.submit_count == 3
    assert len({id(request) for request in relay.requests}) == 1
    assert len({request.request_id for request in relay.requests}) == 1
    assert relay.requests[0].request_id
    assert "refusing to resubmit SQL" in str(excinfo.value)
    assert "relay.invalid" not in str(excinfo.value)
    assert _is_retryable_error(excinfo.value) == ""
    assert _is_permanent_error(excinfo.value) is True
