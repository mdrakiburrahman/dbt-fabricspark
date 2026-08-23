import json
import re
import threading
from types import SimpleNamespace

import pytest
import requests
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.fabricspark.connections import _is_permanent_error, _is_retryable_error
from dbt.adapters.fabricspark.privysession import (
    PrivyConnectionWrapper,
    PrivyTransportRetryError,
    _extract_marked_json,
    _scheduler_pool_for_request_id,
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
        self.scheduler_pools = set()

    def submit(self, request):
        marker_match = re.search(r"__PRIVY_RESULT_([0-9a-f]+)__", request.code)
        node_match = re.search(r"model\.relay\.value_(\d+)", request.code)
        assert marker_match is not None
        assert node_match is not None
        request_id = marker_match.group(1)
        marker = marker_match.group(0)
        index = int(node_match.group(1))
        pool = _scheduler_pool_for_request_id(request_id)
        assert repr(pool) in request.code
        with self._lock:
            self.submit_count += 1
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
    assert max(relay.poll_waits) <= 1.0
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


class _AmbiguousSubmitRelayServer:
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


def test_19_ambiguous_submit_retries_execute_materialization_once():
    relay = _AmbiguousSubmitRelayServer()
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

    def submit(self, request):
        del request
        self.submit_count += 1
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
    assert "refusing to resubmit SQL" in str(excinfo.value)
    assert "relay.invalid" not in str(excinfo.value)
    assert _is_retryable_error(excinfo.value) == ""
    assert _is_permanent_error(excinfo.value) is True
