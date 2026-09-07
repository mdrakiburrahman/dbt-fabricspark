import datetime as dt
import json
import multiprocessing
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from dbt_common.exceptions import DbtRuntimeError

import dbt.adapters.fabricspark.privysession as privysession
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials

WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
NOTEBOOK_ID = "22222222-2222-2222-2222-222222222222"
JOB_ID = "33333333-3333-3333-3333-333333333333"
OTHER_JOB_ID = "44444444-4444-4444-4444-444444444444"
OTHER_NOTEBOOK_ID = "55555555-5555-5555-5555-555555555555"
NOW = dt.datetime(2026, 9, 2, 3, 40, tzinfo=dt.timezone.utc)


class _Response:
    def __init__(self, status_code=200, *, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class _Logger:
    def __init__(self):
        self.messages = []

    def debug(self, message):
        self.messages.append(str(message))

    def info(self, message):
        self.messages.append(str(message))

    def warning(self, message):
        self.messages.append(str(message))


class _Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds):
        self.now += seconds


def _use_clock(monkeypatch, clock):
    monkeypatch.setattr(
        privysession,
        "time",
        SimpleNamespace(
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            time=lambda: clock.now,
            perf_counter=clock.monotonic,
        ),
    )


@pytest.fixture(autouse=True)
def _isolate_job_cache(monkeypatch):
    root = Path.cwd() / ".privy-job-test-state" / uuid.uuid4().hex
    root.mkdir(parents=True)
    cache_path = root / "privy-notebook-job.json"
    privysession._job_refs_by_target.clear()
    privysession._job_thread_locks.clear()
    monkeypatch.setattr(privysession, "_job_cache_path", lambda: str(cache_path))
    monkeypatch.setattr(privysession, "_utc_now", lambda: NOW)
    yield cache_path
    privysession._job_refs_by_target.clear()
    privysession._job_thread_locks.clear()
    shutil.rmtree(root, ignore_errors=True)
    try:
        root.parent.rmdir()
    except OSError:
        pass


def _credentials(**overrides):
    values = dict(
        method="privy",
        authentication="CLI",
        endpoint="https://api.fabric.microsoft.com/v1",
        privy_relay_namespace="fake-relay-namespace",
        privy_relay_path="fake-relay-path",
        privy_relay_keyrule="fake-relay-rule",
        privy_relay_key="fake-relay-secret",
        privy_notebook_url=(
            "https://app.fabric.microsoft.com/groups/"
            f"{WORKSPACE_ID}/synapsenotebooks/{NOTEBOOK_ID}"
        ),
        privy_max_workers=64,
        privy_serialize_inprocess=False,
        privy_campaign_correlation_token="campaign-central-frozen",
        spark_config={"name": "test"},
    )
    values.update(overrides)
    return FabricSparkCredentials(**values)


def _job(
    job_id=JOB_ID,
    *,
    notebook_id=NOTEBOOK_ID,
    status="InProgress",
    started=NOW - dt.timedelta(minutes=1),
):
    return {
        "id": job_id,
        "itemId": notebook_id,
        "jobType": "RunNotebook",
        "invokeType": "Manual",
        "status": status,
        "rootActivityId": job_id,
        "startTimeUtc": started.isoformat().replace("+00:00", "Z"),
        "endTimeUtc": None,
        "failureReason": None,
    }


def _install_requests(monkeypatch, responses):
    calls = []
    scripted = iter(responses)

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        response = next(scripted)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(privysession.requests, "request", request)
    monkeypatch.setattr(
        privysession,
        "get_headers",
        lambda credentials: {
            "Authorization": "Bearer fake-token",
            "x-ms-fabric-skill": "wrong-value",
        },
    )
    return calls


def _assert_skill_header(calls):
    assert calls
    for _, _, kwargs in calls:
        assert kwargs["headers"]["x-ms-fabric-skill"] == "spark-cli"


def _multiprocess_trigger_worker(
    cache_path,
    token,
    notebook_id,
    relay_path,
    job_id,
    start_event,
    state_lock,
    post_count,
    active,
    result_queue,
):
    privysession._job_refs_by_target.clear()
    privysession._job_thread_locks.clear()
    privysession._job_cache_path = lambda: cache_path
    privysession._utc_now = lambda: NOW
    privysession.get_headers = lambda credentials: {"Authorization": "******"}

    def request(method, url, **kwargs):
        del kwargs
        if method == "GET" and url.endswith("/jobs/instances"):
            with state_lock:
                jobs = [_job(job_id, notebook_id=notebook_id, started=NOW)] if active.value else []
            return _Response(payload={"value": jobs})
        if method == "POST":
            with state_lock:
                post_count.value += 1
                active.value = True
            return _Response(
                status_code=202,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{notebook_id}/jobs/instances/{job_id}"
                    )
                },
            )
        raise AssertionError((method, url))

    privysession.requests.request = request
    credentials = _credentials(
        privy_notebook_url=(
            "https://app.fabric.microsoft.com/groups/"
            f"{WORKSPACE_ID}/synapsenotebooks/{notebook_id}"
        ),
        privy_relay_path=relay_path,
        privy_campaign_correlation_token=token,
    )
    start_event.wait(timeout=10)
    try:
        job_ref = privysession._trigger_notebook_run(credentials)
        result_queue.put(("ok", token, job_ref.job_instance_id))
    except Exception as exc:  # noqa: BLE001 - returned to the parent test process
        result_queue.put(("error", token, type(exc).__name__, str(exc)))


def _hold_mapping_lock(cache_path, acquired_event, release_event):
    privysession._job_thread_locks.clear()
    privysession._job_cache_path = lambda: cache_path
    with privysession._job_cache_mapping_lock():
        acquired_event.set()
        release_event.wait(timeout=20)


def _fork_context():
    try:
        return multiprocessing.get_context("fork")
    except ValueError:
        pytest.skip("POSIX fork is required for interprocess lock coverage")


def test_notebook_parameter_payload_uses_existing_credentials_and_typed_settings():
    credentials = _credentials()

    body = privysession._notebook_run_body(
        credentials,
        correlation_token="campaign-central-frozen",
    )

    assert body == {
        "parameters": [
            {"name": "relay_namespace", "value": "fake-relay-namespace", "type": "Text"},
            {"name": "relay_path", "value": "fake-relay-path", "type": "Text"},
            {"name": "relay_key_rule", "value": "fake-relay-rule", "type": "Text"},
            {"name": "relay_key", "value": "fake-relay-secret", "type": "Text"},
            {"name": "max_workers", "value": 64, "type": "Integer"},
            {"name": "listener_connections", "value": 25, "type": "Integer"},
            {"name": "serialize_inprocess", "value": False, "type": "Boolean"},
            {
                "name": "campaign_correlation_token",
                "value": "campaign-central-frozen",
                "type": "Text",
            },
        ]
    }


def test_default_correlation_token_is_stable_and_nonsecret():
    credentials = _credentials(privy_campaign_correlation_token=None)

    first = privysession._resolve_job_correlation_token(
        credentials,
        WORKSPACE_ID,
        NOTEBOOK_ID,
    )
    second = privysession._resolve_job_correlation_token(
        credentials,
        WORKSPACE_ID,
        NOTEBOOK_ID,
    )

    assert first == second
    assert first.startswith("dbt-fabricspark-")
    assert "fake-relay" not in first
    assert "secret" not in first


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"privy_max_workers": 0}, "privy_max_workers"),
        ({"privy_max_workers": True}, "privy_max_workers"),
        ({"privy_serialize_inprocess": "false"}, "privy_serialize_inprocess"),
        ({"privy_campaign_correlation_token": ""}, "privy_campaign_correlation_token"),
    ],
)
def test_privy_job_settings_are_typed_and_validated(overrides, expected):
    with pytest.raises(DbtRuntimeError, match=expected):
        _credentials(**overrides)


def test_credentials_repr_redacts_all_relay_credential_values():
    credentials = _credentials()

    rendered = repr(credentials)

    for value in (
        "fake-relay-namespace",
        "fake-relay-path",
        "fake-relay-rule",
        "fake-relay-secret",
    ):
        assert value not in rendered
    assert rendered.count("'***'") >= 6
    assert "campaign-central-frozen" in rendered
    connection_keys = credentials._connection_keys()
    for field_name in (
        "privy_relay_namespace",
        "privy_relay_path",
        "privy_relay_keyrule",
        "privy_relay_key",
    ):
        assert field_name not in connection_keys
    for field_name in (
        "privy_max_workers",
        "privy_serialize_inprocess",
        "privy_campaign_correlation_token",
    ):
        assert field_name in connection_keys


def test_owned_active_job_is_reused_across_paginated_documented_history(monkeypatch):
    credentials = _credentials()
    target_id = privysession._job_target_id(credentials)
    cached = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=NOTEBOOK_ID,
        target_id=target_id,
        job_instance_id=JOB_ID,
        correlation_token="campaign-central-frozen",
    )
    privysession._cache_job_ref(cached)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": [], "continuationToken": "next-page"}),
            _Response(payload={"value": [_job()]}),
        ],
    )

    job_ref = privysession._trigger_notebook_run(credentials)

    assert job_ref == cached
    assert [method for method, _, _ in calls] == ["GET", "GET"]
    assert calls[1][2]["params"] == {"continuationToken": "next-page"}
    _assert_skill_header(calls)
    assert (
        privysession._read_cached_job_ref(
            WORKSPACE_ID,
            NOTEBOOK_ID,
            target_id,
        )
        == job_ref
    )


def test_active_job_after_cache_loss_is_never_adopted(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [_Response(payload={"value": [_job()]})],
    )

    with pytest.raises(DbtRuntimeError, match="no local ownership record"):
        privysession._trigger_notebook_run(_credentials())

    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_active_job_owned_by_another_campaign_is_never_reused(monkeypatch):
    owner_credentials = _credentials(privy_campaign_correlation_token="campaign-owner")
    target_id = privysession._job_target_id(owner_credentials)
    privysession._cache_job_ref(
        privysession._NotebookJobRef(
            workspace_id=WORKSPACE_ID,
            notebook_id=NOTEBOOK_ID,
            target_id=target_id,
            job_instance_id=JOB_ID,
            correlation_token="campaign-owner",
        )
    )
    calls = _install_requests(
        monkeypatch,
        [_Response(payload={"value": [_job()]})],
    )

    with pytest.raises(DbtRuntimeError, match="different campaign token"):
        privysession._trigger_notebook_run(
            _credentials(privy_campaign_correlation_token="campaign-contender")
        )

    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_multiple_active_jobs_fail_singleton_ambiguity_without_post(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [_Response(payload={"value": [_job(), _job(OTHER_JOB_ID)]})],
    )

    with pytest.raises(DbtRuntimeError, match="multiple active"):
        privysession._trigger_notebook_run(_credentials())

    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_atomic_keyed_cache_clear_removes_only_the_affected_target():
    first_credentials = _credentials()
    second_credentials = _credentials(
        privy_notebook_url=(
            "https://app.fabric.microsoft.com/groups/"
            f"{WORKSPACE_ID}/synapsenotebooks/{OTHER_NOTEBOOK_ID}"
        ),
        privy_relay_path="other-relay-path",
        privy_campaign_correlation_token="other-campaign",
    )
    first = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=NOTEBOOK_ID,
        target_id=privysession._job_target_id(first_credentials),
        job_instance_id=JOB_ID,
        correlation_token="campaign-central-frozen",
    )
    second = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=OTHER_NOTEBOOK_ID,
        target_id=privysession._job_target_id(second_credentials),
        job_instance_id=OTHER_JOB_ID,
        correlation_token="other-campaign",
    )

    privysession._cache_job_ref(first)
    privysession._cache_job_ref(second)
    privysession._clear_cached_job_ref(first)

    assert (
        privysession._read_cached_job_ref(
            first.workspace_id,
            first.notebook_id,
            first.target_id,
        )
        is None
    )
    assert (
        privysession._read_cached_job_ref(
            second.workspace_id,
            second.notebook_id,
            second.target_id,
        )
        == second
    )


def test_successful_post_caches_returned_job_id_immediately(monkeypatch):
    credentials = _credentials()
    target_id = privysession._job_target_id(credentials)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=202,
                payload={"id": JOB_ID},
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances/{JOB_ID}"
                    )
                },
            ),
        ],
    )

    job_ref = privysession._trigger_notebook_run(credentials)

    assert [method for method, _, _ in calls] == ["GET", "POST"]
    post_body = calls[1][2]["json"]
    assert post_body["parameters"]
    assert post_body != {}
    assert (
        privysession._read_cached_job_ref(
            WORKSPACE_ID,
            NOTEBOOK_ID,
            target_id,
        )
        == job_ref
    )
    _assert_skill_header(calls)


def test_accepted_post_without_location_id_fails_without_timing_only_adoption(monkeypatch):
    monkeypatch.setattr(privysession, "_JOB_RECONCILE_TIMEOUT_S", 0)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=202,
                payload={"id": JOB_ID},
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances"
                        "?jobType=RunNotebook"
                    )
                },
            ),
        ],
    )

    with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
        privysession._trigger_notebook_run(_credentials())

    assert exc_info.value.evidence["location_job_instance_id"] is None
    assert exc_info.value.evidence["post_retried"] is False
    assert exc_info.value.evidence["cancel_attempted"] is False
    assert [method for method, _, _ in calls] == ["GET", "POST"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_ambiguous_post_never_adopts_unrelated_then_later_submitted_timing_matches(
    monkeypatch,
):
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    monkeypatch.setattr(privysession, "_JOB_RECONCILE_TIMEOUT_S", 2)
    credentials = _credentials()
    target_id = privysession._job_target_id(credentials)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            requests.Timeout("ambiguous submit"),
            _Response(payload={"value": [_job(OTHER_JOB_ID, started=NOW)]}),
            _Response(
                payload={
                    "value": [
                        _job(OTHER_JOB_ID, started=NOW),
                        _job(JOB_ID, started=NOW),
                    ]
                }
            ),
        ],
    )

    with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
        privysession._trigger_notebook_run(credentials)

    observed_ids = {job["job_instance_id"] for job in exc_info.value.evidence["observed_new_jobs"]}
    assert observed_ids == {JOB_ID, OTHER_JOB_ID}
    assert privysession._read_cached_job_ref(WORKSPACE_ID, NOTEBOOK_ID, target_id) is None
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET", "GET"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    assert clock.sleeps == [1, 1]
    _assert_skill_header(calls)


def test_ambiguous_http_response_with_location_reconciles_only_that_exact_job(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=503,
                payload={"errorCode": "TransientFailure"},
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances/{JOB_ID}"
                    ),
                    "x-ms-request-id": OTHER_JOB_ID,
                },
            ),
            _Response(payload=_job(started=NOW)),
        ],
    )

    job_ref = privysession._trigger_notebook_run(_credentials())

    assert job_ref.job_instance_id == JOB_ID
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_location_detail_mismatch_returns_only_structured_server_identifiers(monkeypatch):
    credentials = _credentials()
    target_id = privysession._job_target_id(credentials)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=503,
                payload={"message": "fake-relay-secret"},
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances/{JOB_ID}"
                    ),
                    "x-ms-request-id": OTHER_JOB_ID,
                },
            ),
            _Response(payload=_job(notebook_id=OTHER_NOTEBOOK_ID, started=NOW)),
        ],
    )

    with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
        privysession._trigger_notebook_run(credentials)

    evidence = exc_info.value.evidence
    assert evidence["server_request_id"] == OTHER_JOB_ID
    assert evidence["location_job_instance_id"] == JOB_ID
    assert evidence["location_job_detail"]["root_activity_id"] == JOB_ID
    assert evidence["location_validation"] == "mismatch"
    assert "fake-relay-secret" not in str(exc_info.value)
    assert privysession._read_cached_job_ref(WORKSPACE_ID, NOTEBOOK_ID, target_id) is None
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
    _assert_skill_header(calls)


def test_location_detail_near_deadline_cannot_start_fresh_mapping_lock_budget(
    monkeypatch,
    _isolate_job_cache,
):
    context = _fork_context()
    acquired_event = context.Event()
    release_event = context.Event()
    holder = context.Process(
        target=_hold_mapping_lock,
        args=(str(_isolate_job_cache), acquired_event, release_event),
    )

    clock = _Clock()
    _use_clock(monkeypatch, clock)
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if len(calls) == 1:
            return _Response(payload={"value": []})
        if len(calls) == 2:
            return _Response(
                status_code=503,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances/{JOB_ID}"
                    )
                },
            )
        assert kwargs["timeout"] == pytest.approx(30)
        holder.start()
        assert acquired_event.wait(timeout=10)
        clock.advance(29.5)
        return _Response(payload=_job(started=NOW))

    monkeypatch.setattr(privysession.requests, "request", request)
    monkeypatch.setattr(
        privysession,
        "get_headers",
        lambda credentials: {"Authorization": "******"},
    )
    credentials = _credentials()
    target_id = privysession._job_target_id(credentials)
    try:
        with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
            privysession._trigger_notebook_run(credentials)
    finally:
        release_event.set()
        if holder.pid is not None:
            holder.join(timeout=10)
    assert holder.exitcode == 0

    evidence = exc_info.value.evidence
    assert evidence["cache_persistence"] == "budget_exhausted"
    assert evidence["cache_budget_stage"] == "interprocess lock acquisition"
    assert evidence["elapsed_seconds"] == pytest.approx(30)
    assert clock.now == pytest.approx(130)
    assert privysession._read_cached_job_ref(WORKSPACE_ID, NOTEBOOK_ID, target_id) is None
    with privysession._notebook_scope_lock(
        WORKSPACE_ID,
        NOTEBOOK_ID,
        deadline=clock.now + 1,
    ):
        with privysession._notebook_job_lock(
            WORKSPACE_ID,
            NOTEBOOK_ID,
            target_id,
            deadline=clock.now + 1,
        ):
            pass
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
    _assert_skill_header(calls)


def test_cache_write_budget_is_rechecked_before_fsync(monkeypatch, _isolate_job_cache):
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    original_dump = privysession.json.dump

    def slow_dump(*args, **kwargs):
        original_dump(*args, **kwargs)
        clock.advance(1)

    monkeypatch.setattr(privysession.json, "dump", slow_dump)
    job_ref = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=NOTEBOOK_ID,
        target_id=privysession._job_target_id(_credentials()),
        job_instance_id=JOB_ID,
        correlation_token="campaign-central-frozen",
    )

    with pytest.raises(privysession._PrivyOwnershipBudgetExceeded) as exc_info:
        privysession._cache_job_ref(job_ref, deadline=clock.now + 1)

    assert exc_info.value.stage == "cache fsync"
    assert not _isolate_job_cache.exists()
    assert list(_isolate_job_cache.parent.glob("*.tmp-*")) == []


def test_cache_write_budget_is_rechecked_before_replace(monkeypatch, _isolate_job_cache):
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    original_fsync = privysession.os.fsync

    def slow_fsync(fd):
        original_fsync(fd)
        clock.advance(1)

    monkeypatch.setattr(privysession.os, "fsync", slow_fsync)
    monkeypatch.setattr(
        privysession.os,
        "replace",
        lambda *args: pytest.fail("replace must not run after the budget expires"),
    )
    job_ref = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=NOTEBOOK_ID,
        target_id=privysession._job_target_id(_credentials()),
        job_instance_id=JOB_ID,
        correlation_token="campaign-central-frozen",
    )

    with pytest.raises(privysession._PrivyOwnershipBudgetExceeded) as exc_info:
        privysession._cache_job_ref(job_ref, deadline=clock.now + 1)

    assert exc_info.value.stage == "cache replace"
    assert not _isolate_job_cache.exists()
    assert list(_isolate_job_cache.parent.glob("*.tmp-*")) == []


@pytest.mark.parametrize("status_code", [408, 500, 502, 503, 504])
def test_ambiguous_submit_http_status_without_location_fails_unattributable(
    monkeypatch,
    status_code,
):
    monkeypatch.setattr(privysession, "_JOB_RECONCILE_TIMEOUT_S", 0)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(status_code=status_code, payload={"errorCode": "TransientFailure"}),
        ],
    )

    with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
        privysession._trigger_notebook_run(_credentials())

    assert exc_info.value.evidence["ambiguity_kind"] == f"HTTP {status_code}"
    assert exc_info.value.evidence["location_job_instance_id"] is None
    assert [method for method, _, _ in calls] == ["GET", "POST"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_stalled_first_reconciliation_page_consumes_only_remaining_budget_and_unlocks(
    monkeypatch,
):
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if len(calls) == 1:
            return _Response(payload={"value": []})
        if len(calls) == 2:
            raise requests.Timeout("ambiguous submit")
        assert method == "GET"
        assert kwargs["timeout"] == pytest.approx(30)
        clock.advance(kwargs["timeout"])
        raise requests.Timeout("stalled history page")

    monkeypatch.setattr(privysession.requests, "request", request)
    monkeypatch.setattr(
        privysession,
        "get_headers",
        lambda credentials: {"Authorization": "******"},
    )
    credentials = _credentials()

    with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
        privysession._trigger_notebook_run(credentials)

    assert exc_info.value.evidence["last_history_error"] == "Timeout"
    assert exc_info.value.evidence["elapsed_seconds"] == pytest.approx(30)
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
    _assert_skill_header(calls)
    target_id = privysession._job_target_id(credentials)
    with privysession._notebook_scope_lock(WORKSPACE_ID, NOTEBOOK_ID):
        with privysession._notebook_job_lock(WORKSPACE_ID, NOTEBOOK_ID, target_id):
            pass


def test_slow_history_pagination_caps_each_page_by_remaining_budget(monkeypatch):
    clock = _Clock()
    _use_clock(monkeypatch, clock)
    calls = []

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if len(calls) == 1:
            assert kwargs["timeout"] == pytest.approx(30)
            clock.advance(20)
            return _Response(
                payload={
                    "value": [],
                    "continuationToken": "next-page",
                }
            )
        assert kwargs["timeout"] == pytest.approx(10)
        clock.advance(11)
        return _Response(payload={"value": []})

    monkeypatch.setattr(privysession.requests, "request", request)
    monkeypatch.setattr(
        privysession,
        "get_headers",
        lambda credentials: {"Authorization": "******"},
    )

    with pytest.raises(DbtRuntimeError, match="expired after a page"):
        privysession._list_item_job_instances(
            _credentials(),
            WORKSPACE_ID,
            NOTEBOOK_ID,
            timeout_s=30,
        )

    assert [call[2]["timeout"] for call in calls] == pytest.approx([30, 10])
    assert calls[1][2]["params"] == {"continuationToken": "next-page"}
    _assert_skill_header(calls)


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 409, 429])
def test_deterministic_submit_http_errors_fail_closed_without_reconciliation(
    monkeypatch,
    status_code,
):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(status_code=status_code, payload={"errorCode": "DeterministicFailure"}),
        ],
    )

    with pytest.raises(DbtRuntimeError, match="parameterless fallback was not attempted"):
        privysession._trigger_notebook_run(_credentials())

    assert [method for method, _, _ in calls] == ["GET", "POST"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_unreconciled_post_fails_without_chaining_secret_bearing_request(monkeypatch):
    monkeypatch.setattr(privysession, "_JOB_RECONCILE_TIMEOUT_S", 0)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            requests.Timeout("fake-relay-secret"),
        ],
    )

    with pytest.raises(privysession.PrivyNotebookSubmissionAmbiguousError) as exc_info:
        privysession._trigger_notebook_run(_credentials())

    assert exc_info.value.__context__ is None
    assert "fake-relay-secret" not in str(exc_info.value)
    assert exc_info.value.evidence["observed_new_jobs"] == []
    assert [method for method, _, _ in calls] == ["GET", "POST"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_parameter_rejection_fails_closed_without_body_or_secret_leak(monkeypatch):
    logger = _Logger()
    monkeypatch.setattr(privysession, "logger", logger)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=400,
                payload={
                    "errorCode": "FeatureNotAvailable",
                    "message": "rejected fake-relay-secret",
                },
                text='{"parameters":[{"value":"fake-relay-secret"}]}',
            ),
        ],
    )

    with pytest.raises(DbtRuntimeError) as exc_info:
        privysession._trigger_notebook_run(_credentials())

    message = str(exc_info.value)
    assert "FeatureNotAvailable" in message
    assert "parameterless fallback was not attempted" in message
    assert "fake-relay-secret" not in message
    assert "parameters" not in message
    assert [method for method, _, _ in calls] == ["GET", "POST"]
    assert calls[1][2]["json"]["parameters"]
    assert calls[1][2]["json"] != {}
    assert "fake-relay-secret" not in "\n".join(logger.messages)
    _assert_skill_header(calls)


def test_status_poll_uses_mandatory_header_without_logging_response_body(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(
                payload={
                    "id": JOB_ID,
                    "itemId": NOTEBOOK_ID,
                    "jobType": "RunNotebook",
                    "invokeType": "Manual",
                    "status": "InProgress",
                    "rootActivityId": JOB_ID,
                    "startTimeUtc": NOW.isoformat().replace("+00:00", "Z"),
                    "endTimeUtc": None,
                    "failureReason": None,
                }
            )
        ],
    )

    result = privysession._get_job_instance_status(
        _credentials(),
        WORKSPACE_ID,
        NOTEBOOK_ID,
        JOB_ID,
    )

    assert result["status"] == "InProgress"
    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_multiprocess_concurrent_campaigns_submit_once_and_never_cross_adopt(
    _isolate_job_cache,
):
    context = _fork_context()
    start_event = context.Event()
    state_lock = context.Lock()
    post_count = context.Value("i", 0)
    active = context.Value("b", False)
    result_queue = context.Queue()
    args = (
        str(_isolate_job_cache),
        NOTEBOOK_ID,
        "fake-relay-path",
        JOB_ID,
        start_event,
        state_lock,
        post_count,
        active,
        result_queue,
    )
    processes = [
        context.Process(
            target=_multiprocess_trigger_worker,
            args=(args[0], token, *args[1:]),
        )
        for token in ("campaign-a", "campaign-b")
    ]

    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0

    results = [result_queue.get(timeout=5) for _ in processes]
    assert post_count.value == 1
    assert sum(result[0] == "ok" for result in results) == 1
    errors = [result for result in results if result[0] == "error"]
    assert len(errors) == 1
    assert "different campaign token" in errors[0][3]


def test_multiprocess_concurrent_targets_preserve_both_atomic_mapping_entries(
    _isolate_job_cache,
):
    context = _fork_context()
    start_event = context.Event()
    result_queue = context.Queue()
    states = [
        (context.Lock(), context.Value("i", 0), context.Value("b", False)),
        (context.Lock(), context.Value("i", 0), context.Value("b", False)),
    ]
    specifications = [
        ("campaign-a", NOTEBOOK_ID, "relay-a", JOB_ID),
        ("campaign-b", OTHER_NOTEBOOK_ID, "relay-b", OTHER_JOB_ID),
    ]
    processes = []
    for specification, state in zip(specifications, states):
        token, notebook_id, relay_path, job_id = specification
        state_lock, post_count, active = state
        processes.append(
            context.Process(
                target=_multiprocess_trigger_worker,
                args=(
                    str(_isolate_job_cache),
                    token,
                    notebook_id,
                    relay_path,
                    job_id,
                    start_event,
                    state_lock,
                    post_count,
                    active,
                    result_queue,
                ),
            )
        )

    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0

    results = [result_queue.get(timeout=5) for _ in processes]
    assert all(result[0] == "ok" for result in results)
    assert [state[1].value for state in states] == [1, 1]
    with open(_isolate_job_cache) as f:
        mapping = json.load(f)
    assert mapping["version"] == 2
    assert len(mapping["entries"]) == 2


def test_auto_start_does_not_adopt_an_unowned_responding_relay(monkeypatch):
    monkeypatch.setattr(privysession, "_build_relay_client", lambda *args, **kwargs: object())
    monkeypatch.setattr(privysession, "_probe", lambda client: True)
    calls = _install_requests(
        monkeypatch,
        [_Response(payload={"value": [_job()]})],
    )

    with pytest.raises(DbtRuntimeError, match="no local ownership record"):
        privysession._ensure_notebook_ready(object(), _credentials())

    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_auto_start_reuses_a_responding_relay_only_for_its_owner(monkeypatch):
    credentials = _credentials()
    owned = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=NOTEBOOK_ID,
        target_id=privysession._job_target_id(credentials),
        job_instance_id=JOB_ID,
        correlation_token="campaign-central-frozen",
    )
    privysession._cache_job_ref(owned)
    monkeypatch.setattr(privysession, "_build_relay_client", lambda *args, **kwargs: object())
    monkeypatch.setattr(privysession, "_probe", lambda client: True)
    calls = _install_requests(
        monkeypatch,
        [_Response(payload={"value": [_job()]})],
    )

    privysession._ensure_notebook_ready(object(), credentials)

    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_non_auto_start_mode_never_calls_job_scheduler(monkeypatch):
    credentials = _credentials(privy_auto_start_notebook=False)
    monkeypatch.setattr(privysession, "_build_relay_client", lambda *args, **kwargs: object())
    monkeypatch.setattr(privysession, "_probe", lambda client: False)
    observed = {}

    def wait_for_relay(probe_client, actual_credentials, job_ref):
        observed["credentials"] = actual_credentials
        observed["job_ref"] = job_ref

    monkeypatch.setattr(privysession, "_wait_for_relay", wait_for_relay)
    monkeypatch.setattr(
        privysession.requests,
        "request",
        lambda *args, **kwargs: pytest.fail("Job Scheduler must not be called"),
    )

    privysession._ensure_notebook_ready(object(), credentials)

    assert observed == {"credentials": credentials, "job_ref": None}
