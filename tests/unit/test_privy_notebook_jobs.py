import datetime as dt

import pytest
import requests
from dbt_common.exceptions import DbtRuntimeError

import dbt.adapters.fabricspark.privysession as privysession
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials

WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
NOTEBOOK_ID = "22222222-2222-2222-2222-222222222222"
JOB_ID = "33333333-3333-3333-3333-333333333333"
OTHER_JOB_ID = "44444444-4444-4444-4444-444444444444"
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


@pytest.fixture(autouse=True)
def _isolate_job_cache(monkeypatch):
    privysession._job_refs_by_target.clear()
    monkeypatch.setattr(privysession, "_write_job_ref_file", lambda job_ref: None)
    monkeypatch.setattr(privysession, "_utc_now", lambda: NOW)
    yield
    privysession._job_refs_by_target.clear()


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
    token="campaign-central-frozen",
    status="InProgress",
    started=NOW - dt.timedelta(minutes=1),
    include_parameters=True,
):
    job = {
        "id": job_id,
        "itemId": NOTEBOOK_ID,
        "jobType": "RunNotebook",
        "status": status,
        "startTimeUtc": started.isoformat().replace("+00:00", "Z"),
    }
    if include_parameters:
        job["parameters"] = [
            {
                "name": "campaign_correlation_token",
                "value": token,
                "type": "Text",
            }
        ]
    return job


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


def test_recent_matching_active_job_is_reused_across_paginated_history(monkeypatch):
    credentials = _credentials()
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": [], "continuationToken": "next-page"}),
            _Response(payload={"value": [_job()]}),
        ],
    )

    job_ref = privysession._trigger_notebook_run(credentials)

    assert job_ref.job_instance_id == JOB_ID
    assert [method for method, _, _ in calls] == ["GET", "GET"]
    assert calls[1][2]["params"] == {"continuationToken": "next-page"}
    _assert_skill_header(calls)
    assert (
        privysession._read_cached_job_ref(
            WORKSPACE_ID,
            NOTEBOOK_ID,
            "campaign-central-frozen",
        )
        == job_ref
    )


def test_cached_token_association_reuses_history_that_omits_parameters(monkeypatch):
    credentials = _credentials()
    cached = privysession._NotebookJobRef(
        workspace_id=WORKSPACE_ID,
        notebook_id=NOTEBOOK_ID,
        job_instance_id=JOB_ID,
        correlation_token="campaign-central-frozen",
    )
    privysession._cache_job_ref(cached)
    calls = _install_requests(
        monkeypatch,
        [_Response(payload={"value": [_job(include_parameters=False)]})],
    )

    assert privysession._trigger_notebook_run(credentials) == cached
    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_multiple_recent_matching_jobs_fail_ambiguous_without_post(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(
                payload={
                    "value": [
                        _job(include_parameters=False),
                        _job(OTHER_JOB_ID, include_parameters=False),
                    ]
                }
            )
        ],
    )

    with pytest.raises(DbtRuntimeError, match="multiple active"):
        privysession._trigger_notebook_run(_credentials())

    assert [method for method, _, _ in calls] == ["GET"]
    _assert_skill_header(calls)


def test_successful_post_caches_returned_job_id_immediately(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=202,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances/{JOB_ID}"
                    )
                },
            ),
        ],
    )

    job_ref = privysession._trigger_notebook_run(_credentials())

    assert [method for method, _, _ in calls] == ["GET", "POST"]
    post_body = calls[1][2]["json"]
    assert post_body["parameters"]
    assert post_body != {}
    assert (
        privysession._read_cached_job_ref(
            WORKSPACE_ID,
            NOTEBOOK_ID,
            "campaign-central-frozen",
        )
        == job_ref
    )
    _assert_skill_header(calls)


def test_accepted_post_without_location_id_reconciles_instead_of_using_notebook_id(
    monkeypatch,
):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            _Response(
                status_code=202,
                headers={
                    "Location": (
                        "https://api.fabric.microsoft.com/v1/workspaces/"
                        f"{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances"
                        "?jobType=RunNotebook"
                    )
                },
            ),
            _Response(payload={"value": [_job(started=NOW, include_parameters=False)]}),
        ],
    )

    job_ref = privysession._trigger_notebook_run(_credentials())

    assert job_ref.job_instance_id == JOB_ID
    assert job_ref.job_instance_id != NOTEBOOK_ID
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_ambiguous_post_reconciles_once_without_retrying_post(monkeypatch):
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            requests.Timeout("ambiguous submit"),
            _Response(payload={"value": [_job(started=NOW, include_parameters=False)]}),
        ],
    )

    job_ref = privysession._trigger_notebook_run(_credentials())

    assert job_ref.job_instance_id == JOB_ID
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_ambiguous_post_history_retry_keeps_header_and_never_retries_post(monkeypatch):
    monkeypatch.setattr(privysession, "_JOB_RECONCILE_POLL_S", 0)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            requests.ConnectionError("ambiguous submit"),
            _Response(status_code=429, payload={"errorCode": "TooManyRequests"}),
            _Response(payload={"value": [_job(started=NOW, include_parameters=False)]}),
        ],
    )

    job_ref = privysession._trigger_notebook_run(_credentials())

    assert job_ref.job_instance_id == JOB_ID
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET", "GET"]
    assert sum(method == "POST" for method, _, _ in calls) == 1
    _assert_skill_header(calls)


def test_unreconciled_post_fails_without_chaining_secret_bearing_request(monkeypatch):
    monkeypatch.setattr(privysession, "_JOB_RECONCILE_TIMEOUT_S", 0)
    calls = _install_requests(
        monkeypatch,
        [
            _Response(payload={"value": []}),
            requests.Timeout("fake-relay-secret"),
            _Response(payload={"value": []}),
        ],
    )

    with pytest.raises(DbtRuntimeError) as exc_info:
        privysession._trigger_notebook_run(_credentials())

    assert exc_info.value.__context__ is None
    assert "fake-relay-secret" not in str(exc_info.value)
    assert [method for method, _, _ in calls] == ["GET", "POST", "GET"]
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
                    "status": "InProgress",
                    "parameters": [{"value": "fake-relay-secret"}],
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
