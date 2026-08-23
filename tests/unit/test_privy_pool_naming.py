"""Regression tests for Privy's model-stable Spark scheduler pool naming.

Complements ``test_privy_exec_snippet.py``/``test_privy_relay_recovery.py`` by
covering ``_scheduler_pool_for_node`` (and the opt-in
``privy_pool_priority_map`` credentials field it consumes) directly:

* retries of the same dbt model must always resolve to the identical pool
  name, so a pre-declared ``spark.scheduler.allocation.file`` weight keeps
  applying across a run;
* two different models must never silently collide on a truncated/derived
  pool name;
* an operator-supplied ``privy_pool_priority_map`` override must be honored
  verbatim, including deliberate pool-sharing across models, and must never
  be clobbered by an unrelated auto-derived name (or vice versa);
* statements without a dbt ``node_id`` keep the pre-existing, fully isolated
  per-request pool-naming behaviour unchanged.
"""

import pytest

import dbt.adapters.fabricspark.privysession as privysession_module
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.privysession import (
    PrivyConnectionWrapper,
    _build_exec_snippet,
    _model_key_for_node,
    _scheduler_pool_for_node,
    _scheduler_pool_for_request_id,
)


@pytest.fixture(autouse=True)
def _clean_pool_registry():
    """Start (and leave) every test with a blank pool-name registry.

    ``_pool_name_by_node``/``_pool_owner_by_name`` are intentionally
    process-wide/module-level in production -- a retry from a different
    thread or a fresh ``PrivyConnectionWrapper`` must still land in the same
    pool -- so tests isolate themselves explicitly instead of relying on
    every test picking never-before-seen node_id strings.
    """
    saved_by_node = dict(privysession_module._pool_name_by_node)
    saved_owner = dict(privysession_module._pool_owner_by_name)
    privysession_module._pool_name_by_node.clear()
    privysession_module._pool_owner_by_name.clear()
    try:
        yield
    finally:
        privysession_module._pool_name_by_node.clear()
        privysession_module._pool_name_by_node.update(saved_by_node)
        privysession_module._pool_owner_by_name.clear()
        privysession_module._pool_owner_by_name.update(saved_owner)


def _privy_credentials(**overrides):
    values = dict(
        method="privy",
        privy_relay_namespace="ns",
        privy_relay_path="path",
        privy_relay_keyrule="rule",
        privy_relay_key="key",
        privy_notebook_url=(
            "https://x.fabric.microsoft.com/groups/"
            "11111111-1111-1111-1111-111111111111/synapsenotebooks/"
            "22222222-2222-2222-2222-222222222222"
        ),
        spark_config={"name": "test"},
    )
    values.update(overrides)
    return FabricSparkCredentials(**values)


def test_model_key_for_node_strips_resource_type_and_package():
    assert _model_key_for_node("model.my_pkg.dim_customer") == "dim_customer"
    assert _model_key_for_node("model.my_pkg.dim_customer.v2") == "dim_customer.v2"


def test_model_key_for_node_falls_back_for_non_standard_shape():
    assert _model_key_for_node("not_a_dotted_id") == "not_a_dotted_id"
    assert _model_key_for_node("only.two") == "only.two"


def test_retry_of_same_model_reuses_the_same_pool():
    node_id = "model.pooltest.dim_customer"
    first = _scheduler_pool_for_node(node_id, "request-1")
    second = _scheduler_pool_for_node(node_id, "request-2")
    assert first == second == "privy_dim_customer"


def test_statement_without_node_id_keeps_full_per_request_isolation():
    pool_a = _scheduler_pool_for_node(None, "request-a")
    pool_b = _scheduler_pool_for_node(None, "request-b")
    assert pool_a != pool_b
    assert pool_a == _scheduler_pool_for_request_id("request-a")
    assert pool_b == _scheduler_pool_for_request_id("request-b")


def test_two_models_truncating_to_the_same_key_do_not_collide():
    common_prefix = "x" * 40
    node_a = f"model.pooltest.{common_prefix}_variant_a"
    node_b = f"model.pooltest.{common_prefix}_variant_b"

    pool_a = _scheduler_pool_for_node(node_a, "request-a")
    pool_b = _scheduler_pool_for_node(node_b, "request-b")

    assert pool_a != pool_b
    assert pool_a == f"privy_{common_prefix}"
    assert pool_b.startswith(f"privy_{common_prefix}_")

    # Retrying node_b resolves to the SAME disambiguated pool -- the hash
    # suffix is stable/cached, not re-derived (and re-suffixed) each time.
    assert _scheduler_pool_for_node(node_b, "request-c") == pool_b


def test_priority_map_override_is_honored_verbatim_by_full_node_id():
    node_id = "model.pooltest.fact_machine_status_monthly_snapshot"
    pool = _scheduler_pool_for_node(
        node_id,
        "request-1",
        {node_id: "critical_path_pool"},
    )
    assert pool == "critical_path_pool"


def test_priority_map_override_matches_by_bare_model_name_too():
    node_id = "model.my_project.int_machine_status_transaction"
    pool = _scheduler_pool_for_node(
        node_id,
        "request-1",
        {"int_machine_status_transaction": "critical_path_pool"},
    )
    assert pool == "critical_path_pool"


def test_priority_map_allows_deliberate_pool_sharing_across_models():
    node_a = "model.pooltest.shared_a"
    node_b = "model.pooltest.shared_b"
    pool_map = {node_a: "shared_pool", node_b: "shared_pool"}

    pool_a = _scheduler_pool_for_node(node_a, "request-a", pool_map)
    pool_b = _scheduler_pool_for_node(node_b, "request-b", pool_map)

    assert pool_a == pool_b == "shared_pool"


def test_auto_derived_name_is_disambiguated_from_a_prior_override():
    overridden_node = "model.pooltest.custom"
    pool_map = {overridden_node: "privy_custom"}
    overridden_pool = _scheduler_pool_for_node(overridden_node, "request-a", pool_map)
    assert overridden_pool == "privy_custom"

    # A second, unrelated model whose auto-derived name would collide with
    # that override must be disambiguated, not silently merged into it.
    colliding_node = "model.otherpkg.custom"
    colliding_pool = _scheduler_pool_for_node(colliding_node, "request-b", pool_map)
    assert colliding_pool != overridden_pool
    assert colliding_pool.startswith("privy_custom_")


def test_default_empty_priority_map_behaves_like_no_map():
    node_id = "model.pooltest.no_map_model"
    assert _scheduler_pool_for_node(node_id, "request-1", {}) == _scheduler_pool_for_node(
        node_id, "request-2", None
    )


def test_build_exec_snippet_embeds_model_stable_pool_across_retries():
    sql = '/* {"node_id": "model.pooltest.dim_customer_exec"} */ select 1'
    first = _build_exec_snippet(sql, "MARKER_1", request_id="retry-1")
    second = _build_exec_snippet(sql, "MARKER_2", request_id="retry-2")
    assert "'privy_dim_customer_exec'" in first
    assert "'privy_dim_customer_exec'" in second


def test_build_exec_snippet_honors_pool_priority_map():
    sql = '/* {"node_id": "model.pooltest.fact_priority"} */ select 1'
    code = _build_exec_snippet(
        sql,
        "MARKER",
        request_id="req-1",
        pool_priority_map={"fact_priority": "critical_pool"},
    )
    assert "'critical_pool'" in code


def test_build_exec_snippet_without_node_id_is_unaffected_by_priority_map():
    sql = "select 1"
    code = _build_exec_snippet(
        sql,
        "MARKER",
        request_id="req-no-node",
        pool_priority_map={"anything": "should_not_apply"},
    )
    assert "'should_not_apply'" not in code
    assert repr(_scheduler_pool_for_request_id("req-no-node")) in code


def test_wrapper_sources_pool_priority_map_from_credentials():
    credentials = _privy_credentials(privy_pool_priority_map={"dim_customer": "critical_pool"})
    wrapper = PrivyConnectionWrapper(relay_client=object(), credentials=credentials)
    assert wrapper._pool_priority_map == {"dim_customer": "critical_pool"}


def test_wrapper_defaults_to_empty_pool_priority_map():
    credentials = _privy_credentials()
    wrapper = PrivyConnectionWrapper(relay_client=object(), credentials=credentials)
    assert wrapper._pool_priority_map == {}


def test_credentials_reject_non_dict_priority_map():
    with pytest.raises(Exception, match="privy_pool_priority_map must be a mapping"):
        _privy_credentials(privy_pool_priority_map=["not", "a", "dict"])


def test_credentials_reject_invalid_pool_name_value():
    with pytest.raises(Exception, match="is not a valid Spark scheduler pool name"):
        _privy_credentials(privy_pool_priority_map={"dim_customer": "bad pool name!"})


def test_credentials_reject_empty_key_or_value():
    with pytest.raises(Exception, match="keys must be non-empty strings"):
        _privy_credentials(privy_pool_priority_map={"": "critical_pool"})
    with pytest.raises(Exception, match="must be a non-empty string"):
        _privy_credentials(privy_pool_priority_map={"dim_customer": ""})


def test_credentials_accept_valid_priority_map_and_expose_it():
    credentials = _privy_credentials(privy_pool_priority_map={"dim_customer": "critical_pool"})
    assert credentials.privy_pool_priority_map == {"dim_customer": "critical_pool"}
    assert "privy_pool_priority_map" in credentials._connection_keys()
    assert "privy_pool_priority_map={'dim_customer': 'critical_pool'}" in repr(credentials)


def test_credentials_default_priority_map_is_empty():
    credentials = _privy_credentials()
    assert credentials.privy_pool_priority_map == {}
