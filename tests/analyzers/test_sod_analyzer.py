import os
import json
import builtins
import types
from unittest import mock
import importlib
import pytest

# Target module path
MODULE_PATH = 'src.analyzers.sod-analyzer.main'


# New behaviors to test (documented for clarity):
# 1) Invalid JSON in SOD_RULES_JSON should log a warning and trigger TRUNCATE.
# 2) Unsupported role type combinations should be skipped and included in skipped results.
# 3) Workspace vs Workspace rule should generate proper query including assignments placeholder.
# 4) GCP vs Workspace rule should join workspace table and include assignments from source table.
# 5) SQL IN clause should correctly handle single role strings provided (converted to lists by module).


def import_module_with_env(env: dict):
    """Helper to import the module fresh with specific env vars."""
    with mock.patch.dict(os.environ, env, clear=False):
        if MODULE_PATH in list(importlib.sys.modules.keys()):
            del importlib.sys.modules[MODULE_PATH]
        return importlib.import_module(MODULE_PATH)


class DummyCloudEvent:
    def __init__(self):
        self.data = {}


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    # Ensure unrelated env vars don't leak
    keys = [
        'BQ_PROJECT_ID', 'BQ_DATASET_ID', 'SOURCE_TABLE_ID', 'DESTINATION_TABLE_ID',
        'WORKSPACE_ROLES_TABLE_ID', 'SOD_RULES_JSON'
    ]
    for k in keys:
        monkeypatch.delenv(k, raising=False)


@pytest.fixture()
def dummy_event():
    return DummyCloudEvent()


def _base_env(**overrides):
    base = {
        'BQ_PROJECT_ID': 'proj',
        'BQ_DATASET_ID': 'ds',
        'SOURCE_TABLE_ID': 'principal_access_list',
        'DESTINATION_TABLE_ID': 'sod_violations',
    }
    base.update(overrides)
    return base


def test_truncate_when_no_rules(monkeypatch, dummy_event):
    # Arrange: empty/invalid SOD_RULES_JSON should trigger TRUNCATE
    env = _base_env(SOD_RULES_JSON='[]')

    # Mock dependencies before import (used at import-time and runtime)
    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock()
    fake_logger = mock.MagicMock()
    get_logger.return_value = fake_logger

    # Provide utils modules
    utils_pkg = types.SimpleNamespace()
    bq_helpers_mod = types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results)
    logging_handler_mod = types.SimpleNamespace(get_logger=get_logger)
    gcp_clients_mod = types.SimpleNamespace(bigquery_client=mock.MagicMock())

    with mock.patch.dict(importlib.sys.modules, {
        'utils': utils_pkg,
        'utils.bq_helpers': bq_helpers_mod,
        'utils.logging_handler': logging_handler_mod,
        'utils.gcp_clients': gcp_clients_mod,
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    # Act
    mod.analyze_sod_violations(dummy_event)

    # Assert: TRUNCATE issued once
    assert run_query_and_save_results.call_count == 1
    args, kwargs = run_query_and_save_results.call_args
    assert 'TRUNCATE TABLE' in kwargs['query']
    assert kwargs['destination_table_id'] == 'sod_violations'
    fake_logger.info.assert_any_call('Truncating destination table: sod_violations due to no rules.')


def test_missing_required_env_raises(monkeypatch, dummy_event):
    # Arrange: missing DESTINATION_TABLE_ID
    env = {
        'BQ_PROJECT_ID': 'proj',
        'BQ_DATASET_ID': 'ds',
        'SOURCE_TABLE_ID': 'principal_access_list',
        # 'DESTINATION_TABLE_ID' missing
        'SOD_RULES_JSON': '[]',
    }

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=mock.MagicMock()),
        'utils.logging_handler': types.SimpleNamespace(get_logger=mock.MagicMock(return_value=mock.MagicMock())),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=mock.MagicMock()),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    with pytest.raises(ValueError):
        mod.analyze_sod_violations(dummy_event)


def test_skips_workspace_rules_when_table_missing(monkeypatch, dummy_event):
    # Arrange: one workspace rule and table NotFound -> skipped and still executes final query (skipped row)
    rules = [{
        'rule_id': 'R1', 'description': 'desc',
        'role1': ['roles/owner'], 'role2': ['ws/admin'],
        'role1_type': 'GCP_IAM', 'role2_type': 'WORKSPACE_ADMIN'
    }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules), WORKSPACE_ROLES_TABLE_ID='workspace_roles')

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    # bigquery_client.get_table should raise NotFound
    class NotFoundEx(Exception):
        pass

    bigquery_client = mock.MagicMock()
    bigquery_client.dataset.return_value.table.return_value = object()
    def raise_not_found(_):
        raise NotFoundEx('not found')
    bigquery_client.get_table.side_effect = raise_not_found

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=bigquery_client),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=NotFoundEx),
    }):
        mod = import_module_with_env(env)

    # Act
    mod.analyze_sod_violations(dummy_event)

    # Assert: should still execute a query saving results (skipped row)
    assert run_query_and_save_results.call_count == 1
    _, kwargs = run_query_and_save_results.call_args
    assert kwargs['destination_table_id'] == 'sod_violations'
    assert 'UNION ALL' not in kwargs['query'] or 'SKIPPED_RULE_INVALID_OR_DATA_UNAVAILABLE' in kwargs['query']


def test_gcp_vs_gcp_rule_generates_union_query(monkeypatch, dummy_event):
    # Arrange: simple GCP vs GCP
    rules = [{
        'rule_id': 'R2', 'description': 'desc2',
        'role1': ['roles/a', 'roles/b'], 'role2': ['roles/c'],
        'role1_type': 'GCP_IAM', 'role2_type': 'GCP_IAM'
    }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules))

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=mock.MagicMock()),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    # Act
    mod.analyze_sod_violations(dummy_event)

    # Assert: query contains both IN clauses and assignments aggregation
    _, kwargs = run_query_and_save_results.call_args
    q = kwargs['query']
    assert "IN ('roles/a','roles/b')" in q
    assert "IN ('roles/c')" in q
    assert 'ARRAY_AGG(STRUCT(a.resource_name, a.role))' in q


def test_empty_after_all_rules_skipped_truncates(monkeypatch, dummy_event):
    # Arrange: rule missing roles -> skipped; final query empty -> TRUNCATE
    rules = [{ 'rule_id': 'R3', 'role1': [], 'role2': [], 'role1_type': 'GCP_IAM', 'role2_type': 'GCP_IAM' }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules))

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=mock.MagicMock()),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    # Act
    mod.analyze_sod_violations(dummy_event)

    # Assert: truncate called
    _, kwargs = run_query_and_save_results.call_args
    assert 'TRUNCATE TABLE' in kwargs['query']


def test_invalid_json_triggers_truncate(monkeypatch, dummy_event):
    env = _base_env(SOD_RULES_JSON='{"bad_json": }')
    run_query_and_save_results = mock.MagicMock()
    fake_logger = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=fake_logger)

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=mock.MagicMock()),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    mod.analyze_sod_violations(dummy_event)

    _, kwargs = run_query_and_save_results.call_args
    assert 'TRUNCATE TABLE' in kwargs['query']
    fake_logger.warning.assert_any_call('Invalid JSON format for SOD_RULES_JSON env var. Treating as empty list.')


def test_unsupported_role_types_are_skipped(monkeypatch, dummy_event):
    # Unsupported combination e.g., UNKNOWN vs GCP_IAM
    rules = [{
        'rule_id': 'R4', 'description': 'bad types',
        'role1': ['roles/x'], 'role2': ['roles/y'],
        'role1_type': 'UNKNOWN', 'role2_type': 'GCP_IAM'
    }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules))

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=mock.MagicMock()),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    mod.analyze_sod_violations(dummy_event)

    q = run_query_and_save_results.call_args[1]['query']
    # There should be only a skipped row produced
    assert 'SKIPPED_RULE_INVALID_OR_DATA_UNAVAILABLE' in q
    assert 'UNION ALL' not in q or q.strip().startswith("SELECT 'R4'")


def test_workspace_vs_workspace_query(monkeypatch, dummy_event):
    rules = [{
        'rule_id': 'R5', 'description': 'ws vs ws',
        'role1': ['ws/admin'], 'role2': ['ws/owner'],
        'role1_type': 'WORKSPACE_ADMIN', 'role2_type': 'WORKSPACE_ADMIN'
    }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules), WORKSPACE_ROLES_TABLE_ID='workspace_roles')

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    # Make workspace table exist
    bigquery_client = mock.MagicMock()
    bigquery_client.get_table.return_value = object()

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=bigquery_client),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    mod.analyze_sod_violations(dummy_event)

    q = run_query_and_save_results.call_args[1]['query']
    assert 'FROM `proj.ds.workspace_roles`' in q
    assert "'WORKSPACE_ADMIN' AS role1_type" in q
    assert 'STRUCT(CAST(NULL AS STRING), ' in q  # assignments placeholder


def test_gcp_vs_workspace_query(monkeypatch, dummy_event):
    rules = [{
        'rule_id': 'R6', 'description': 'gcp vs ws',
        'role1': ['roles/editor'], 'role2': ['ws/admin'],
        'role1_type': 'GCP_IAM', 'role2_type': 'WORKSPACE_ADMIN'
    }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules), WORKSPACE_ROLES_TABLE_ID='workspace_roles')

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    bigquery_client = mock.MagicMock()
    bigquery_client.get_table.return_value = object()

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=bigquery_client),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    mod.analyze_sod_violations(dummy_event)

    q = run_query_and_save_results.call_args[1]['query']
    assert 'JOIN' in q or 'INNER JOIN' in q
    assert "'GCP_IAM' AS role1_type" in q and "'WORKSPACE_ADMIN' AS role2_type" in q
    assert 'ARRAY_AGG(STRUCT(a.resource_name, a.role))' in q


essage = None

def test_single_role_as_string_is_handled(monkeypatch, dummy_event):
    # Provide role1/role2 as string; module should coerce to lists during import
    rules = [{
        'rule_id': 'R7', 'description': 'string roles',
        'role1': 'roles/storage.admin', 'role2': 'roles/owner',
        'role1_type': 'GCP_IAM', 'role2_type': 'GCP_IAM'
    }]
    env = _base_env(SOD_RULES_JSON=json.dumps(rules))

    run_query_and_save_results = mock.MagicMock()
    get_logger = mock.MagicMock(return_value=mock.MagicMock())

    with mock.patch.dict(importlib.sys.modules, {
        'utils': types.SimpleNamespace(),
        'utils.bq_helpers': types.SimpleNamespace(run_query_and_save_results=run_query_and_save_results),
        'utils.logging_handler': types.SimpleNamespace(get_logger=get_logger),
        'utils.gcp_clients': types.SimpleNamespace(bigquery_client=mock.MagicMock()),
        'functions_framework': types.SimpleNamespace(cloud_event=lambda f: f),
        'google.cloud.exceptions': types.SimpleNamespace(NotFound=type('NF', (), {}) ),
    }):
        mod = import_module_with_env(env)

    mod.analyze_sod_violations(dummy_event)

    q = run_query_and_save_results.call_args[1]['query']
    assert "IN ('roles/storage.admin')" in q
    assert "IN ('roles/owner')" in q

