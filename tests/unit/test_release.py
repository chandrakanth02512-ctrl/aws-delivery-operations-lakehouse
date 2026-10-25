import importlib.util
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_deterministic_glue_package(tmp_path):
    packaging = load_script("package_glue")
    first, second = tmp_path / "first.zip", tmp_path / "second.zip"
    packaging.package(ROOT, first)
    packaging.package(ROOT, second)
    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        assert "delivery_lakehouse/pipeline.py" in archive.namelist()
        assert all(name.endswith(".py") for name in archive.namelist())
        assert archive.testzip() is None


def test_deploy_rejects_unreviewed_context():
    deploy = load_script("deploy")
    revision = "a" * 40
    role = "arn:aws:iam::123456789012:role/delivery-ops-dev-cfn-execution"
    environment = {
        "GITHUB_REPOSITORY": deploy.REPOSITORY,
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": revision,
    }
    deploy.validate_context(revision, role, environment)
    for change in [
        {"GITHUB_REF": "refs/heads/feature"},
        {"GITHUB_REPOSITORY": "attacker/fork"},
        {"GITHUB_SHA": "b" * 40},
    ]:
        with pytest.raises(ValueError):
            deploy.validate_context(revision, role, {**environment, **change})
    with pytest.raises(ValueError):
        deploy.reviewed_change_set(f"delivery-ops-dev-{'b' * 40}-1", revision)


def test_apply_uses_actual_cloudformation_response_contract(monkeypatch):
    """Stub real SDK response shapes; DescribeChangeSet has no RoleARN field."""
    import sys
    from datetime import UTC, datetime

    import boto3
    from botocore.stub import Stubber

    deploy = load_script("deploy")
    revision = "a" * 40
    role = "arn:aws:iam::123456789012:role/delivery-ops-dev-cfn-execution"
    change_set = f"delivery-ops-dev-{revision}-1"
    for key, value in {
        "GITHUB_REPOSITORY": deploy.REPOSITORY,
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": revision,
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "deploy.py",
            "--operation",
            "apply",
            "--revision",
            revision,
            "--service-role",
            role,
            "--change-set",
            change_set,
        ],
    )
    client = boto3.client(
        "cloudformation",
        region_name="ap-southeast-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    monkeypatch.setattr(deploy.boto3, "client", lambda *a, **k: client)
    target = {"StackName": deploy.STACK, "ChangeSetName": change_set}
    stack = {
        "StackName": deploy.STACK,
        "CreationTime": datetime(2026, 1, 1, tzinfo=UTC),
        "StackStatus": "REVIEW_IN_PROGRESS",
        "RoleARN": role,
    }
    with Stubber(client) as stub:
        stub.add_response(
            "describe_change_set",
            {
                "Status": "CREATE_COMPLETE",
                "ExecutionStatus": "AVAILABLE",
                "Parameters": [{"ParameterKey": "ArtifactRevision", "ParameterValue": revision}],
            },
            target,
        )
        stub.add_response("describe_stacks", {"Stacks": [stack]}, {"StackName": deploy.STACK})
        stub.add_response(
            "get_template",
            {"TemplateBody": (ROOT / "infrastructure/lakehouse.yaml").read_text()},
            target,
        )
        stub.add_response("execute_change_set", {}, target)
        stub.add_response(
            "describe_stacks",
            {"Stacks": [{**stack, "StackStatus": "CREATE_COMPLETE"}]},
            {"StackName": deploy.STACK},
        )
        deploy.main()
        stub.assert_no_pending_responses()
