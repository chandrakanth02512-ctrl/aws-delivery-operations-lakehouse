from pathlib import Path

import pytest
from cfnlint.decode import decode

ROOT = Path(__file__).resolve().parents[2]


def template(name):
    document, errors = decode(str(ROOT / "infrastructure" / name))
    assert not errors
    return document


def test_cost_and_existing_bucket_guardrails():
    resources = template("lakehouse.yaml")["Resources"]
    kinds = {r["Type"] for r in resources.values()}
    assert kinds <= {
        "AWS::Glue::Job",
        "AWS::Glue::Database",
        "AWS::Logs::LogGroup",
        "AWS::Athena::WorkGroup",
    }
    job = resources["Phase1Job"]["Properties"]
    for key, expected in {
        "GlueVersion": "6.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 15,
        "MaxRetries": 0,
        "ExecutionProperty": {"MaxConcurrentRuns": 1},
    }.items():
        assert job[key] == expected
    assert not job["JobRunQueuingEnabled"]
    assert "--batch_id" not in job["DefaultArguments"]
    assert (
        resources["AthenaWorkgroup"]["Properties"]["WorkGroupConfiguration"][
            "BytesScannedCutoffPerQuery"
        ]
        == 10485760
    )


def test_oidc_subject_audience_and_no_iam_escalation():
    resources = template("bootstrap.yaml")["Resources"]
    trust = resources["GitHubDeployRole"]["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]
    assert trust["Condition"]["StringEquals"] == {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
        "token.actions.githubusercontent.com:sub": (
            "repo:chandrakanth02512-ctrl/aws-delivery-operations-lakehouse:ref:refs/heads/main"
        ),
    }
    for name in ["GitHubDeployRole", "CloudFormationExecutionRole"]:
        statements = resources[name]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        denied = {a for s in statements if s["Effect"] == "Deny" for a in s["Action"]}
        assert "glue:StartJobRun" in denied
        assert "athena:StartQueryExecution" in denied
        for s in statements:
            actions = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
            if s["Effect"] == "Allow":
                assert all(not a.startswith("iam:") or a == "iam:PassRole" for a in actions)
            if "iam:PassRole" in actions:
                assert "iam:PassedToService" in s["Condition"]["StringEquals"]


@pytest.mark.parametrize("name", ["bootstrap.yaml", "lakehouse.yaml"])
def test_no_permanent_credentials_or_schedulers(name):
    forbidden = {
        "AWS::IAM::AccessKey",
        "AWS::Glue::Trigger",
        "AWS::Events::Rule",
        "AWS::Scheduler::Schedule",
        "AWS::S3::Bucket",
    }
    assert not {r["Type"] for r in template(name)["Resources"].values()} & forbidden


def test_deployment_is_manual_and_gated():
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/deploy.yml").read_text())
    # PyYAML's YAML 1.1 treats the key 'on' as boolean true; GitHub uses YAML 1.2.
    assert set(workflow.get("on", workflow.get(True))) == {"workflow_dispatch"}
    condition = workflow["jobs"]["deploy"]["if"]
    assert "github.ref == 'refs/heads/main'" in condition
    assert "vars.AWS_DEPLOYMENT_APPROVED == 'true'" in condition
    assert "DEPLOY_REVIEWED_INFRASTRUCTURE" in condition
