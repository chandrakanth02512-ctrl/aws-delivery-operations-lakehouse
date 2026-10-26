from pathlib import Path

from cfnlint.decode import decode

ROOT = Path(__file__).resolve().parents[2]


def document():
    result, errors = decode(str(ROOT / "infrastructure/operations.yaml"))
    assert not errors
    return result


def test_orchestration_is_manual_and_reuses_one_bounded_glue_job():
    template = document()
    resources = template["Resources"]
    forbidden = {
        "AWS::Glue::Job",
        "AWS::Glue::Trigger",
        "AWS::Events::Rule",
        "AWS::Scheduler::Schedule",
        "AWS::S3::Bucket",
        "AWS::IAM::AccessKey",
        "AWS::SNS::Subscription",
    }
    assert not forbidden & {resource["Type"] for resource in resources.values()}
    machine = resources["DeliveryStateMachine"]["Properties"]
    assert machine["StateMachineType"] == "STANDARD"
    states = machine["Definition"]["States"]
    assert machine["Definition"]["StartAt"] == "RequiredFields"
    validations = states["ValidateParameters"]["Choices"][0]["And"]
    assert {"Variable": "$.batchId", "IsString": True} in validations
    assert {"Not": {"Variable": "$.batchId", "StringEquals": ""}} in validations
    task = states["RunDeliveryPipeline"]
    assert task["Resource"]["Fn::Sub"].endswith("glue:startJobRun.sync")
    assert task["Parameters"]["JobName"] == {"Ref": "ExistingJobName"}
    assert task["Parameters"]["Arguments"]["--execution_id.$"] == "$$.Execution.Name"
    assert task["Parameters"]["Arguments"]["--resume_execution"] == "true"
    assert "Retry" not in task
    assert task["Catch"][0]["Next"] == "ExecutionFailed"
    assert task["Next"] == "PublicationSucceeded"
    assert states["PublicationSucceeded"]["Type"] == "Succeed"
    assert states["ExecutionFailed"]["Type"] == "Fail"


def test_all_monitoring_is_opt_in_and_alarm_actions_are_disabled():
    template = document()
    assert template["Parameters"]["EnableMonitoring"]["Default"] == "false"
    resources = template["Resources"]
    monitored = [
        resource
        for resource in resources.values()
        if resource["Type"] in {"AWS::Logs::MetricFilter", "AWS::CloudWatch::Alarm"}
    ]
    assert len(monitored) == 7
    for resource in monitored:
        assert resource["Condition"] == "MonitoringEnabled"
        if resource["Type"] == "AWS::CloudWatch::Alarm":
            props = resource["Properties"]
            assert props["ActionsEnabled"] is False
            assert "AlarmActions" not in props
            assert props["TreatMissingData"] == "notBreaching"
    for resource in monitored:
        if resource["Type"] == "AWS::Logs::MetricFilter":
            assert all(
                "Dimensions" not in metric
                for metric in resource["Properties"]["MetricTransformations"]
            )


def test_orchestration_role_cannot_change_iam_read_data_or_start_other_jobs():
    role = document()["Resources"]["OrchestrationRole"]["Properties"]
    trust = role["AssumeRolePolicyDocument"]["Statement"][0]
    assert trust["Principal"] == {"Service": "states.amazonaws.com"}
    assert trust["Condition"]["StringEquals"]["aws:SourceAccount"] == {"Ref": "AWS::AccountId"}
    assert (
        "stateMachine:delivery-ops-dev"
        in trust["Condition"]["ArnEquals"]["aws:SourceArn"]["Fn::Sub"]
    )
    statements = role["Policies"][0]["PolicyDocument"]["Statement"]
    assert len(statements) == 1
    assert set(statements[0]["Action"]) == {
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun",
    }
    assert statements[0]["Resource"]["Fn::Sub"].endswith(":job/${ExistingJobName}")
    assert document()["Parameters"]["ExistingJobName"]["AllowedValues"] == [
        "delivery-ops-dev-phase1"
    ]
