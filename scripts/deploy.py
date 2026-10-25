"""Explicit GitHub-main plan/apply flow; has no APIs for running compute or editing IAM."""

import argparse
import json
import os
import re
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, WaiterError

from delivery_lakehouse.config import Settings
from delivery_lakehouse.ingestion.storage import S3Store

STACK = "delivery-ops-dev"
REPOSITORY = "chandrakanth02512-ctrl/aws-delivery-operations-lakehouse"


def validate_context(revision, service_role, environment):
    if environment.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise ValueError("Deployment is restricted to the reviewed GitHub repository")
    if environment.get("GITHUB_REF") != "refs/heads/main":
        raise ValueError("Deployment is restricted to main")
    if not re.fullmatch(r"[a-f0-9]{40}", revision) or revision != environment.get("GITHUB_SHA"):
        raise ValueError("Artifact revision must equal the current reviewed main commit")
    if not re.fullmatch(
        r"arn:aws:iam::[0-9]{12}:role/delivery-ops-dev-cfn-execution", service_role
    ):
        raise ValueError("Unexpected CloudFormation service role")


def reviewed_change_set(change_set, revision):
    if not re.fullmatch(rf"{STACK}-{revision}-[0-9]+", change_set):
        raise ValueError("Change set must be a plan for this exact main commit")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", choices=["plan", "apply"], required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--service-role", required=True)
    parser.add_argument("--change-set", default="")
    args = parser.parse_args()
    validate_context(args.revision, args.service_role, os.environ)
    settings = Settings()
    client = boto3.client("cloudformation", region_name=settings.region)
    if args.operation == "plan":
        store = S3Store(settings.bucket, settings.region)
        for local, remote in [
            ("jobs/phase1.py", "phase1.py"),
            ("dist/delivery_lakehouse.zip", "delivery_lakehouse.zip"),
        ]:
            store.write(
                f"landing/_artifacts/{args.revision}/{remote}",
                Path(local).read_bytes(),
                immutable=True,
            )
        try:
            existing = client.describe_stacks(StackName=STACK)["Stacks"][0]
            kind = "CREATE" if existing["StackStatus"] == "REVIEW_IN_PROGRESS" else "UPDATE"
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ValidationError" or "does not exist" not in str(
                exc
            ):
                raise
            kind = "CREATE"
        run_id = os.environ["GITHUB_RUN_ID"]
        name = f"{STACK}-{args.revision}-{run_id}"
        reviewed_change_set(name, args.revision)
        client.create_change_set(
            StackName=STACK,
            ChangeSetName=name,
            ChangeSetType=kind,
            RoleARN=args.service_role,
            TemplateBody=Path("infrastructure/lakehouse.yaml").read_text(),
            Parameters=[{"ParameterKey": "ArtifactRevision", "ParameterValue": args.revision}],
            Description=f"Reviewed main revision {args.revision}; definitions only",
        )
        try:
            client.get_waiter("change_set_create_complete").wait(
                StackName=STACK, ChangeSetName=name, WaiterConfig={"Delay": 5, "MaxAttempts": 60}
            )
        except WaiterError:
            details = client.describe_change_set(StackName=STACK, ChangeSetName=name)
            if "didn't contain changes" not in details.get("StatusReason", ""):
                raise
            print("No infrastructure changes required.")
            client.delete_change_set(StackName=STACK, ChangeSetName=name)
            return
        details = client.describe_change_set(StackName=STACK, ChangeSetName=name)
        Path("artifacts").mkdir(exist_ok=True)
        Path("artifacts/change-set.json").write_text(json.dumps(details, indent=2, default=str))
        print(f"Review artifacts/change-set.json and the AWS change set before apply: {name}")
    else:
        reviewed_change_set(args.change_set, args.revision)
        details = client.describe_change_set(StackName=STACK, ChangeSetName=args.change_set)
        if (
            details.get("Status") != "CREATE_COMPLETE"
            or details.get("ExecutionStatus") != "AVAILABLE"
        ):
            raise ValueError("Change set is not available for execution")
        # DescribeChangeSet does not return RoleARN. IAM restricts CreateChangeSet
        # to the reviewed role; check the existing stack's role when present too.
        stack = client.describe_stacks(StackName=STACK)["Stacks"][0]
        if stack.get("RoleARN") not in (None, args.service_role):
            raise ValueError("Existing stack service role differs from reviewed role")
        parameters = {p["ParameterKey"]: p["ParameterValue"] for p in details["Parameters"]}
        if parameters.get("ArtifactRevision") != args.revision:
            raise ValueError("Change set artifact revision mismatch")
        # Compare the exact reviewed template; refuse a same-name, modified change set.
        template = client.get_template(StackName=STACK, ChangeSetName=args.change_set)[
            "TemplateBody"
        ]
        if template != Path("infrastructure/lakehouse.yaml").read_text():
            raise ValueError("Change set template differs from this reviewed commit")
        creating = stack["StackStatus"] == "REVIEW_IN_PROGRESS"
        client.execute_change_set(StackName=STACK, ChangeSetName=args.change_set)
        waiter = "stack_create_complete" if creating else "stack_update_complete"
        client.get_waiter(waiter).wait(
            StackName=STACK, WaiterConfig={"Delay": 10, "MaxAttempts": 90}
        )
        print("Infrastructure definitions deployed. No Glue job or Athena query was started.")


if __name__ == "__main__":
    main()
