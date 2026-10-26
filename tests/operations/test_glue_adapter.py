import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from delivery_lakehouse.config import Settings
from delivery_lakehouse.quality.report import QualityGateError

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("mode,fail", [("snapshot", False), ("cdc", False), ("cdc", True)])
def test_glue_adapter_commits_only_after_orchestrated_verification(monkeypatch, mode, fail):
    arguments = {
        "JOB_NAME": "delivery-ops-dev-phase1",
        "bucket": Settings().bucket,
        "region": "ap-southeast-2",
        "database_prefix": "delivery_ops_dev",
        "batch_id": "a" * 64,
        "max_reject_fraction": "0.15",
        "pipeline_mode": mode,
        "execution_id": "reviewed-execution",
        "max_attempts": "2",
        "retry_delay_seconds": "1",
        "resume_execution": "true",
    }
    calls = []

    def options(argv, names):
        calls.extend(names)
        return {name: arguments[name] for name in names}

    context, job = MagicMock(), MagicMock()
    for name in ("awsglue", "awsglue.context", "awsglue.job", "awsglue.utils"):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    sys.modules["awsglue.context"].GlueContext = lambda _: context
    sys.modules["awsglue.job"].Job = lambda _: job
    sys.modules["awsglue.utils"].getResolvedOptions = options
    monkeypatch.setattr(sys, "argv", ["phase1.py", *[f"--{name}" for name in arguments]])
    spec = importlib.util.spec_from_file_location("glue_entrypoint_test", ROOT / "jobs/phase1.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.SparkContext, "getOrCreate", lambda: object())
    monkeypatch.setattr(module, "S3Store", lambda *args: object())
    monkeypatch.setattr(module, "configure_logging", lambda: None)
    run = MagicMock(side_effect=QualityGateError("verification failed") if fail else None)
    monkeypatch.setattr(module, "run_execution", run)
    if fail:
        with pytest.raises(QualityGateError):
            module.main()
        job.commit.assert_not_called()
    else:
        module.main()
        job.commit.assert_called_once()
    assert run.call_args.kwargs["mode"] == mode
    assert run.call_args.kwargs["execution_id"] == "reviewed-execution"
    assert run.call_args.kwargs["max_attempts"] == 2
    assert run.call_args.kwargs["resume"]
    assert "execution_id" in calls
