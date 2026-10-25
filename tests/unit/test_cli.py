import json
import sys

import pytest

from delivery_lakehouse.cli import main


def test_generate_and_bronze_cli_without_aws(monkeypatch, tmp_path, capsys):
    import boto3

    def unexpected_aws(*args, **kwargs):
        raise AssertionError("Local commands must not construct an AWS client")

    monkeypatch.setattr(boto3, "client", unexpected_aws)
    monkeypatch.setattr(sys, "argv", ["delivery-lakehouse", "generate", "--root", str(tmp_path)])
    main()
    batch = capsys.readouterr().out.strip()
    assert len(batch) == 64
    monkeypatch.setattr(
        sys, "argv", ["delivery-lakehouse", "bronze", "--root", str(tmp_path), "--batch-id", batch]
    )
    main()
    assert json.loads(capsys.readouterr().out)["batch_id"] == batch


def test_upload_requires_explicit_approval(monkeypatch, tmp_path):
    import boto3

    monkeypatch.setattr(boto3, "client", lambda *a, **k: pytest.fail("Unexpected AWS client"))
    monkeypatch.setattr(
        sys,
        "argv",
        ["delivery-lakehouse", "upload-landing", "--root", str(tmp_path), "--batch-id", "a" * 64],
    )
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
