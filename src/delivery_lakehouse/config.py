"""Validated, portable settings; AWS access is never inferred from GitHub access."""

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PREFIXES = ("landing", "bronze", "silver", "gold", "quarantine", "athena-results")


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def batch_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Batch ID must be a complete SHA-256 digest")
    return value


@dataclass(frozen=True)
class Settings:
    environment: str = "dev"
    region: str = "ap-southeast-2"
    bucket: str = "ck-delivery-ops-lakehouse-2026-1025"
    database_prefix: str = "delivery_ops_dev"
    profile: str = "small"
    seed: int = 42
    start_date: str = "2026-01-01"
    max_reject_fraction: float = 0.15

    def __post_init__(self):
        identifier(self.environment)
        identifier(self.database_prefix)
        if self.region != "ap-southeast-2":
            raise ValueError("Phase 1 is restricted to ap-southeast-2")
        if self.bucket != "ck-delivery-ops-lakehouse-2026-1025":
            raise ValueError("Phase 1 uses the reviewed existing bucket only")
        if self.profile not in {"small", "medium", "large"}:
            raise ValueError("Unknown data profile")
        if not 0 <= self.max_reject_fraction <= 1:
            raise ValueError("max_reject_fraction must be in [0, 1]")
        date.fromisoformat(self.start_date)

    @classmethod
    def load(cls, path: str | Path) -> "Settings":
        return cls(**json.loads(Path(path).read_text()))
