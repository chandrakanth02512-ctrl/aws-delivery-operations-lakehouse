"""Render source contracts as reviewable Markdown; --check detects documentation drift."""

import argparse
from pathlib import Path

from delivery_lakehouse.contracts import CONTRACT_VERSION, CONTRACTS


def render():
    lines = [
        "# Source data dictionary",
        "",
        f"Contract version: {CONTRACT_VERSION}.",
        "",
        "Generated from `src/delivery_lakehouse/contracts.py`. Run "
        "`uv run python scripts/data_dictionary.py` after an intentional contract change.",
        "",
        "UTF-8 newline-delimited JSON, one object per physical line. "
        "Business keys are synthetic strings; timestamps require explicit UTC/offset information. "
        "Silver timestamps are UTC Spark `timestamp` values. Numeric strings may be cast; "
        "invalid casts are quarantined. Empty/whitespace-only strings become null. "
        "Optional fields may be omitted. Additional fields are rejected in Phase 1.",
        "",
    ]
    for name, contract in CONTRACTS.items():
        lines.extend(
            [
                f"## {name}",
                "",
                f"Primary key: `{contract.primary_key}`.",
                "",
                "| Field | Silver type | Required | Constraints / relationship | Meaning |",
                "|---|---|---|---|---|",
            ]
        )
        for name_, spec in contract.fields.items():
            rules = []
            if spec.allowed:
                rules.append(", ".join(spec.allowed))
            if spec.minimum is not None:
                rules.append(f">= {spec.minimum}; finite")
            if name_ in contract.foreign_keys:
                parent, key = contract.foreign_keys[name_]
                rules.append(f"FK → {parent}.{key}")
            lines.append(
                f"| `{name_}` | {spec.kind} | {'yes' if spec.required else 'no'} "
                f"| {'; '.join(rules) or '—'} | {spec.description or 'See layer rules'} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Bronze and Silver lineage",
            "",
            "Every input row retains these fields in Bronze, Silver and quarantine. "
            "Gold shipment facts carry `_batch_id`; attempt facts and source dimensions "
            "retain all lineage except `_raw_json`.",
            "",
            "| Field | Type | Meaning |",
            "|---|---|---|",
            "| `_raw_json` | string | Original physical line, excluding newline delimiter |",
            "| `_source_file` | string | Absolute local path or full S3 URI |",
            "| `_source_line` | long | One-based physical line number |",
            "| `_batch_id` | string | SHA-256 of the canonical manifest payload |",
            "| `_ingested_at` | string | First successful Bronze ingestion time, UTC ISO-8601 |",
            "| `_record_hash` | string | SHA-256 of raw line |",
            "| `_record_id` | string | SHA-256 of batch/entity/line identity; "
            "duplicates stay distinct |",
            "| `_errors` | array<string> | Quarantine only: all validation reasons |",
            "",
            "Gold schemas and grains are described in [layers.md](layers.md); "
            "KPI formulas are defined in [kpis.md](kpis.md).",
            "",
        ]
    )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    path = Path(__file__).resolve().parents[1] / "docs" / "data-dictionary.md"
    content = render()
    if args.check:
        if not path.exists() or path.read_text() != content:
            raise SystemExit("Data dictionary differs from source contracts")
    else:
        path.write_text(content)


if __name__ == "__main__":
    main()
