"""Explainable, local agents that learn from Bronze data.

The agents use deterministic profiling so a pipeline run is reproducible and does
not require an API key. Their recommendations are materialized as files for review.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


class DataQualityAgent:
    """Profiles Bronze values and supplies the canonical labels used by Silver."""

    PLACEHOLDERS = {"", "null", "none", "unknown", "???", "n/a"}

    @classmethod
    def missing(cls, value: str | None) -> bool:
        return (value or "").strip().lower() in cls.PLACEHOLDERS

    @staticmethod
    def category(value: str | None) -> str:
        text = (value or "").strip().lower()
        rules = {
            "Pest Control": ("pest", "exterminator"),
            "Fire Safety": ("fire", "sprinkler", "smoke"),
            "Plumbing": ("plumb", "water"),
            "Housekeeping": ("clean", "janitorial", "housekeeping"),
            "Elevator": ("elevator", "lift", "vertical transport"),
            "Security & Access": ("access", "badge", "secur"),
            "Electrical": ("electr", "power", "outlet"),
            "HVAC": ("a/c", "hvac", "air condition"),
        }
        for label, keywords in rules.items():
            if any(keyword in text for keyword in keywords):
                return label
        return "General Maintenance" if "maintenance" in text else "Other"

    @staticmethod
    def priority(value: str | None) -> str:
        text = (value or "").strip().lower()
        if text in {"critical", "crit", "urgent!!!", "asap"}:
            return "Critical"
        if text in {"high", "hi"}:
            return "High"
        if text in {"medium", "med", "normal"}:
            return "Medium"
        if text in {"low", "lo"}:
            return "Low"
        return "Unspecified"

    @staticmethod
    def status(value: str | None) -> str:
        return {
            "open": "Open", "in progress": "In Progress", "pending vendor": "Pending Vendor",
            "escalated": "Escalated", "closed": "Closed", "resolved": "Resolved",
        }.get((value or "").strip().lower(), "Unknown")

    def profile(self, rows: list[dict[str, str]]) -> dict:
        fields = rows[0].keys() if rows else []
        profile = {"row_count": len(rows), "fields": {}}
        for field in fields:
            values = [(row.get(field) or "").strip() for row in rows]
            present = [value for value in values if not self.missing(value)]
            counts = Counter(present)
            profile["fields"][field] = {
                "null_count": len(values) - len(present),
                "null_rate": round((len(values) - len(present)) / len(values), 4) if values else 0,
                "cardinality": len(counts),
                "top_values": counts.most_common(10),
            }
        profile["numeric_outliers"] = self._numeric_outliers(rows, "cost")
        profile["numeric_outliers"]["sla_hours"] = self._numeric_outliers(rows, "sla_hours")
        return profile

    @staticmethod
    def _numeric_outliers(rows: list[dict[str, str]], field: str) -> dict:
        values = []
        for row in rows:
            try:
                value = Decimal((row.get(field) or "").replace("$", "").replace(",", ""))
                if value >= 0:
                    values.append(value)
            except InvalidOperation:
                continue
        if len(values) < 4:
            return {"count": len(values), "outlier_count": 0}
        values.sort()
        q1, q3 = values[len(values) // 4], values[(3 * len(values)) // 4]
        upper_bound = q3 + Decimal("1.5") * (q3 - q1)
        return {
            "count": len(values), "q1": str(q1), "q3": str(q3),
            "upper_outlier_bound": str(upper_bound),
            "outlier_count": sum(value > upper_bound for value in values),
        }

    @staticmethod
    def rules() -> list[dict[str, str]]:
        return [
            {"rule": "Treat NULL, unknown, ??? and n/a as missing.", "why": "A single missing-value representation makes null rates and filters reliable."},
            {"rule": "Keep TKT-<digits> ticket IDs; send other IDs to a reject table.", "why": "Gold metrics must have one stable ticket grain."},
            {"rule": "Normalize category, priority and status labels.", "why": "Case and spelling variants otherwise split the same operational workload across groups."},
            {"rule": "Parse known date formats; do not calculate duration when dates are invalid or reversed.", "why": "Bad timestamps create misleading SLA breach metrics."},
            {"rule": "Accept only non-negative costs and positive SLA hours.", "why": "Negative or malformed values distort cost and SLA reporting."},
        ]

    @staticmethod
    def generated_python() -> str:
        return '''# Generated validation rules (implemented by the Silver stage)\n\ndef is_valid_ticket_id(value):\n    return value.startswith("TKT-") and value[4:].isdigit()\n\ndef valid_non_negative(value):\n    return value is not None and value >= 0\n\ndef valid_sla_hours(value):\n    return value is not None and value > 0\n'''


class MetadataTaggingAgent:
    """Classifies a landing dataset from its column names and sample values."""

    PII_COLUMNS = {"submitted_by", "assigned_to"}
    FREE_TEXT_COLUMNS = {"description", "resolution_notes"}

    def tag(self, rows: list[dict[str, str]]) -> dict:
        columns = list(rows[0]) if rows else []
        column_tags = {}
        for column in columns:
            tags = ["operational_support"]
            if column in self.PII_COLUMNS:
                tags += ["pii_hint", "person_name", "restricted"]
            elif column in self.FREE_TEXT_COLUMNS:
                tags += ["free_text", "potential_pii", "review_before_external_sharing"]
            elif column == "cost":
                tags += ["financial", "internal"]
            elif column == "ticket_id":
                tags += ["operational_identifier", "internal"]
            elif column.startswith("_"):
                tags += ["technical_metadata", "lineage"]
            else:
                tags += ["internal"]
            column_tags[column] = tags

        has_pii = bool(self.PII_COLUMNS.intersection(columns))
        return {
            "dataset": {
                "domain": "operational_support",
                "classification": "restricted" if has_pii else "internal",
                "sensitivity": "moderate" if has_pii else "low",
                "reason": "Person-name columns and free-text fields may contain support-ticket PII.",
            },
            "columns": column_tags,
        }


class SchemaInferenceAgent:
    """Infers a stable Silver contract and flags Bronze column changes."""

    SILVER_SCHEMA = [
        ("ticket_id", "VARCHAR(32)", False), ("created_at_parsed", "TIMESTAMP", True),
        ("resolved_at_parsed", "TIMESTAMP", True), ("category", "VARCHAR(64)", False),
        ("priority", "VARCHAR(16)", False), ("status", "VARCHAR(32)", False),
        ("building", "VARCHAR(128)", True), ("cost_decimal", "DECIMAL(12,2)", True),
        ("sla_hours_decimal", "DECIMAL(10,2)", True), ("resolution_hours", "DECIMAL(12,2)", True),
        ("is_sla_breached", "BOOLEAN", True), ("_row_hash", "CHAR(64)", False),
        ("_source_file", "VARCHAR(512)", False), ("_ingested_at", "TIMESTAMP", False),
    ]

    def inspect(self, rows: list[dict[str, str]], output_dir: Path) -> dict:
        columns = list(rows[0].keys()) if rows else []
        baseline_path = output_dir / "bronze_schema_baseline.json"
        previous = json.loads(baseline_path.read_text()) if baseline_path.exists() else {"columns": columns}
        added, removed = sorted(set(columns) - set(previous["columns"])), sorted(set(previous["columns"]) - set(columns))
        result = {
            "observed_columns": columns,
            "drift": {"added_columns": added, "removed_columns": removed, "migration": self.migration(added, removed)},
            "silver_ddl": self.ddl(), "transformation_sql": self.transformation_sql(),
        }
        output_dir.mkdir(parents=True, exist_ok=True)
        baseline_path.write_text(json.dumps({"columns": columns}, indent=2) + "\n")
        return result

    def ddl(self) -> str:
        columns = [f"  {name} {kind}{'' if nullable else ' NOT NULL'}" for name, kind, nullable in self.SILVER_SCHEMA]
        return "CREATE TABLE silver_tickets (\n" + ",\n".join(columns) + "\n);"

    @staticmethod
    def transformation_sql() -> str:
        return '''INSERT INTO silver_tickets\nSELECT\n  UPPER(TRIM(ticket_id)) AS ticket_id,\n  TRY_CAST(created_at AS TIMESTAMP) AS created_at_parsed,\n  TRY_CAST(resolved_at AS TIMESTAMP) AS resolved_at_parsed,\n  CASE WHEN LOWER(category) LIKE '%pest%' THEN 'Pest Control' ELSE 'Other' END AS category,\n  TRY_CAST(REPLACE(cost, '$', '') AS DECIMAL(12,2)) AS cost_decimal,\n  TRY_CAST(sla_hours AS DECIMAL(10,2)) AS sla_hours_decimal,\n  _row_hash, _source_file, _ingested_at\nFROM bronze_tickets\nWHERE REGEXP_LIKE(ticket_id, '^TKT-[0-9]+$');'''

    @staticmethod
    def migration(added: list[str], removed: list[str]) -> list[str]:
        changes = [f"ALTER TABLE bronze_tickets ADD COLUMN {column} VARCHAR;" for column in added]
        changes += [f"-- Review before removing missing source column: {column}" for column in removed]
        return changes or ["No schema drift detected."]
