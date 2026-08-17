"""A small, rerunnable Bronze -> Silver -> Gold ticket pipeline."""

import argparse
import hashlib
import json
import logging
import os
import shutil
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from medallion_pipeline.agents import DataQualityAgent, MetadataTaggingAgent, SchemaInferenceAgent
from medallion_pipeline.common.utils import (
    clean,
    parse_date,
    parse_decimal,
    read_csv,
    setup_logging,
    valid_ticket_id,
    write_csv,
    write_text,
)

RAW_FIELDS = [
    "ticket_id", "created_at", "resolved_at", "category", "priority", "status", "building",
    "description", "submitted_by", "assigned_to", "resolution_notes", "cost", "sla_hours",
]
METADATA_FIELDS = ["_source_file", "_ingested_at", "_pipeline_run_id", "_pipeline_actor", "_row_hash", "_raw_row_json"]
SILVER_FIELDS = RAW_FIELDS + [
    "created_at_parsed", "resolved_at_parsed", "cost_decimal", "sla_hours_decimal",
    "resolution_hours", "is_sla_breached",
] + METADATA_FIELDS


def bronze(source: Path, output: Path, run: dict[str, str]) -> list[dict[str, str]]:
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for raw in read_csv(source):
        fields = {field: raw.get(field, "") for field in RAW_FIELDS}
        snapshot = {"fields": fields, "extra_columns": raw.get("_extra_columns", [])}
        rows.append({
            **fields,
            "_source_file": str(source),
            "_ingested_at": ingested_at,
            "_pipeline_run_id": run["id"],
            "_pipeline_actor": run["actor"],
            "_row_hash": hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest(),
            "_raw_row_json": json.dumps(snapshot, ensure_ascii=False),
        })
    write_csv(output, rows, RAW_FIELDS + METADATA_FIELDS)
    shutil.copyfile(source, output.with_name("raw_tickets_source_snapshot.csv"))
    logging.info("Bronze: %d rows", len(rows))
    return rows


def silver(bronze_rows: list[dict[str, str]], output: Path, rejects_output: Path) -> dict:
    unique_rows = {row["_row_hash"]: row for row in bronze_rows}
    rejects = []
    tickets = defaultdict(list)
    for row in unique_rows.values():
        ticket_id = clean(row["ticket_id"]).upper()
        if not valid_ticket_id(ticket_id):
            rejects.append({**row, "_rejection_reason": "invalid_ticket_id"})
        else:
            tickets[ticket_id].append({**row, "ticket_id": ticket_id})

    warnings = Counter()
    clean_rows = [clean_row(max(rows, key=completeness), warnings) for rows in tickets.values()]
    clean_rows.sort(key=lambda row: row["ticket_id"])
    write_csv(output, clean_rows, SILVER_FIELDS)
    write_csv(rejects_output, rejects, RAW_FIELDS + METADATA_FIELDS + ["_rejection_reason"])

    result = {
        "input_rows": len(bronze_rows), "output_rows": len(clean_rows),
        "exact_duplicate_rows_removed": len(bronze_rows) - len(unique_rows),
        "duplicate_ticket_ids_removed": len(unique_rows) - len(rejects) - len(clean_rows),
        "invalid_ticket_ids_rejected": len(rejects), "quality_warnings": dict(warnings),
    }
    logging.info("Silver: %d clean rows, %d rejected rows", len(clean_rows), len(rejects))
    return result


def completeness(row: dict[str, str]) -> tuple[int, str]:
    return sum(bool(clean(row[field])) for field in RAW_FIELDS), row["_row_hash"]


def clean_row(row: dict[str, str], warnings: Counter) -> dict:
    created, resolved = parse_date(clean(row["created_at"])), parse_date(clean(row["resolved_at"]))
    cost, sla = parse_decimal(clean(row["cost"])), parse_decimal(clean(row["sla_hours"]))
    for field, raw, parsed in (("created_at", row["created_at"], created), ("resolved_at", row["resolved_at"], resolved)):
        if clean(raw) and not parsed:
            warnings[f"unparseable_{field}"] += 1
    if clean(row["cost"]) and cost is None:
        warnings["invalid_or_negative_cost"] += 1
    if clean(row["sla_hours"]) and (sla is None or sla == 0):
        warnings["invalid_sla_hours"] += 1

    resolution_hours = None
    if created and resolved and resolved >= created:
        resolution_hours = Decimal(str((resolved - created).total_seconds() / 3600)).quantize(Decimal("0.01"))
    elif created and resolved:
        warnings["resolved_before_created"] += 1
    breached = resolution_hours is not None and sla is not None and sla > 0 and resolution_hours > sla

    return {
        **{field: clean(row[field]) for field in RAW_FIELDS},
        "ticket_id": row["ticket_id"],
        "category": DataQualityAgent.category(row["category"]),
        "priority": DataQualityAgent.priority(row["priority"]),
        "status": DataQualityAgent.status(row["status"]),
        "building": clean(row["building"]) or "Unspecified",
        "created_at_parsed": created.isoformat(timespec="seconds") if created else "",
        "resolved_at_parsed": resolved.isoformat(timespec="seconds") if resolved else "",
        "cost_decimal": str(cost) if cost is not None else "",
        "sla_hours_decimal": str(sla) if sla is not None else "",
        "resolution_hours": str(resolution_hours) if resolution_hours is not None else "",
        "is_sla_breached": str(breached).lower() if resolution_hours is not None and sla else "",
        **{field: row[field] for field in METADATA_FIELDS},
    }


def gold(silver_path: Path, output_dir: Path) -> dict:
    rows = read_csv(silver_path)
    volume = Counter((row["category"], row["priority"]) for row in rows)
    write_csv(output_dir / "ticket_volume_by_category_priority.csv", [
        {"category": category, "priority": priority, "ticket_count": count}
        for (category, priority), count in sorted(volume.items())
    ], ["category", "priority", "ticket_count"])

    sla = defaultdict(lambda: {"resolved": 0, "measured": 0, "breached": 0, "hours": Decimal("0")})
    for row in rows:
        metric = sla[row["category"]]
        metric["resolved"] += row["status"] in {"Resolved", "Closed"}
        if row["resolution_hours"] and row["is_sla_breached"]:
            metric["measured"] += 1
            metric["breached"] += row["is_sla_breached"] == "true"
            metric["hours"] += Decimal(row["resolution_hours"])
    write_csv(output_dir / "sla_performance_by_category.csv", [
        {"category": category, "resolved_ticket_count": data["resolved"], "tickets_with_sla_measure": data["measured"], "sla_breached_count": data["breached"], "sla_breach_rate": round(data["breached"] / data["measured"], 4) if data["measured"] else "", "avg_resolution_hours": round(data["hours"] / data["measured"], 2) if data["measured"] else ""}
        for category, data in sorted(sla.items())
    ], ["category", "resolved_ticket_count", "tickets_with_sla_measure", "sla_breached_count", "sla_breach_rate", "avg_resolution_hours"])

    open_statuses = {"Open", "In Progress", "Pending Vendor", "Escalated", "Unknown"}
    backlog = Counter((row["building"], row["priority"]) for row in rows if row["status"] in open_statuses)
    write_csv(output_dir / "open_backlog_by_building_priority.csv", [
        {"building": building, "priority": priority, "open_ticket_count": count}
        for (building, priority), count in sorted(backlog.items())
    ], ["building", "priority", "open_ticket_count"])
    logging.info("Gold: wrote 3 operational datasets")
    return {"input_rows": len(rows), "outputs": [
        "ticket_volume_by_category_priority.csv", "sla_performance_by_category.csv",
        "open_backlog_by_building_priority.csv",
    ]}


def run_agents(bronze_rows: list[dict[str, str]], output_dir: Path) -> dict:
    """Materialize agent findings so they can be reviewed before deployment."""
    quality_agent = DataQualityAgent()
    quality = quality_agent.profile(bronze_rows)
    schema = SchemaInferenceAgent().inspect(bronze_rows, output_dir)
    rules = quality_agent.rules()
    profile_path = output_dir / "data_quality_profile.json"
    previous = json.loads(profile_path.read_text()) if profile_path.exists() else {"fields": {}}
    null_deltas = {
        field: round(data["null_rate"] - previous["fields"].get(field, {}).get("null_rate", data["null_rate"]), 4)
        for field, data in quality["fields"].items()
    }

    write_text(profile_path, json.dumps(quality, indent=2) + "\n")
    write_text(output_dir / "quality_rules.md", "\n".join(
        f"- **{item['rule']}** Why: {item['why']}" for item in rules
    ) + "\n")
    write_text(output_dir / "generated_validation_rules.py", quality_agent.generated_python())
    write_text(output_dir / "silver_schema.sql", schema["silver_ddl"] + "\n")
    write_text(output_dir / "silver_transformation.sql", schema["transformation_sql"] + "\n")
    write_text(output_dir / "schema_recommendations.json", json.dumps(schema, indent=2) + "\n")
    logging.info("Agents: wrote quality profile, Silver schema, SQL, and drift report")
    return {
        "schema_drift": schema["drift"], "quality_rules": len(rules),
        "null_rate_deltas": null_deltas,
        "usage": {"provider": "local_deterministic", "calls": 2, "failures": 0, "input_tokens": 0, "output_tokens": 0, "estimated_cost_usd": 0.0},
    }


def approval_gate(agent_result: dict, output_dir: Path, approve: bool) -> dict:
    """Block high-risk agent recommendations until explicitly approved."""
    drift = agent_result["schema_drift"]
    requests = []
    if drift["added_columns"] or drift["removed_columns"]:
        requests.append({
            "id": "schema-drift",
            "risk": "high",
            "suggestion": drift["migration"],
            "why_review": "Schema changes can invalidate downstream transformations or consumers.",
        })
    result = {
        "policy": {
            "schema_changes": "approval required before migration execution",
            "destructive_transforms": "approval required before deleting source records",
            "major_recategorizations": "approval required before changing reviewed mappings",
        },
        "requests": [{**request, "status": "approved" if approve else "pending"} for request in requests],
    }
    write_text(output_dir / "approval_requests.json", json.dumps(result, indent=2) + "\n")
    if requests and not approve:
        raise RuntimeError("High-risk agent suggestions require review. Inspect data/agent_outputs/approval_requests.json and rerun with --approve-agent-changes.")
    return result


def tag_bronze(bronze_rows: list[dict[str, str]], bronze_dir: Path) -> dict:
    """Classify a dataset as soon as it lands, before downstream processing."""
    tags = MetadataTaggingAgent().tag(bronze_rows)
    write_text(bronze_dir / "metadata_tags.json", json.dumps(tags, indent=2) + "\n")
    logging.info("Bronze metadata: tagged dataset as %s", tags["dataset"]["classification"])
    return tags


def observe(root: Path, bronze_rows: list[dict], agent_result: dict, silver_result: dict, gold_result: dict) -> dict:
    """Write small, actionable health metrics and log threshold breaches."""
    thresholds = {"max_reject_rate": 0.02, "max_null_rate_increase": 0.05, "max_agent_failure_rate": 0.0}
    input_rows = len(bronze_rows)
    reject_rate = silver_result["invalid_ticket_ids_rejected"] / input_rows if input_rows else 0
    agent_usage = agent_result["usage"]
    agent_failure_rate = agent_usage["failures"] / agent_usage["calls"] if agent_usage["calls"] else 0
    alerts = []
    if reject_rate > thresholds["max_reject_rate"]:
        alerts.append(f"Reject rate {reject_rate:.2%} exceeds {thresholds['max_reject_rate']:.2%}.")
    for field, delta in agent_result["null_rate_deltas"].items():
        if delta > thresholds["max_null_rate_increase"]:
            alerts.append(f"Null rate for {field} increased by {delta:.2%}.")
    if agent_failure_rate > thresholds["max_agent_failure_rate"]:
        alerts.append(f"Agent failure rate {agent_failure_rate:.2%} exceeds threshold.")
    for alert in alerts:
        logging.warning("ALERT: %s", alert)

    metrics = {
        "status": "warning" if alerts else "healthy", "thresholds": thresholds, "alerts": alerts,
        "pipeline": {"bronze_rows": input_rows, "silver_rows": silver_result["output_rows"], "rejected_rows": silver_result["invalid_ticket_ids_rejected"], "reject_rate": round(reject_rate, 4), "gold_input_rows": gold_result["input_rows"]},
        "agents": {"null_rate_deltas": agent_result["null_rate_deltas"], "usage": agent_usage, "failure_rate": agent_failure_rate},
    }
    write_text(root / "data" / "observability.json", json.dumps(metrics, indent=2) + "\n")
    logging.info("Observability: %s (%d alerts)", metrics["status"], len(alerts))
    return metrics


def run(root: Path, replay_days: int = 7, backfill: bool = False, approve_agent_changes: bool = False) -> None:
    """Run a deterministic replay; retain replay settings for future incremental loads."""
    if replay_days < 0:
        raise ValueError("replay_days must be zero or greater")
    source = root / "data" / "raw_tickets.csv"
    if not source.exists():
        raise FileNotFoundError(f"Source data not found: {source}")
    run_context = {
        "id": str(uuid.uuid4()), "actor": os.getenv("USER", "local_runner"),
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    bronze_path = root / "data" / "bronze" / "raw_tickets_bronze.csv"
    silver_path = root / "data" / "silver" / "tickets_clean.csv"
    bronze_rows = bronze(source, bronze_path, run_context)
    bronze_tags = tag_bronze(bronze_rows, bronze_path.parent)
    agent_result = run_agents(bronze_rows, root / "data" / "agent_outputs")
    approvals = approval_gate(agent_result, root / "data" / "agent_outputs", approve_agent_changes)
    silver_result = silver(bronze_rows, silver_path, root / "data" / "silver" / "tickets_rejected.csv")
    gold_result = gold(silver_path, root / "data" / "gold")
    observability = observe(root, bronze_rows, agent_result, silver_result, gold_result)
    incremental = {
        "mode": "full_replay" if backfill else "replay_window",
        "replay_window_days": replay_days,
        "dedupe_keys": ["_row_hash", "ticket_id"],
        "late_arrival_handling": "All rows in the local source are replayed. A late record is therefore included even when its business date is older than the watermark.",
        "double_count_protection": "Exact rows are removed by _row_hash; duplicate ticket IDs keep one deterministic, most-complete record; Gold is rebuilt from the deduplicated Silver table.",
    }
    lineage = {
        "run": run_context,
        "incremental_strategy": incremental,
        "approval_gate": approvals,
        "stages": [
            {"stage": "bronze", "when": datetime.now(timezone.utc).isoformat(timespec="seconds"), "what": "Copied raw records without transformation; added source, hash, run, actor, and automatic metadata tags.", "input": str(source), "outputs": [str(bronze_path), str(bronze_path.with_name("raw_tickets_source_snapshot.csv")), str(bronze_path.parent / "metadata_tags.json")], "row_count": len(bronze_rows), "metadata_classification": bronze_tags["dataset"]},
            {"stage": "silver", "when": datetime.now(timezone.utc).isoformat(timespec="seconds"), "what": "Validated, typed, normalized, and deduplicated Bronze records; rejected invalid ticket IDs.", "input": str(bronze_path), "outputs": [str(silver_path), str(root / "data" / "silver" / "tickets_rejected.csv")], **silver_result},
            {"stage": "gold", "when": datetime.now(timezone.utc).isoformat(timespec="seconds"), "what": "Aggregated validated Silver tickets into volume, SLA, and backlog metrics.", "input": str(silver_path), "output_directory": str(root / "data" / "gold"), **gold_result},
        ],
    }
    write_text(root / "data" / "lineage.json", json.dumps(lineage, indent=2) + "\n")
    write_text(root / "data" / "incremental_state.json", json.dumps({
        "last_successful_run": run_context, **incremental,
    }, indent=2) + "\n")
    write_text(root / "data" / "run_report.json", json.dumps({"agents": agent_result, "approvals": approvals, "observability": observability, "silver": silver_result}, indent=2) + "\n")
    logging.info("Pipeline complete")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-days", type=int, default=7, help="Metadata window for daily incremental deployments.")
    parser.add_argument("--backfill", action="store_true", help="Record this run as a deterministic full backfill.")
    parser.add_argument("--approve-agent-changes", action="store_true", help="Approve pending high-risk agent recommendations for this run.")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    setup_logging(root)
    run(root, replay_days=args.replay_days, backfill=args.backfill, approve_agent_changes=args.approve_agent_changes)


if __name__ == "__main__":
    main()
