# Operational Tickets Medallion Pipeline

This project transforms `data/raw_tickets.csv` into reproducible Bronze, Silver, and Gold datasets using only the Python standard library.

## Architecture

```text
┌──────────────────────┐
│ data/raw_tickets.csv │
└──────────┬───────────┘
           │ ingest + lineage metadata
           ▼
┌──────────────────────┐      ┌───────────────────────────────┐
│ Bronze               │─────▶│ Schema Inference Agent        │
│ raw_tickets_bronze   │      │ DDL, SQL, schema-drift report │
└──────────┬───────────┘      └───────────────────────────────┘
           │ profile + clean
           ├──────────────────▶┌───────────────────────────────┐
           │                   │ Data Quality Agent            │
           ▼                   │ profile + validation rules    │
┌──────────────────────┐      └───────────────────────────────┘
│ Silver               │
│ clean tickets +      │
│ rejected tickets     │
└──────────┬───────────┘
           │ aggregate
           ▼
┌──────────────────────┐
│ Gold                 │
│ volume, SLA, backlog │
└──────────────────────┘
```

## How to Run

```bash
.venv/bin/python main.py
```

This is intentionally a simple Python script. The supplied dataset has about 10k rows, so it runs quickly with the standard library and fits comfortably in memory. Docker is unnecessary because there are no external services or third-party dependencies to package. Spark is unnecessary at this size; its local setup and execution overhead would slow down iteration without providing a benefit. See [What changes at 100x scale](#what-changes-at-100x-scale) for when those tools become appropriate.

The pipeline atomically replaces its output files on every run, so it is safe to re-run. It never mutates the supplied source CSV. Logs are written to `logs/pipeline.log`; data-quality counts are written to `data/run_report.json`.

## Observability and SLA checks

Each run writes `data/observability.json` and emits matching log lines. It records Bronze/Silver/Gold row counts, rejected-row rate, agent null-rate deltas compared with the prior run, agent failures, and agent token/cost usage. The current agents are local and deterministic, so token usage and estimated cost are both `0`.

| Alert | Threshold | Response |
| --- | --- | --- |
| Invalid ticket reject rate | More than 2% of Bronze rows | Log `ALERT` and investigate source-format changes. |
| Null-rate increase | More than 5 percentage points for any Bronze column | Log `ALERT` and review source quality/schema drift. |
| Agent failure rate | More than 0% | Log `ALERT`; do not trust agent recommendations until the failure is resolved. |

The pipeline does not silently hide these conditions: alerts remain in the observability artifact and `logs/pipeline.log`. At production scale, these same metrics should feed a monitoring service and page on-call owners for sustained failures.

## Layers

| Layer | Output | Purpose |
| --- | --- | --- |
| Bronze | `data/bronze/raw_tickets_bronze.csv` | Schema-on-read source values plus `_source_file`, UTC `_ingested_at`, SHA-256 `_row_hash`, and `_raw_row_json`. `raw_tickets_source_snapshot.csv` is an unchanged source copy. |
| Silver | `data/silver/tickets_clean.csv` | One validated, typed record per ticket; rejects are retained in `tickets_rejected.csv` with a reason. |
| Gold | `data/gold/*.csv` | Business-ready operational metrics. |

### Why CSV storage

All three layers use CSV because this is a small, local, dependency-free exercise: the source is roughly 10k rows, files are easy to inspect in an editor or spreadsheet, and standard-library Python can read and write them without Docker, a database, or cloud credentials.

- **Bronze CSV** preserves a portable snapshot of the landed source with lineage metadata; an unchanged source copy is retained alongside it.
- **Silver CSV** makes the cleaned and rejected records easy to review during data-quality debugging.
- **Gold CSV** keeps the small business aggregates simple to share with analysts or open in a spreadsheet.

CSV is not the long-term storage choice for large or concurrent workloads: it lacks efficient typed queries, partitions, transactions, and access controls. At production scale, Bronze/Silver would be partitioned Parquet or Delta/Iceberg tables in object storage or a warehouse, while Gold would be warehouse tables or materialized views. See [What changes at 100x scale](#what-changes-at-100x-scale).

## Data lineage

Every run writes [data/lineage.json](data/lineage.json), an end-to-end record of the source, transformations, outputs, run time, and pipeline actor.

| Stage | What is traceable | Where it is recorded |
| --- | --- | --- |
| Source → Bronze | The original file path, an unchanged source snapshot, ingestion time, a SHA-256 row hash, run ID, and actor. | Bronze columns: `_source_file`, `_ingested_at`, `_row_hash`, `_pipeline_run_id`, `_pipeline_actor`. |
| Bronze → Silver | The input Bronze file, validation/normalization/deduplication action, clean/rejected outputs, and row counts. Silver retains the Bronze lineage columns. | `data/lineage.json` and Silver lineage columns. |
| Silver → Gold | The input Silver file, aggregation action, output metric files, and number of input records. | `data/lineage.json`. |

The lineage manifest records the **who** (`actor`), **what** (stage transformation description and files), and **when** (UTC run and stage timestamps). Gold datasets are aggregates, so their row-level provenance is the referenced Silver dataset and the documented aggregation rather than a copied ticket hash on every metric row.

## Incremental and backfill strategy

The current local CSV implementation uses a deterministic full replay: every run re-reads the source and atomically replaces Bronze, Silver, and Gold outputs. This is deliberate for a 10k-row file—it makes late-arriving records safe even if their `created_at` is older than the last run, and it avoids maintaining fragile local checkpoints.

Double counting is prevented at two levels: exact records are removed by `_row_hash`, then duplicate `ticket_id` values resolve to one deterministic, most-complete record before Gold is rebuilt. Each run records its replay policy, dedupe keys, and run details in `data/incremental_state.json` and `data/lineage.json`.

```bash
# Default daily replay policy (seven-day window for a future partitioned deployment)
.venv/bin/python main.py --replay-days 7

# Record a deliberate full backfill after replacing or extending the source file
.venv/bin/python main.py --backfill
```

For a partitioned production source, the same policy would reprocess the last seven **ingestion-date** partitions (not just business dates), merge Silver by `ticket_id` and `_row_hash`, and rebuild only affected Gold partitions. A historical backfill would run the same logic over an explicit ingestion-date range, using the same keys, so it remains deterministic and cannot add duplicate tickets.

## Metadata auto-tagging at landing

As soon as Bronze is written, `MetadataTaggingAgent` automatically creates [data/bronze/metadata_tags.json](data/bronze/metadata_tags.json). It classifies the dataset as `operational_support` and tags each column from its name and role before Silver can consume it.

For this source, `submitted_by` and `assigned_to` receive `pii_hint`, `person_name`, and `restricted` tags; `description` and `resolution_notes` receive `potential_pii` and `free_text`; `cost` receives `financial`; and lineage fields receive `technical_metadata` and `lineage`. Because of the person-name and free-text fields, the dataset classification is `restricted` with `moderate` sensitivity. The Bronze entry in `data/lineage.json` links to this tag artifact.

## Silver cleaning rules

- Preserve the Bronze raw values and metadata. Empty strings and placeholders (`NULL`, `unknown`, `???`, `n/a`) become empty values in Silver to make missingness consistent.
- Exact duplicate source rows are removed using `_row_hash`. For duplicate valid ticket IDs, retain the most complete record; ties resolve by hash, making the outcome deterministic.
- Invalid ticket IDs (anything other than `TKT-<digits>`) are excluded from the clean table but retained in the reject table. This protects the ticket grain of Gold metrics.
- Parse ISO dates, `dd-Mon-yyyy HH:MM`, slash/dash dates (including AM/PM), and Unix epoch timestamps into ISO-8601 fields. Unparseable dates remain as raw values with a warning in the run report.
- Map category, priority, and status spelling/case variants into operational labels. The mappings are token-based and explainable in `agents.py`; unmapped values are `Other`/`Unspecified`/`Unknown`, rather than silently guessed.
- Convert non-negative monetary costs and positive SLA values to decimals. Invalid or negative costs and invalid SLAs are null and reported. Resolution hours and breach flags are computed only when both timestamps are valid and chronological.

## Agent assistance

Two local, explainable agents run after Bronze ingestion. They have no credentials or network dependency.

- `SchemaInferenceAgent` proposes the typed Silver contract and transformation SQL. It compares Bronze columns with the previous run and proposes migrations when a column is added or removed.
- `DataQualityAgent` profiles null rates, cardinality, value distributions, and numeric outliers. It emits rule explanations and generated validation Python.

Review the generated artifacts in `data/agent_outputs/`:

- `silver_schema.sql` and `silver_transformation.sql`
- `schema_recommendations.json` (including schema-drift migrations)
- `data_quality_profile.json`, `quality_rules.md`, and `generated_validation_rules.py`

## Human-in-the-loop approval gates

Agent outputs are recommendations, not executable changes. The generated DDL and migration SQL are never applied by this pipeline. A schema addition or removal creates a high-risk request in `data/agent_outputs/approval_requests.json` and stops the run before Silver/Gold processing.

Review the request and its migration proposal, then explicitly approve that run only when the downstream impact is understood:

```bash
.venv/bin/python main.py --approve-agent-changes
```

The same fail-closed policy applies to future destructive transforms and major category-mapping changes: they must be represented as an approval request before their implementation can replace a reviewed rule. Current Silver cleanup is non-destructive—the original Bronze row and rejected-record table are retained—and its category mappings are reviewed code, not agent-written changes.

## Agent assessment

### Schema Inference & Evolution Agent

**What it does:** reads the Bronze header, compares it with the prior run's baseline, proposes a typed Silver table, and writes a migration recommendation when columns change.

**Sample input:** the Bronze columns `ticket_id`, `created_at`, `resolved_at`, `cost`, `sla_hours`, plus lineage fields such as `_row_hash`.

**Sample output:** `created_at_parsed TIMESTAMP`, `cost_decimal DECIMAL(12,2)`, and `_row_hash CHAR(64) NOT NULL` in `silver_schema.sql`. On the current run it reported: `No schema drift detected.`

**Honest take:** this saved me roughly 20 minutes of repeatedly translating the raw columns into a Silver contract and remembering lineage fields. It is intentionally rule-based, not a free-form LLM: a newly added column is detected, but its business meaning still needs a human review before applying the proposed migration.

### Data Quality Agent

**What it does:** profiles each Bronze field for null rate, cardinality, and common values; checks `cost` and `sla_hours` for IQR outliers; then writes the reasons behind the Silver validation rules.

**Sample input:** 10,280 Bronze ticket rows with labels such as `Pest`, `pest control`, `CRITICAL`, `hi`, `NULL`, and mixed timestamp formats.

**Sample output:** it found `resolved_at` is null for 48.2% of records, `category` has 111 raw labels, and 6,452 valid `cost` values (with no IQR outliers above `19,851.275`). It then proposes rules such as “normalize category, priority and status labels” because variants would otherwise split workload metrics.

**Honest take:** this saved me the most time—about 30 minutes of manually scanning unique values and deciding which data issues could corrupt Gold metrics. It would be more trouble than it is worth for a tiny, already-clean file; for this messy 10k-row input, the recorded evidence makes each cleaning rule easy to justify.

## What changes at 100x scale

For 1M+ rows arriving daily, I would replace CSV outputs with partitioned Parquet tables in object storage and process them with Spark, DuckDB, or a warehouse. Bronze would be append-only and partitioned by ingestion date; Silver would use incremental `MERGE`/upsert logic keyed by `ticket_id` and row hash instead of loading all data into memory.

I would persist the schema baseline, data-quality metrics, and rejected records in tables, then alert on material drift or threshold breaches (for example, a sudden rise in invalid dates). Finally, I would schedule the stages with an orchestrator such as Airflow or Dagster, add retries and lineage tracking, and build Gold aggregates incrementally rather than recreating every metric on each run.

## Gold datasets and why they matter

1. `ticket_volume_by_category_priority.csv`: reveals demand hotspots and urgent workload by work type.
2. `sla_performance_by_category.csv`: shows resolution speed and breach rates, highlighting process or vendor bottlenecks.
3. `open_backlog_by_building_priority.csv`: supports dispatching and escalation by location and urgency.
