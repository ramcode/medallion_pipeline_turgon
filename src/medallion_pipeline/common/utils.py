"""Shared utilities for the medallion pipeline."""

import csv
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from medallion_pipeline.agents import DataQualityAgent


def setup_logging(root: Path) -> None:
    (root / "logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(root / "logs" / "pipeline.log"), logging.StreamHandler()],
        force=True,
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file, restkey="_extra_columns", restval=""))


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    """Replace an output file only after its complete replacement is written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def clean(value: str | None) -> str:
    return "" if DataQualityAgent.missing(value) else (value or "").strip()


def parse_date(value: str) -> datetime | None:
    if not value:
        return None
    value = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        pass
    for pattern in (
        "%d-%b-%Y %H:%M", "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M:%S",
        "%m-%d-%Y %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y",
    ):
        try:
            return datetime.strptime(value, pattern)
        except ValueError:
            continue
    if value.isdigit() and len(value) in {10, 13}:
        seconds = int(value) / (1000 if len(value) == 13 else 1)
        try:
            return datetime.fromtimestamp(seconds, timezone.utc).replace(tzinfo=None)
        except (OSError, OverflowError, ValueError):
            pass
    return None


def parse_decimal(value: str) -> Decimal | None:
    try:
        number = Decimal(value.replace("$", "").replace(",", ""))
        return number if number >= 0 else None
    except (InvalidOperation, AttributeError):
        return None


def valid_ticket_id(ticket_id: str) -> bool:
    return ticket_id.startswith("TKT-") and ticket_id[4:].isdigit()
