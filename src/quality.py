"""Pure Python data-quality helpers. No Spark."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Sequence

ISO_FORMATS = (
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
)


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip() == ""


def parse_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if is_blank(value):
        return None
    text = str(value).strip()
    sign = ""
    if text[0] in "+-":
        sign, text = text[0], text[1:]
        if sign == "+":
            sign = ""
    if text.isdigit():
        return int(f"{sign}{text}")
    return None


def parse_iso(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if is_blank(value):
        return None
    text = str(value).strip()
    candidates = [text]
    if text.endswith("Z"):
        candidates.extend((text[:-1] + "+00:00", text[:-1] + "+0000"))
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass
        for fmt in ISO_FORMATS:
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
    return None


def quality_reasons(
    row_dict: Mapping[str, Any],
    dept_ids: set[int],
    job_ids: set[int],
) -> list[str]:
    reasons: list[str] = []
    if parse_int(row_dict.get("id")) is None:
        reasons.append("null_id")
    if is_blank(row_dict.get("name")):
        reasons.append("null_or_empty_name")
    if parse_iso(row_dict.get("datetime")) is None:
        reasons.append("invalid_datetime")

    department_id = parse_int(row_dict.get("department_id"))
    if department_id is None:
        reasons.append("null_department_id")
    elif department_id not in dept_ids:
        reasons.append("fk_department")

    job_id = parse_int(row_dict.get("job_id"))
    if job_id is None:
        reasons.append("null_job_id")
    elif job_id not in job_ids:
        reasons.append("fk_job")

    return reasons


def employee_tiebreak_key(row: Mapping[str, Any]) -> tuple:
    """Higher key wins: latest file date, then ingestion time, then source_file.

    A null ``source_file_date`` loses to any real date.
    """
    file_date = row.get("source_file_date")
    ingested = row.get("ingestion_timestamp")
    return (
        file_date is not None,
        file_date or date.min,
        ingested or datetime.min,
        row.get("source_file") or "",
    )


def pick_employee_winner(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    if not rows:
        raise ValueError("pick_employee_winner requires at least one row")
    return max(rows, key=employee_tiebreak_key)
