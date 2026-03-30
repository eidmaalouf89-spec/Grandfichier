"""
JANSA GrandFichier Updater — Date parsing utilities (V1)
Adapted from OLD processing/dates.py — minimal changes.
"""
from datetime import datetime, date
from typing import Optional
from processing.config import DATE_FORMAT


def parse_date(raw) -> Optional[date]:
    """Parse a raw cell value into a date. Returns None if unparseable."""
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    try:
        return datetime.strptime(str(raw).strip(), DATE_FORMAT).date()
    except (ValueError, TypeError):
        pass
    # Try ISO format as fallback
    try:
        return datetime.fromisoformat(str(raw).strip()[:10]).date()
    except (ValueError, TypeError):
        return None


def parse_delay(raw) -> Optional[int]:
    """Parse a raw delay/delta cell value into integer days. Returns None if unparseable."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    try:
        return int(str(raw).replace("+", "").strip())
    except (ValueError, TypeError):
        return None


def date_to_str(d: Optional[date]) -> str:
    """Convert a date to ISO string (YYYY-MM-DD), or empty string if None."""
    if d is None:
        return ""
    return d.isoformat()


def str_to_date(s: str) -> Optional[date]:
    """Convert ISO string back to date. Returns None if empty or invalid."""
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def compare_dates(date_a: Optional[date], date_b: Optional[date]) -> int:
    """
    Compare two dates. Returns:
      -1 if date_a < date_b (a is older)
       0 if equal or both None
       1 if date_a > date_b (a is newer)
    None values are treated as oldest possible.
    """
    if date_a is None and date_b is None:
        return 0
    if date_a is None:
        return -1
    if date_b is None:
        return 1
    if date_a < date_b:
        return -1
    if date_a > date_b:
        return 1
    return 0
