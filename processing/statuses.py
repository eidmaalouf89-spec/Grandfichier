"""
JANSA GrandFichier Updater — Status normalization (V1)
Adapted from OLD processing/statuses.py — unchanged logic.
Loads status_map.json and resolves raw GED response strings to normalized codes.
"""
import json
from pathlib import Path
from typing import Optional


def load_status_map(path: Path) -> dict:
    """Load status_map.json. Strips _meta key."""
    with open(path, "r", encoding="utf-8") as f:
        sm = json.load(f)
    sm.pop("_meta", None)
    return sm


def resolve_status(tag_raw: Optional[str], status_map: dict) -> tuple[dict, bool]:
    """
    Resolve a raw GED response string to a normalized status entry.
    Returns (status_entry, found_in_map).
    Falls back to _NONE_ entry if not found.
    """
    tkey = str(tag_raw).strip() if tag_raw is not None else "_NONE_"
    if tkey in status_map:
        return status_map[tkey], True
    # Try stripping " - En retard" suffix (common GED variant)
    if " - En retard" in tkey:
        base = tkey.replace(" - En retard", "").strip()
        if base in status_map:
            return status_map[base], True
    return status_map.get("_NONE_", {"code": "NONE", "label": "(absent)", "severity": "non_response"}), False


def get_normalized_code(tag_raw: Optional[str], status_map: dict) -> str:
    """Convenience: return just the normalized code string."""
    entry, _ = resolve_status(tag_raw, status_map)
    return entry.get("code", "NONE")
