"""
JANSA GrandFichier Updater — Actor normalization (V1)
Adapted from OLD processing/actors.py — unchanged logic.
Loads actor_map.json and resolves raw GED mission names to canonical entries.
"""
import json
from pathlib import Path
from typing import Optional
from processing.config import DEFAULT_ACTOR_RELEVANT, DEFAULT_ACTOR_FAMILY


def load_actor_map(path: Path) -> dict:
    """Load actor_map.json. Strips _meta key."""
    with open(path, "r", encoding="utf-8") as f:
        am = json.load(f)
    am.pop("_meta", None)
    return am


def resolve_actor(actor_raw: Optional[str], actor_map: dict) -> tuple[dict, bool]:
    """
    Resolve a raw GED mission/actor name to a canonical actor dict.
    Returns (actor_entry, found_in_map).
    If not found, returns a fallback dict and False.
    """
    akey = str(actor_raw).strip() if actor_raw is not None else "_UNKNOWN_"
    if akey in actor_map:
        return actor_map[akey], True
    fallback = {
        "canonical":  akey,
        "prefix":     "_",
        "role":       akey,
        "family":     DEFAULT_ACTOR_FAMILY,
        "relevant":   DEFAULT_ACTOR_RELEVANT,
        "is_moex":    False,
    }
    return fallback, False


def load_mission_map(path: Path) -> dict:
    """
    Load mission_map.json.
    Returns a dict: GED mission name → {gf_names, gf_canonical, family}.
    Strips _meta and _note keys.
    """
    with open(path, "r", encoding="utf-8") as f:
        mm = json.load(f)
    mm.pop("_meta", None)
    mm.pop("_version", None)
    mm.pop("_note", None)
    return mm


def resolve_gf_approbateur(
    ged_mission: Optional[str],
    mission_map: dict,
    gf_row8_names: list[str],
) -> tuple[str, bool]:
    """
    Find the GrandFichier approbateur display name that matches a GED mission name.

    Strategy:
    1. Look up GED mission in mission_map to get candidate GF names.
    2. Try to match each candidate against gf_row8_names (case-insensitive).
    3. If a match is found, return (matched_gf_name, True).
    4. If no match, return ("", False).

    Args:
        ged_mission: raw mission name from GED column 25
        mission_map: loaded mission_map.json content
        gf_row8_names: list of approbateur display names from GrandFichier row 8
    """
    if not ged_mission:
        return "", False

    mission_key = str(ged_mission).strip()
    entry = mission_map.get(mission_key)
    if not entry:
        return "", False

    candidates = entry.get("gf_names", [])
    gf_lower = {name.lower(): name for name in gf_row8_names}

    for candidate in candidates:
        c_lower = candidate.lower()
        if c_lower in gf_lower:
            return gf_lower[c_lower], True
        # Also try partial / contains matching
        for gf_name_lower, gf_name_orig in gf_lower.items():
            if c_lower in gf_name_lower or gf_name_lower in c_lower:
                return gf_name_orig, True

    return "", False
