"""
JANSA GrandFichier Updater — Actor normalization (V1 / PATCH 3.0)
Adapted from OLD processing/actors.py — unchanged logic.
Loads actor_map.json and resolves raw GED mission names to canonical entries.

V3.0 additions:
  - MOEX_MISSIONS set
  - has_moex_mission() for MOEX-only filter
"""
import json
from pathlib import Path
from typing import Optional
from processing.config import DEFAULT_ACTOR_RELEVANT, DEFAULT_ACTOR_FAMILY

# ---------------------------------------------------------------------------
# MOEX mission names — these are the four variants of Maîtrise d'Oeuvre EXE
# (PATCH 3.0 PATCH 2)
# ---------------------------------------------------------------------------
MOEX_MISSIONS = {
    "0-Maître d'Oeuvre EXE",
    "A-Maître d'Oeuvre EXE",
    "B-Maître d'Oeuvre EXE",
    "H-Maître d'Oeuvre EXE",
}


def has_moex_mission(
    record,
    moex_docs: set,
) -> bool:
    """
    Check if this document (identified by NUMERO + INDICE) has at least one MOEX response.

    Args:
        record: CanonicalResponse record to check
        moex_docs: Pre-built set of (numero, indice) pairs that have a MOEX mission.
                   Built once before filtering: see run_update_grandfichier.py.

    Returns True if the document should be processed (has a MOEX response).
    """
    return (record.numero, record.indice) in moex_docs


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
    Load mission_map.json (v2.0 bidirectional format).
    Returns the full dict with all nested keys intact:
      - ged_to_group: GED mission name → group name
      - group_to_gf_appro: group name → list of GF approbateur name variants
      - special_groups: group → "VISA_GLOBAL" | "SKIP"
      - no_gf_column: list of groups with no GF column
      - group_role: group → "primary" | "secondary" | "not_concerned"
    """
    with open(path, "r", encoding="utf-8") as f:
        mm = json.load(f)
    mm.pop("_meta", None)
    mm.pop("_version", None)
    mm.pop("_note", None)
    return mm


def get_mission_group(ged_mission: Optional[str], mission_map: dict) -> str:
    """
    Resolve a raw GED mission name to its unified group name.
    Returns "" if not found in the mapping.
    """
    if not ged_mission:
        return ""
    return mission_map.get("ged_to_group", {}).get(str(ged_mission).strip(), "")


def resolve_gf_approbateur(
    ged_mission: Optional[str],
    mission_map: dict,
    gf_row8_names: list[str],
) -> tuple[str, bool]:
    """
    Find the GrandFichier approbateur display name that matches a GED mission name.

    Strategy (v2.0 two-step lookup):
    1. GED mission → group (via ged_to_group)
    2. Group → candidate GF names (via group_to_gf_appro)
    3. Match candidates against gf_row8_names (case-insensitive, then partial)

    Returns (matched_gf_name, True) if found, ("", False) otherwise.
    """
    if not ged_mission:
        return "", False

    group = get_mission_group(ged_mission, mission_map)
    if not group:
        return "", False

    candidates = mission_map.get("group_to_gf_appro", {}).get(group, [])
    if not candidates:
        return "", False

    gf_lower = {name.lower(): name for name in gf_row8_names}

    for candidate in candidates:
        c_lower = candidate.lower()
        if c_lower in gf_lower:
            return gf_lower[c_lower], True
        # Partial / contains matching as fallback
        for gf_name_lower, gf_name_orig in gf_lower.items():
            if c_lower in gf_name_lower or gf_name_lower in c_lower:
                return gf_name_orig, True

    return "", False
