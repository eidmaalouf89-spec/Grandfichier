"""
JANSA GrandFichier Updater — Canonical key building and normalization (V1)

Builds composite document keys and normalizes field values to handle
encoding discrepancies between GED and GrandFichier.
"""
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LOT prefix normalization
# GED uses prefixes like I003, A031 — GrandFichier may use G003, 031 etc.
# ---------------------------------------------------------------------------
_LOT_PREFIX_STRIP = re.compile(r"^[A-Z]{1,2}(?=\d)")

def normalize_lot(lot_raw: Optional[str]) -> str:
    """
    Normalize a LOT code for matching purposes.
    Strips leading alpha prefix and zero-pads the numeric portion.
    Examples:
      "I003" → "003"
      "A031" → "031"
      "G003" → "003"
      "031"  → "031"
    """
    if not lot_raw:
        return ""
    s = str(lot_raw).strip().upper()
    # Strip leading letter(s) prefix
    m = _LOT_PREFIX_STRIP.match(s)
    if m:
        s = s[m.end():]
    return s


def normalize_numero(numero_raw: Optional[str]) -> str:
    """
    Normalize a document NUMERO for matching.
    - Convert to string, strip whitespace
    - Strip trailing alpha characters (e.g. "028000A" → "028000")
    - Zero-pad to 6 digits
    - Remove leading zeros for fallback: "028000" → "28000" handled in caller
    """
    if numero_raw is None:
        return ""
    s = str(numero_raw).strip()
    # Strip trailing alpha (e.g. revision suffix accidentally appended)
    s = re.sub(r"[A-Za-z]+$", "", s).strip()
    # Try to zero-pad integer part to 6 digits
    try:
        return str(int(s)).zfill(6)
    except ValueError:
        return s


def normalize_numero_stripped(numero_raw: Optional[str]) -> str:
    """
    More aggressive normalization: strip trailing alpha AND leading zeros.
    Used for Level 4 matching.
    """
    n = normalize_numero(numero_raw)
    try:
        return str(int(n))   # removes leading zeros
    except ValueError:
        return n


def normalize_text(s: Optional[str]) -> str:
    """Strip, uppercase, remove extra whitespace."""
    if not s:
        return ""
    return " ".join(str(s).strip().upper().split())


# ---------------------------------------------------------------------------
# Composite key building
# ---------------------------------------------------------------------------

def build_ged_key(row_values: dict) -> str:
    """
    Build the full composite key from GED row values.
    Format: AFFAIRE + PROJET + BATIMENT + PHASE + EMETTEUR + SPECIALITE + LOT + TYPE_DOC + ZONE + NIVEAU + NUMERO

    row_values keys: affaire, projet, batiment, phase, emetteur, specialite, lot, type_doc, zone, niveau, numero
    """
    parts = [
        _s(row_values.get("affaire")),
        _s(row_values.get("projet")),
        _s(row_values.get("batiment")),
        _s(row_values.get("phase")),
        _s(row_values.get("emetteur")),
        _s(row_values.get("specialite")),
        _s(row_values.get("lot")),
        _s(row_values.get("type_doc")),
        _s(row_values.get("zone")),
        _s(row_values.get("niveau")),
        _s(row_values.get("numero")),
    ]
    return "".join(parts)


def build_gf_key(cell_value: Optional[str]) -> str:
    """
    Normalize a GrandFichier DOCUMENT column A value for comparison.
    Just strips whitespace and uppercases.
    """
    if not cell_value:
        return ""
    return str(cell_value).strip().upper()


def _s(v) -> str:
    """Safe string — None → empty string, else strip + upper."""
    if v is None:
        return ""
    return str(v).strip().upper()


# ---------------------------------------------------------------------------
# Level 2 partial key: LOT + TYPE_DOC + NUMERO (normalized)
# ---------------------------------------------------------------------------

def build_level2_key(lot: str, type_doc: str, numero: str) -> str:
    """Normalized LOT (no prefix) + TYPE_DOC + zero-padded NUMERO."""
    return normalize_lot(lot) + _s(type_doc) + normalize_numero(numero)


# ---------------------------------------------------------------------------
# Level 3 partial key: TYPE_DOC + NUMERO
# ---------------------------------------------------------------------------

def build_level3_key(type_doc: str, numero: str) -> str:
    return _s(type_doc) + normalize_numero(numero)


# ---------------------------------------------------------------------------
# Level 4 partial key: stripped NUMERO only
# ---------------------------------------------------------------------------

def build_level4_key(numero: str) -> str:
    return normalize_numero_stripped(numero)
