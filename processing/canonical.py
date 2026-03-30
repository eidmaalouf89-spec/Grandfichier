"""
JANSA GrandFichier Updater — Canonical key building and normalization (V3.0)

V3.0: Composite key builders removed (build_gf_key, build_ged_key, build_level2/3/4_key).
NUMERO-anchored matching no longer relies on composite keys.

Retained utilities:
  - normalize_numero() — leading-zero stripping (the real anchor)
  - normalize_lot()    — lot prefix normalization for field scoring
  - normalize_key()    — separator stripping for display/logging only
  - normalize_text()   — strip + uppercase
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
    Strips leading alpha prefix and returns the numeric portion.
    Examples:
      "I003" → "003"
      "A031" → "031"
      "G003" → "003"
      "031"  → "031"
    """
    if not lot_raw:
        return ""
    s = str(lot_raw).strip().upper()
    m = _LOT_PREFIX_STRIP.match(s)
    if m:
        s = s[m.end():]
    return s


def normalize_numero(numero_raw: Optional[str]) -> str:
    """
    Normalize a document NUMERO for NUMERO-anchored matching.
    Rule: leading-zero stripping only — str(int(float(str(n)))).
    No other transformations.

    Examples:
      "028000" → "28000"
      "28000"  → "28000"
      "028000.0" → "28000"
      ""       → ""
    """
    if numero_raw is None:
        return ""
    s = str(numero_raw).strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except (ValueError, OverflowError):
        return s


def normalize_text(s: Optional[str]) -> str:
    """Strip, uppercase, remove extra whitespace."""
    if not s:
        return ""
    return " ".join(str(s).strip().upper().split())


def normalize_key(raw_key: str) -> str:
    """
    Strip separators for display/logging comparison (_, -, space).
    NOTE: Used for display and logging only — not for NUMERO-anchored matching.
    """
    if not raw_key:
        return ""
    return raw_key.replace('_', '').replace('-', '').replace(' ', '').strip()


def _s(v) -> str:
    """Safe string — None → empty string, else strip + upper."""
    if v is None:
        return ""
    return str(v).strip().upper()
