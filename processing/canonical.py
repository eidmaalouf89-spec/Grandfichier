"""
JANSA GrandFichier Updater — Canonical key building and normalization (V3.0 / V3.1)

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


# ---------------------------------------------------------------------------
# SAS REF detection and document identity comparison (V3.1 PATCH 11 / 12)
# ---------------------------------------------------------------------------
import re as _re

SAS_REF_PATTERN = _re.compile(
    r'SAS\s*[\n:]*\s*REF|REF\s*SAS|GEMO\s*(?::?\s*)?SAS\s*:\s*REF',
    _re.IGNORECASE,
)

# Words to ignore when comparing document titles
_TITRE_NOISE = {
    'PLAN', 'DE', 'DES', 'DU', 'LA', 'LE', 'LES', 'ET', 'EN',
    'NOTE', 'FICHE', 'TECHNIQUE', 'DOCUMENT', 'PDF', 'ANNEXE',
    'NIVEAU', 'ZONE', 'PH', 'R0', 'R1', 'R2', 'R3', 'R4', 'R5',
    'R6', 'R7', 'R8', 'SS1', 'SS2', 'SS3', 'SS4', 'RDC',
    'AUBERGE', 'HOTEL', 'BUREAU', 'BUREAUX',
}

_WORD_RE = _re.compile(r'[A-Za-z\u00C0-\u00FF]{3,}')


def _extract_significant_words(titre: Optional[str]) -> set:
    """Extract meaningful words from a document title, ignoring noise."""
    if not titre:
        return set()
    words = set(_WORD_RE.findall(str(titre).upper()))
    return words - _TITRE_NOISE


def is_same_sas_ref_document(ged_record, gf_sas_ref_row) -> bool:
    """
    Return True if the GED record is the SAME document that was SAS REF'd
    in the GrandFichier (= disregard — do not update or create new row).
    Return False if it's a different document (= create new row).

    Logic:
      1. NUMERO must match (caller guarantees this)
      2. If GED indice > GF indice → resubmission → NOT the same doc
      3. Title word overlap ≥ 50% → same doc
      4. Response date within 60 days of GF diffusion date → same doc
      5. If title is unknown → conservative: assume same doc
    """
    # 2. Indice check — resubmission has higher indice
    ged_indice = str(getattr(ged_record, 'indice', '') or '').strip().upper()
    gf_indice  = str(gf_sas_ref_row.indice or '').strip().upper()
    if ged_indice and gf_indice and ged_indice > gf_indice:
        return False  # newer revision → not the same refused document

    # 3. Title similarity
    ged_titre = str(getattr(ged_record, 'titre', '') or '').strip()
    ged_words = _extract_significant_words(ged_titre)
    gf_words  = _extract_significant_words(gf_sas_ref_row.titre)

    if not ged_words or not gf_words:
        # Cannot compare names → conservative: assume same document
        return True

    overlap    = ged_words & gf_words
    similarity = len(overlap) / max(len(ged_words), len(gf_words))
    if similarity >= 0.5:
        return True

    # 4. Date proximity
    try:
        from datetime import datetime as _dt
        ged_date_str = str(getattr(ged_record, 'response_date', '') or '')
        ged_date = _dt.fromisoformat(ged_date_str) if ged_date_str else None

        gf_date_raw = gf_sas_ref_row.date_diffusion
        if isinstance(gf_date_raw, _dt):
            gf_date = gf_date_raw
        elif isinstance(gf_date_raw, str) and gf_date_raw:
            gf_date = _dt.fromisoformat(gf_date_raw)
        else:
            gf_date = None

        if ged_date and gf_date:
            delta = abs((ged_date - gf_date).days)
            if delta <= 60:
                return True
    except (ValueError, TypeError, AttributeError):
        pass

    # Different name AND different period → genuinely new
    return False
