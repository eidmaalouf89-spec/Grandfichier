"""
JANSA GrandFichier Updater — GF-Master matching engine (V4.0)

Flow: GrandFichier is the master. For each GF row, look up matching GED responses
by NUMERO. No guessing, no new rows, no SAS REF logic needed.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from processing.models import CanonicalResponse, GFRow
from processing.canonical import normalize_numero, normalize_lot

logger = logging.getLogger(__name__)


class GEDNumeroIndex:
    """
    Index GED CanonicalResponse records by normalized NUMERO.
    Structure: normalized_numero → list[CanonicalResponse]
    """
    def __init__(self, ged_records: list[CanonicalResponse]):
        self._index: dict[str, list[CanonicalResponse]] = {}
        for cr in ged_records:
            num = normalize_numero(cr.numero)
            if num:
                self._index.setdefault(num, []).append(cr)
        logger.info(
            "GEDNumeroIndex built: %d unique NUMEROs from %d records",
            len(self._index), len(ged_records),
        )

    def find(self, numero: str) -> list[CanonicalResponse]:
        """
        Find GED responses for a given NUMERO.
        Strict match with leading-zero normalization.
        Falls back to single-embedded-zero tolerance.
        """
        num = normalize_numero(numero)
        if not num:
            return []

        # Strict match
        if num in self._index:
            return self._index[num]

        # Tolerance: remove one internal '0' from GF NUMERO
        for i, c in enumerate(num):
            if c == '0' and i > 0:
                variant = num[:i] + num[i + 1:]
                if variant in self._index:
                    return self._index[variant]

        # Tolerance: GED has extra '0'
        for ged_num, records in self._index.items():
            for i, c in enumerate(ged_num):
                if c == '0' and i > 0:
                    if ged_num[:i] + ged_num[i + 1:] == num:
                        return records
                    break
        return []

    @property
    def all_numeros(self) -> set[str]:
        """All normalized NUMEROs in the index."""
        return set(self._index.keys())


OLD_SHEET_PREFIX = "OLD "


class MatchSummary:
    """Tracks match statistics for GF-master lookup."""

    LEVELS = ["GF_MATCHED", "GF_INDICE_MISMATCH", "GF_NO_GED", "GF_OLD_SHEET_SKIP"]

    def __init__(self):
        self._counts: dict[str, int] = {lvl: 0 for lvl in self.LEVELS}

    def record(self, strategy: str) -> None:
        if strategy in self._counts:
            self._counts[strategy] += 1
        else:
            self._counts["GF_NO_GED"] += 1

    @property
    def total_matched(self) -> int:
        return self._counts.get("GF_MATCHED", 0)

    @property
    def total_unmatched(self) -> int:
        return self._counts.get("GF_NO_GED", 0) + self._counts.get("GF_INDICE_MISMATCH", 0)

    @property
    def total(self) -> int:
        return sum(self._counts.values())

    def to_rows(self) -> list[dict]:
        total = self.total or 1
        return [
            {
                "match_level": level,
                "count": self._counts.get(level, 0),
                "percentage": f"{100 * self._counts.get(level, 0) / total:.1f}%",
            }
            for level in self.LEVELS
        ]

    def log_summary(self) -> None:
        logger.info("Match summary:")
        for row in self.to_rows():
            logger.info("  %-30s %5d  (%s)", row["match_level"], row["count"], row["percentage"])


def lookup_ged_for_gf(
    gf_rows: list[GFRow],
    ged_index: GEDNumeroIndex,
    match_summary: MatchSummary,
    anomaly_logger,
) -> tuple[list, list, list]:
    """
    For each GF row, look up matching GED responses by NUMERO.

    CRITICAL: The same GED response CAN and SHOULD be matched to
    MULTIPLE GF rows if they share the same NUMERO+INDICE. This is
    normal — the GF has one row per LOT variant for the same document.

    Returns:
        matched_gf: list of (GFRow, list[CanonicalResponse])
        unmatched_gf: list of GFRow
        orphan_ged: list of CanonicalResponse — GED docs not matching ANY GF row
    """
    matched_gf = []
    unmatched_gf = []

    # Track which (NUMERO, INDICE) pairs were matched to at least one GF row.
    # Used ONLY for orphan detection — NOT for filtering during matching.
    matched_ged_doc_ids: set[tuple[str, str]] = set()

    for gf_row in gf_rows:
        gf_num = normalize_numero(gf_row.numero)

        # OLD sheets: claim their GED doc IDs (so they don't appear as orphans)
        # but NEVER write to them — skip from matched_gf entirely
        if gf_row.sheet_name.startswith(OLD_SHEET_PREFIX):
            if gf_num:
                ged_candidates = ged_index.find(gf_num)
                for cr in ged_candidates:
                    matched_ged_doc_ids.add((normalize_numero(cr.numero), cr.indice.upper()))
            match_summary.record("GF_OLD_SHEET_SKIP")
            continue  # DO NOT add to matched_gf

        if not gf_num:
            unmatched_gf.append(gf_row)
            continue

        ged_candidates = ged_index.find(gf_num)
        if not ged_candidates:
            unmatched_gf.append(gf_row)
            match_summary.record("GF_NO_GED")
            continue

        # Find all GED responses where INDICE matches this GF row
        best_responses = []
        for cr in ged_candidates:
            score = _score_ged_to_gf(cr, gf_row)
            if score >= 10:  # INDICE matches (10 points)
                best_responses.append(cr)
                matched_ged_doc_ids.add((normalize_numero(cr.numero), cr.indice.upper()))

        if best_responses:
            matched_gf.append((gf_row, best_responses))
            match_summary.record("GF_MATCHED")
        else:
            # GED has this NUMERO but no matching INDICE
            unmatched_gf.append(gf_row)
            match_summary.record("GF_INDICE_MISMATCH")

    # Orphan GED docs: (NUMERO, INDICE) pairs never matched to any GF row
    all_ged_doc_ids: set[tuple[str, str]] = set()
    orphan_ged = []
    seen_orphan: set[tuple[str, str]] = set()

    for num in ged_index.all_numeros:
        for cr in ged_index.find(num):
            doc_id = (normalize_numero(cr.numero), cr.indice.strip().upper())
            all_ged_doc_ids.add(doc_id)
            if doc_id not in matched_ged_doc_ids and doc_id not in seen_orphan:
                seen_orphan.add(doc_id)
                orphan_ged.append(cr)

    logger.info(
        "GF lookup complete: %d GF rows matched, %d unmatched, %d orphan GED docs "
        "(from %d unique GED doc IDs, %d matched to at least one GF row)",
        len(matched_gf), len(unmatched_gf), len(orphan_ged),
        len(all_ged_doc_ids), len(matched_ged_doc_ids),
    )
    return matched_gf, unmatched_gf, orphan_ged


# Maximum allowed gap (days) between GF submittal date and GED response date.
# If the GED response is older than this many days before the GF submittal,
# it's almost certainly from a previous workflow with a reused NUMERO.
_MAX_DATE_GAP_DAYS = 30


def _parse_any_date(date_str: str):
    """Parse a date string in various formats. Returns datetime or None."""
    if not date_str:
        return None
    date_str = str(date_str).strip()
    # Handle datetime objects from openpyxl
    if hasattr(date_str, 'date'):
        return date_str
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(date_str[:len(fmt.replace('%', 'x'))], fmt)
        except (ValueError, TypeError):
            continue
    # Try ISO prefix
    try:
        return datetime.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


def _dates_within_range(gf_date_str: str, ged_response_date_str: str, max_days: int = _MAX_DATE_GAP_DAYS) -> bool:
    """
    Check if the GED response date is plausible for this GF submittal.
    Returns True if the response is within max_days before the submittal date,
    or at any time after it (responses can come after submission).
    Returns True if either date is unparseable (fail-open: don't block on missing data).
    """
    gf_date = _parse_any_date(gf_date_str)
    ged_date = _parse_any_date(ged_response_date_str)
    if gf_date is None or ged_date is None:
        return True  # Can't compare — allow the match (fail-open)
    # GED response must not be more than max_days BEFORE the GF submittal
    # (a response from Feb 2025 for a submittal from Mar 2026 is clearly wrong)
    earliest_allowed = gf_date - timedelta(days=max_days)
    return ged_date >= earliest_allowed


def _score_ged_to_gf(cr: CanonicalResponse, gf_row: GFRow) -> int:
    """Score a GED record against a GF row for disambiguation."""
    score = 0
    doc_key_upper = gf_row.document_key.upper() if gf_row.document_key else ""

    # HARD FILTER 1: TYPE_DOC — if both have it and they differ, reject immediately.
    # Prevents cross-contamination when NUMEROs are reused with different doc types.
    if cr.type_doc and gf_row.type_doc:
        if str(cr.type_doc).strip().upper() != str(gf_row.type_doc).strip().upper():
            return 0
        score += 3

    # HARD FILTER 2: Date proximity — GED response date must be plausible for this GF submittal.
    # Use date_diff (Date diffusion) as the GF reference; fall back to date_recept.
    gf_ref_date = gf_row.date_diff or gf_row.date_recept
    ged_ref_date = cr.response_date
    if not _dates_within_range(gf_ref_date, ged_ref_date):
        return 0

    # INDICE +10
    if cr.indice and gf_row.indice:
        if str(cr.indice).strip().upper() == str(gf_row.indice).strip().upper():
            score += 10

    # EMETTEUR +5
    if cr.emetteur and doc_key_upper:
        if str(cr.emetteur).strip().upper() in doc_key_upper:
            score += 5

    # LOT +2
    if cr.lot and gf_row.lot:
        if normalize_lot(cr.lot) == normalize_lot(gf_row.lot):
            score += 2

    return score


def _flatten_unique(ged_index: GEDNumeroIndex) -> list[CanonicalResponse]:
    """Get one representative CanonicalResponse per unique document (NUMERO+INDICE)."""
    seen = set()
    result = []
    for num in ged_index.all_numeros:
        for cr in ged_index.find(num):
            doc_id = (cr.numero, cr.indice)
            if doc_id not in seen:
                seen.add(doc_id)
                result.append(cr)
    return result
