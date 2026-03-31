"""
JANSA GrandFichier Updater — GF-Master matching engine (V4.0)

Flow: GrandFichier is the master. For each GF row, look up matching GED responses
by NUMERO. No guessing, no new rows, no SAS REF logic needed.
"""
import logging
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
    When multiple GED responses share the same NUMERO, use field scoring
    to pick those matching INDICE (same revision).

    Returns:
        matched_gf: list of (GFRow, list[CanonicalResponse]) — GF rows with GED data
        unmatched_gf: list of GFRow — GF rows with no GED data (normal, leave untouched)
        orphan_ged: list of CanonicalResponse — GED docs not claimed by any GF row
    """
    matched_gf = []
    unmatched_gf = []
    claimed_ged_ids = set()  # track which GED records were matched

    for gf_row in gf_rows:
        gf_num = normalize_numero(gf_row.numero)

        # OLD sheets: index their GED records (so they don't appear as orphans)
        # but NEVER write to them — skip from matched_gf entirely
        if gf_row.sheet_name.startswith(OLD_SHEET_PREFIX):
            if gf_num:
                ged_candidates = ged_index.find(gf_num)
                for cr in ged_candidates:
                    if _score_ged_to_gf(cr, gf_row) >= 10:
                        claimed_ged_ids.add(id(cr))
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

        # Pick candidates that match INDICE (same revision)
        best_responses = []
        for cr in ged_candidates:
            score = _score_ged_to_gf(cr, gf_row)
            if score >= 10:  # INDICE matches
                best_responses.append(cr)
                claimed_ged_ids.add(id(cr))

        if best_responses:
            matched_gf.append((gf_row, best_responses))
            match_summary.record("GF_MATCHED")
        else:
            # GED has this NUMERO but different INDICE — GF row stays untouched
            unmatched_gf.append(gf_row)
            match_summary.record("GF_INDICE_MISMATCH")

    # Orphan GED docs: in GED but not claimed by any GF row
    orphan_ged = [cr for cr in _flatten_unique(ged_index) if id(cr) not in claimed_ged_ids]

    logger.info(
        "GF lookup complete: %d GF rows matched, %d unmatched, %d orphan GED docs",
        len(matched_gf), len(unmatched_gf), len(orphan_ged),
    )
    return matched_gf, unmatched_gf, orphan_ged


def _score_ged_to_gf(cr: CanonicalResponse, gf_row: GFRow) -> int:
    """Score a GED record against a GF row for disambiguation."""
    score = 0
    doc_key_upper = gf_row.document_key.upper() if gf_row.document_key else ""

    # INDICE +10
    if cr.indice and gf_row.indice:
        if str(cr.indice).strip().upper() == str(gf_row.indice).strip().upper():
            score += 10

    # EMETTEUR +5
    if cr.emetteur and doc_key_upper:
        if str(cr.emetteur).strip().upper() in doc_key_upper:
            score += 5

    # TYPE_DOC +3
    if cr.type_doc and gf_row.type_doc:
        if str(cr.type_doc).strip().upper() == str(gf_row.type_doc).strip().upper():
            score += 3

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
