"""
JANSA GrandFichier Updater — NUMERO-anchored matching engine (V3.0)

Replaces 4-level composite-key cascade with NUMERO-anchored field scoring.

Step 1: Index GF rows by normalized NUMERO
Step 2: Find GED candidates by NUMERO (with single-embedded-zero tolerance)
Step 3: Score candidates by field comparison; pick best
Step 4: No match → UNMATCHED
"""
import logging
from typing import Optional

from processing.models import CanonicalResponse, GFRow
from processing.canonical import normalize_numero, normalize_lot

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Confidence levels (PATCH 3.0)
# ---------------------------------------------------------------------------
CONFIDENCE_HIGH   = "HIGH"
CONFIDENCE_MEDIUM = "MEDIUM"
CONFIDENCE_LOW    = "LOW"
CONFIDENCE_NONE   = "UNMATCHED"

MATCH_NUMERO_HIGH   = "NUMERO_HIGH_SCORE"
MATCH_NUMERO_MEDIUM = "NUMERO_MEDIUM_SCORE"
MATCH_NUMERO_LOW    = "NUMERO_LOW_SCORE"
MATCH_NONE          = "UNMATCHED"

# Field score weights
SCORE_INDICE     = 10
SCORE_EMETTEUR   = 5
SCORE_TYPE_DOC   = 3
SCORE_LOT        = 2
SCORE_SPECIALITE = 1
SCORE_BATIMENT   = 1


class GFNumeroIndex:
    """
    Pre-built index of GrandFichier rows by normalized NUMERO.

    Index structure: dict[str, list[GFRow]]
    NUMERO is the real document anchor. Fields are used only for disambiguation
    when multiple GF rows share the same NUMERO.
    """

    def __init__(self, gf_rows: list[GFRow]):
        # normalized NUMERO → list of GF rows with that NUMERO
        self._index: dict[str, list[GFRow]] = {}
        for row in gf_rows:
            num = normalize_numero(row.numero)
            if num:
                self._index.setdefault(num, []).append(row)

        logger.info(
            "GFNumeroIndex built: %d unique NUMEROs from %d rows",
            len(self._index), len(gf_rows),
        )

    def _find_candidates(self, ged_numero: str) -> list[GFRow]:
        """
        Find GF candidates for a normalized GED NUMERO.
        Applies single-embedded-zero tolerance when no direct match is found.
        """
        # Strict match
        candidates = self._index.get(ged_numero, [])
        if candidates:
            return candidates

        # Tolerance A: remove one internal '0' from GED NUMERO
        for i, c in enumerate(ged_numero):
            if c == '0' and i > 0:
                variant = ged_numero[:i] + ged_numero[i + 1:]
                if variant in self._index:
                    return self._index[variant]

        # Tolerance B: GF has an extra '0' that, when removed, equals GED NUMERO
        for gf_num, rows in self._index.items():
            for i, c in enumerate(gf_num):
                if c == '0' and i > 0:
                    if gf_num[:i] + gf_num[i + 1:] == ged_numero:
                        return rows
                    break  # only test the first internal zero per gf_num

        return []

    def _score(self, cr: CanonicalResponse, gf_row: GFRow) -> int:
        """
        Score a GF candidate row against a GED CanonicalResponse.
        Fields not directly available on GFRow are checked via substring on document_key.
        """
        score = 0
        doc_key_upper = gf_row.document_key.upper() if gf_row.document_key else ""

        # INDICE +10 — direct field comparison
        if cr.indice and gf_row.indice:
            if str(cr.indice).strip().upper() == str(gf_row.indice).strip().upper():
                score += SCORE_INDICE

        # EMETTEUR +5 — substring presence in composite document key
        if cr.emetteur and doc_key_upper:
            if str(cr.emetteur).strip().upper() in doc_key_upper:
                score += SCORE_EMETTEUR

        # TYPE_DOC +3 — direct field comparison
        if cr.type_doc and gf_row.type_doc:
            if str(cr.type_doc).strip().upper() == str(gf_row.type_doc).strip().upper():
                score += SCORE_TYPE_DOC

        # LOT +2 — normalized comparison (strip leading alpha prefix)
        if cr.lot and gf_row.lot:
            if normalize_lot(cr.lot) == normalize_lot(gf_row.lot):
                score += SCORE_LOT

        # SPECIALITE +1 — substring in document_key (field not on GFRow)
        if hasattr(cr, 'specialite') and cr.specialite and doc_key_upper:
            if str(cr.specialite).strip().upper() in doc_key_upper:
                score += SCORE_SPECIALITE

        # BATIMENT +1 — substring in document_key
        if cr.batiment and doc_key_upper:
            if str(cr.batiment).strip().upper() in doc_key_upper:
                score += SCORE_BATIMENT

        return score

    def match(self, cr: CanonicalResponse) -> Optional[GFRow]:
        """
        Match a CanonicalResponse to the best-scoring GFRow.
        Updates cr.confidence and cr.match_strategy in place.
        Returns the matched GFRow or None.
        """
        ged_numero = normalize_numero(cr.numero)
        if not ged_numero:
            cr.confidence = CONFIDENCE_NONE
            cr.match_strategy = MATCH_NONE
            return None

        candidates = self._find_candidates(ged_numero)
        if not candidates:
            cr.confidence = CONFIDENCE_NONE
            cr.match_strategy = MATCH_NONE
            return None

        if len(candidates) == 1:
            best_row = candidates[0]
            best_score = self._score(cr, best_row)
        else:
            # Score all candidates, pick highest
            scored = [(self._score(cr, row), row) for row in candidates]
            scored.sort(key=lambda x: x[0], reverse=True)
            best_score, best_row = scored[0]

        # Assign confidence tier from score
        if best_score >= 15:
            cr.confidence = CONFIDENCE_HIGH
            cr.match_strategy = MATCH_NUMERO_HIGH
        elif best_score >= 10:
            cr.confidence = CONFIDENCE_MEDIUM
            cr.match_strategy = MATCH_NUMERO_MEDIUM
        else:
            cr.confidence = CONFIDENCE_LOW
            cr.match_strategy = MATCH_NUMERO_LOW
            logger.warning(
                "Low-confidence match (score=%d): doc=%s → sheet=%s row=%d",
                best_score, cr.document_key, best_row.sheet_name, best_row.row_number,
            )

        return best_row


class MatchSummary:
    """Tracks match statistics per confidence tier."""

    def __init__(self):
        self._counts: dict[str, int] = {
            MATCH_NUMERO_HIGH:   0,
            MATCH_NUMERO_MEDIUM: 0,
            MATCH_NUMERO_LOW:    0,
            MATCH_NONE:          0,
        }

    def record(self, strategy: str) -> None:
        if strategy in self._counts:
            self._counts[strategy] += 1
        else:
            self._counts[MATCH_NONE] += 1

    @property
    def total_matched(self) -> int:
        return sum(v for k, v in self._counts.items() if k != MATCH_NONE)

    @property
    def total_unmatched(self) -> int:
        return self._counts[MATCH_NONE]

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
            for level in [MATCH_NUMERO_HIGH, MATCH_NUMERO_MEDIUM, MATCH_NUMERO_LOW, MATCH_NONE]
        ]

    def log_summary(self) -> None:
        logger.info("Match summary:")
        for row in self.to_rows():
            logger.info("  %-30s %5d  (%s)", row["match_level"], row["count"], row["percentage"])


def match_all(
    canonical_records: list[CanonicalResponse],
    gf_index: GFNumeroIndex,
    summary: Optional[MatchSummary] = None,
) -> tuple[list[CanonicalResponse], list[CanonicalResponse]]:
    """
    Run NUMERO-anchored matching for all CanonicalResponse records.
    Updates each record's .confidence, .match_strategy, .gf_sheet, .gf_row in place.

    Returns:
        (matched_records, unmatched_records)
    """
    matched = []
    unmatched = []

    for cr in canonical_records:
        gf_row = gf_index.match(cr)
        if gf_row is not None:
            cr.gf_sheet = gf_row.sheet_name
            cr.gf_row = gf_row.row_number
            matched.append(cr)
        else:
            unmatched.append(cr)
        if summary:
            summary.record(cr.match_strategy)

    logger.info(
        "Matching complete: %d matched, %d unmatched (total %d)",
        len(matched), len(unmatched), len(canonical_records),
    )
    return matched, unmatched
