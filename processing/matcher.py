"""
JANSA GrandFichier Updater — 4-level cascading key matching engine (V1)

Matches CanonicalResponse records to GrandFichier rows.

Level 1: Full composite key match (EXACT)
Level 2: LOT + TYPE_DOC + NUMERO (normalized, FUZZY)
Level 3: TYPE_DOC + NUMERO within same sheet (PARTIAL)
Level 4: NUMERO-only with trailing-alpha stripping (PARTIAL)
No match → UNMATCHED
"""
import logging
from typing import Optional

from processing.models import CanonicalResponse, GFRow
from processing.canonical import (
    build_gf_key, build_ged_key,
    build_level2_key, build_level3_key, build_level4_key,
    normalize_numero, normalize_lot, normalize_key, _s,
)
from processing.config import (
    CONFIDENCE_EXACT, CONFIDENCE_FUZZY, CONFIDENCE_PARTIAL, CONFIDENCE_NONE,
    MATCH_LEVEL_1, MATCH_LEVEL_2, MATCH_LEVEL_3, MATCH_LEVEL_4, MATCH_NONE,
)

logger = logging.getLogger(__name__)


class GFIndex:
    """
    Pre-built index of GrandFichier rows for fast matching at all 4 cascade levels.
    Build once per pipeline run.
    """

    def __init__(self, gf_rows: list[GFRow]):
        # Level 1: full composite key → GFRow
        self._l1: dict[str, GFRow] = {}
        # Level 2: sheet → (lot_norm + type_doc + numero_norm) → GFRow
        self._l2: dict[str, dict[str, GFRow]] = {}
        # Level 3: sheet → (type_doc + numero_norm) → GFRow
        self._l3: dict[str, dict[str, GFRow]] = {}
        # Level 4: sheet → numero_stripped → GFRow
        self._l4: dict[str, dict[str, GFRow]] = {}

        for row in gf_rows:
            key1 = build_gf_key(row.document_key)
            if key1:
                self._l1[key1] = row

            sheet = row.sheet_name
            if sheet not in self._l2:
                self._l2[sheet] = {}
                self._l3[sheet] = {}
                self._l4[sheet] = {}

            key2 = build_level2_key(row.lot, row.type_doc, row.numero)
            if key2:
                self._l2[sheet][key2] = row

            key3 = build_level3_key(row.type_doc, row.numero)
            if key3:
                self._l3[sheet][key3] = row

            key4 = build_level4_key(row.numero)
            if key4:
                self._l4[sheet][key4] = row

        logger.info(
            "GFIndex built: %d L1 keys, %d sheets indexed",
            len(self._l1), len(self._l2),
        )

    def match(self, cr: CanonicalResponse) -> Optional[GFRow]:
        """
        Try all 4 matching levels. Updates cr.confidence and cr.match_strategy in place.
        Returns matched GFRow or None.
        """
        # Level 1: full key — normalize both sides (strip _, -, space)
        key1 = normalize_key(cr.document_key.upper()) if cr.document_key else ""
        if key1 and key1 in self._l1:
            cr.confidence = CONFIDENCE_EXACT
            cr.match_strategy = MATCH_LEVEL_1
            return self._l1[key1]

        # Level 2: LOT + TYPE_DOC + NUMERO across all sheets
        key2 = build_level2_key(cr.lot, cr.type_doc, cr.numero)
        if key2:
            # Try current sheet first, then all sheets
            candidates = self._search_all_sheets(self._l2, key2)
            if candidates:
                best = candidates[0]
                cr.confidence = CONFIDENCE_FUZZY
                cr.match_strategy = MATCH_LEVEL_2
                return best

        # Level 3: TYPE_DOC + NUMERO
        key3 = build_level3_key(cr.type_doc, cr.numero)
        if key3:
            candidates = self._search_all_sheets(self._l3, key3)
            if candidates:
                best = candidates[0]
                cr.confidence = CONFIDENCE_PARTIAL
                cr.match_strategy = MATCH_LEVEL_3
                return best

        # Level 4: NUMERO-only stripped
        key4 = build_level4_key(cr.numero)
        if key4 and len(key4) >= 4:   # avoid matching on trivially short numbers
            candidates = self._search_all_sheets(self._l4, key4)
            if candidates:
                best = candidates[0]
                cr.confidence = CONFIDENCE_PARTIAL
                cr.match_strategy = MATCH_LEVEL_4
                return best

        cr.confidence = CONFIDENCE_NONE
        cr.match_strategy = MATCH_NONE
        return None

    def _search_all_sheets(
        self,
        index: dict[str, dict[str, GFRow]],
        key: str,
    ) -> list[GFRow]:
        """Search all sheets in the index for a given key. Returns list of matches."""
        results = []
        for sheet_idx in index.values():
            if key in sheet_idx:
                results.append(sheet_idx[key])
        return results


class MatchSummary:
    """Tracks match statistics per level."""

    def __init__(self):
        self._counts: dict[str, int] = {
            MATCH_LEVEL_1: 0,
            MATCH_LEVEL_2: 0,
            MATCH_LEVEL_3: 0,
            MATCH_LEVEL_4: 0,
            MATCH_NONE:    0,
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
            for level in [MATCH_LEVEL_1, MATCH_LEVEL_2, MATCH_LEVEL_3, MATCH_LEVEL_4, MATCH_NONE]
        ]

    def log_summary(self) -> None:
        logger.info("Match summary:")
        for row in self.to_rows():
            logger.info("  %-30s %5d  (%s)", row["match_level"], row["count"], row["percentage"])


def match_all(
    canonical_records: list[CanonicalResponse],
    gf_index: GFIndex,
    summary: Optional[MatchSummary] = None,
) -> tuple[list[CanonicalResponse], list[CanonicalResponse]]:
    """
    Run matching for all CanonicalResponse records.
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
