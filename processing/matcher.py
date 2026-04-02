"""
JANSA GrandFichier Updater — GF-Master matching engine (V4.0)

Flow: GrandFichier is the master. For each GF row, look up matching GED responses
by NUMERO. No guessing, no new rows, no SAS REF logic needed.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

from processing.models import CanonicalResponse, GFRow, AnomalyRecord
from processing.canonical import normalize_numero, normalize_lot
from processing.config import OLD_SHEET_PREFIX, FUZZY_MATCH_THRESHOLD

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


class MatchSummary:
    """Tracks match statistics for GF-master lookup."""

    LEVELS = [
        "GF_MATCHED", "GF_FUZZY_MATCH", "GF_TYPE_DOC_OVERRIDE",
        "GF_INDICE_MISMATCH", "GF_NO_GED",
        "GF_OLD_SHEET_SKIP", "GF_ANCIEN_SKIP", "GF_SAS_REF_SKIP",
        "GF_NO_MOEX_YET", "GF_KNOWN_SKIP",
    ]

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


import re as _sas_re
_SAS_REF_PATTERN = _sas_re.compile(
    r'sas[\s:._-]*ref'   # "SAS REF", "SAS:REF", "SASREF", "SAS.REF"
    r'|ref\s+sas\b'      # reversed order: "REF SAS"  (e.g. "GEMO : REF SAS")
    r'|\bsa\s*:\s*ref\b' # "SA : REF" / "SA  : REF"  (typo for "SAS : REF")
    r'|\.ref\b|\.sas\b|sasref',
    _sas_re.IGNORECASE,
)


def _is_sas_ref(gf_row: "GFRow") -> bool:
    """
    Return True if this GF row is a SAS REF document (rejected at visa synthesis,
    awaiting lot resubmission). These are legitimately absent from the GED.

    Detection: looks for SAS REF / .REF / .SAS / SASREF / SAS:REF patterns
    in visa_global and observations fields.
    """
    for field in (gf_row.visa_global or "", gf_row.observations or ""):
        if _SAS_REF_PATTERN.search(str(field)):
            return True
    return False


def lookup_ged_for_gf(
    gf_rows: list[GFRow],
    ged_index: GEDNumeroIndex,
    match_summary: MatchSummary,
    anomaly_logger,
    ged_index_all: Optional[GEDNumeroIndex] = None,
    known_skip_numeros: Optional[set] = None,
) -> tuple[list, list, list]:
    """
    For each GF row, look up matching GED responses by NUMERO.

    CRITICAL: The same GED response CAN and SHOULD be matched to
    MULTIPLE GF rows if they share the same NUMERO+INDICE. This is
    normal — the GF has one row per LOT variant for the same document.

    Args:
        ged_index_all: Optional secondary index built from ALL GED records
            (before the MOEX-only filter). When provided, rows that have no
            match in ``ged_index`` (MOEX-filtered) but DO exist in
            ``ged_index_all`` are classified as GF_NO_MOEX_YET instead of
            GF_NO_GED — they are known to exist in the GED, just without a
            MOEX mission response yet.
        known_skip_numeros: Optional set of normalized NUMEROs that are
            permanently excluded from error classification. Rows matching these
            NUMEROs (that cannot be matched via TYPE_DOC override or any other
            recovery path) are classified as GF_KNOWN_SKIP instead of
            GF_NO_GED or GF_INDICE_MISMATCH.

    Returns:
        matched_gf: list of (GFRow, list[CanonicalResponse])
        unmatched_gf: list of (GFRow, str) — (row, category) where category is
            "GF_NO_GED" or "GF_INDICE_MISMATCH"
        orphan_ged: list of CanonicalResponse — GED docs not matching ANY GF row
    """
    matched_gf = []
    unmatched_gf: list[tuple] = []

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

        # ANCIEN: superseded document revision — not an error, just skip
        if gf_row.ancien:
            match_summary.record("GF_ANCIEN_SKIP")
            continue

        if not gf_num:
            unmatched_gf.append((gf_row, "GF_NO_GED"))
            continue

        ged_candidates = ged_index.find(gf_num)
        if not ged_candidates:
            # Check: does this NUMERO exist in the full GED (pre-MOEX filter)?
            # If yes, MOEX simply hasn't responded yet — not an error.
            if ged_index_all is not None and ged_index_all.find(gf_num):
                match_summary.record("GF_NO_MOEX_YET")
                continue
            # Check: is this row marked as SAS REF (rejected at visa synthesis)?
            if _is_sas_ref(gf_row):
                match_summary.record("GF_SAS_REF_SKIP")
                continue
            # Check: is this NUMERO on the known-skip list (user-confirmed no GED ref)?
            if known_skip_numeros and gf_num in known_skip_numeros:
                match_summary.record("GF_KNOWN_SKIP")
                continue
            unmatched_gf.append((gf_row, "GF_NO_GED"))
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
            # ── FUZZY FALLBACK — INDICE does not match exactly ────────────────
            # Score every GED candidate with the multi-signal fuzzy scorer.
            fuzzy_scored: list[tuple[float, dict, CanonicalResponse]] = []
            for cr in ged_candidates:
                fscore, fdetail = _fuzzy_score_ged_to_gf(cr, gf_row)
                if fscore >= FUZZY_MATCH_THRESHOLD:
                    fuzzy_scored.append((fscore, fdetail, cr))

            if fuzzy_scored:
                # Sort best score first
                fuzzy_scored.sort(key=lambda x: -x[0])
                best_score, best_detail, _ = fuzzy_scored[0]

                # Collect all candidates sharing the best-scoring GED indice
                best_ged_indice = fuzzy_scored[0][2].indice.strip().upper()
                fuzzy_responses = [
                    cr for _, _, cr in fuzzy_scored
                    if cr.indice.strip().upper() == best_ged_indice
                ]

                # Tag each response — writer sees FUZZY confidence
                for cr in fuzzy_responses:
                    cr.confidence     = "FUZZY"
                    cr.match_strategy = f"FUZZY_INDICE_FALLBACK score={best_score:.1f}"
                    cr.parse_warnings.append(
                        f"FUZZY MATCH: GF indice='{gf_row.indice}' "
                        f"→ GED indice='{best_ged_indice}' "
                        f"(score={best_score:.1f}) detail={best_detail}"
                    )
                    matched_ged_doc_ids.add(
                        (normalize_numero(cr.numero), cr.indice.upper())
                    )

                matched_gf.append((gf_row, fuzzy_responses))
                match_summary.record("GF_FUZZY_MATCH")

                # Log as WARNING-level recoverable anomaly (visible in anomaly_log.json)
                anomaly_logger.log(AnomalyRecord(
                    anomaly_type="INDICE_MISMATCH_RECOVERED",
                    severity="WARNING",
                    source_type="GED",
                    source_file=fuzzy_responses[0].source_file,
                    source_row_or_page=fuzzy_responses[0].source_row_or_page,
                    document_key=gf_row.document_key,
                    description=(
                        f"Fuzzy fallback: GF indice '{gf_row.indice}' matched "
                        f"GED indice '{best_ged_indice}' "
                        f"(score={best_score:.1f}) — "
                        f"GF row {gf_row.sheet_name}/{gf_row.row_number}"
                    ),
                    raw_data={
                        "gf_sheet":    gf_row.sheet_name,
                        "gf_row":      gf_row.row_number,
                        "gf_indice":   gf_row.indice,
                        "ged_indice":  best_ged_indice,
                        "fuzzy_score": best_score,
                        "score_detail": best_detail,
                        "numero":      gf_row.numero,
                        "titre_gf":    gf_row.titre,
                        "libelle_ged": fuzzy_responses[0].libelle,
                    },
                ))
                logger.debug(
                    "FUZZY %s/row%d: GF ind='%s' → GED ind='%s' score=%.1f %s",
                    gf_row.sheet_name, gf_row.row_number,
                    gf_row.indice, best_ged_indice, best_score, best_detail,
                )
            else:
                # ── TYPE_DOC OVERRIDE ─────────────────────────────────────
                # Fuzzy failed (or TYPE_DOC hard-filter blocked fuzzy).
                # Last chance: look for a GED candidate whose INDICE matches
                # exactly but TYPE_DOC differs. This handles documents that
                # are submitted under a different type code in the GED
                # (e.g. GF says COF, GED uses ARM for the same document).
                # Rule: NUMERO+INDICE exact match → accept regardless of
                # TYPE_DOC, but log the discrepancy. Title similarity is
                # recorded in the anomaly for auditing but is NOT a gate.
                type_doc_override: list[CanonicalResponse] = []
                for cr in ged_candidates:
                    if str(cr.indice or "").strip().upper() == str(gf_row.indice or "").strip().upper():
                        type_doc_override.append(cr)

                if type_doc_override:
                    sim = _titre_similarity(gf_row.titre or "", type_doc_override[0].libelle or "")
                    for cr in type_doc_override:
                        cr.confidence = "TYPE_DOC_OVERRIDE"
                        cr.match_strategy = (
                            f"TYPE_DOC_OVERRIDE GF:{gf_row.type_doc} vs GED:{cr.type_doc} "
                            f"(titre_sim={sim:.2f})"
                        )
                        cr.parse_warnings.append(
                            f"TYPE_DOC_OVERRIDE: GF type='{gf_row.type_doc}' "
                            f"GED type='{cr.type_doc}' — NUMERO+INDICE exact match accepted. "
                            f"Titre similarity={sim:.2f}"
                        )
                        matched_ged_doc_ids.add(
                            (normalize_numero(cr.numero), cr.indice.upper())
                        )
                    matched_gf.append((gf_row, type_doc_override))
                    match_summary.record("GF_TYPE_DOC_OVERRIDE")
                    anomaly_logger.log(AnomalyRecord(
                        anomaly_type="TYPE_DOC_OVERRIDE",
                        severity="WARNING",
                        source_type="GED",
                        source_file=type_doc_override[0].source_file,
                        source_row_or_page=type_doc_override[0].source_row_or_page,
                        document_key=gf_row.document_key,
                        description=(
                            f"TYPE_DOC mismatch accepted: GF type='{gf_row.type_doc}' "
                            f"GED type='{type_doc_override[0].type_doc}' — "
                            f"NUMERO+INDICE exact match, titre_sim={sim:.2f} — "
                            f"GF row {gf_row.sheet_name}/{gf_row.row_number}"
                        ),
                        raw_data={
                            "gf_sheet":    gf_row.sheet_name,
                            "gf_row":      gf_row.row_number,
                            "gf_type_doc": gf_row.type_doc,
                            "ged_type_doc": type_doc_override[0].type_doc,
                            "numero":      gf_row.numero,
                            "indice":      gf_row.indice,
                            "titre_sim":   round(sim, 2),
                            "titre_gf":    gf_row.titre,
                            "libelle_ged": type_doc_override[0].libelle,
                        },
                    ))
                    logger.debug(
                        "TYPE_DOC_OVERRIDE %s/row%d: GF type='%s' GED type='%s' sim=%.2f",
                        gf_row.sheet_name, gf_row.row_number,
                        gf_row.type_doc, type_doc_override[0].type_doc, sim,
                    )

                # No recovery path worked — check soft-skip categories
                elif _is_sas_ref(gf_row):
                    match_summary.record("GF_SAS_REF_SKIP")
                elif known_skip_numeros and gf_num in known_skip_numeros:
                    match_summary.record("GF_KNOWN_SKIP")
                else:
                    # True indice mismatch, no recovery possible
                    unmatched_gf.append((gf_row, "GF_INDICE_MISMATCH"))
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


def _indice_distance(ind_a: str, ind_b: str) -> int:
    """
    Alphabetic distance between two single-letter revision indices.
    A→B=1, A→C=2, B→D=2, A→A=0.
    Returns 999 if either index is empty, multi-char, or non-alphabetic.
    """
    a = str(ind_a).strip().upper()
    b = str(ind_b).strip().upper()
    if not a or not b or len(a) != 1 or len(b) != 1:
        return 999
    if not a.isalpha() or not b.isalpha():
        return 999
    return abs(ord(a) - ord(b))


def _titre_similarity(titre_gf: str, libelle_ged: str) -> float:
    """
    Token-overlap (Jaccard) similarity between GF TITRE and GED Libellé.
    Returns 0.0–1.0. Returns 0.0 if either string has fewer than 2 meaningful tokens.

    Stopwords filtered: DE DU LA LE ET EN AU LES DES UN UNE SUR POUR PAR AVEC DANS L D A
    Punctuation replaced by spaces before tokenisation.
    """
    import re as _re
    STOPWORDS = {
        'DE', 'DU', 'LA', 'LE', 'ET', 'EN', 'AU', 'LES', 'DES', 'UN', 'UNE',
        'SUR', 'POUR', 'PAR', 'AVEC', 'DANS', 'L', 'D', 'A',
    }

    def _tokenize(s: str) -> set:
        tokens = _re.sub(r'[^A-Z0-9\s]', ' ', str(s).upper()).split()
        return {t for t in tokens if t not in STOPWORDS and len(t) > 1}

    toks_gf  = _tokenize(titre_gf  or "")
    toks_ged = _tokenize(libelle_ged or "")

    if len(toks_gf) < 2 or len(toks_ged) < 2:
        return 0.0

    union        = toks_gf | toks_ged
    intersection = toks_gf & toks_ged
    return len(intersection) / len(union) if union else 0.0


def _fuzzy_score_ged_to_gf(cr: CanonicalResponse, gf_row: GFRow) -> tuple[float, dict]:
    """
    Multi-signal fuzzy scorer for INDICE_MISMATCH fallback.
    Called ONLY when strict _score_ged_to_gf returns < 10 (no INDICE match).

    Returns (total_score: float, detail: dict).
    Caller accepts the match if total_score >= FUZZY_MATCH_THRESHOLD.

    Signal weights (max 18.0 total):
      TYPE_DOC match        → +3.0  (hard filter if mismatch → return 0 immediately)
      Date proximity        → 0–4.0 (linear decay over FUZZY_DATE_WINDOW_DAYS)
      Titre similarity      → 0–5.0 (Jaccard × 5, only if similarity >= 0.3)
      EMETTEUR match        → +3.0
      LOT match             → +2.0
      Indice adjacence      → +1.0 (dist ≤ 2) or +0.5 (dist ≤ 4)
    """
    from processing.config import FUZZY_DATE_WINDOW_DAYS, FUZZY_MAX_INDICE_DISTANCE
    from datetime import timedelta

    score  = 0.0
    detail: dict = {}

    doc_key_upper = gf_row.document_key.upper() if gf_row.document_key else ""

    # ── HARD FILTER: TYPE_DOC ─────────────────────────────────────────────
    if cr.type_doc and gf_row.type_doc:
        if str(cr.type_doc).strip().upper() != str(gf_row.type_doc).strip().upper():
            return 0.0, {"rejected": f"TYPE_DOC mismatch ({cr.type_doc} vs {gf_row.type_doc})"}
        score += 3.0
        detail["type_doc"] = "+3.0"

    # ── DATE PROXIMITY ────────────────────────────────────────────────────
    # Use GED date_depot (deposit date) first, fall back to response_date.
    # Use GF date_diff (Date diffusion) first, fall back to date_recept.
    gf_ref_date  = gf_row.date_diff or gf_row.date_recept
    ged_ref_date = cr.date_depot    or cr.response_date

    gf_dt  = _parse_any_date(gf_ref_date)
    ged_dt = _parse_any_date(ged_ref_date)

    if gf_dt and ged_dt:
        delta_days = abs((ged_dt - gf_dt).days)
        if delta_days <= FUZZY_DATE_WINDOW_DAYS:
            date_score = 4.0 * (1.0 - delta_days / FUZZY_DATE_WINDOW_DAYS)
            score += date_score
            detail["date"] = f"+{date_score:.2f} (Δ{delta_days}d)"
        else:
            detail["date"] = f"0 (Δ{delta_days}d > window {FUZZY_DATE_WINDOW_DAYS}d)"
    else:
        # Dates unavailable → partial credit (fail-open, same as strict scorer)
        score += 1.0
        detail["date"] = "+1.0 (dates unavailable)"

    # ── TITRE SIMILARITY ──────────────────────────────────────────────────
    sim = _titre_similarity(gf_row.titre or "", cr.libelle or "")
    if sim >= 0.3:
        titre_score = sim * 5.0
        score += titre_score
        detail["titre"] = f"+{titre_score:.2f} (sim={sim:.2f})"
    else:
        detail["titre"] = f"0 (sim={sim:.2f} < 0.3)"

    # ── EMETTEUR ─────────────────────────────────────────────────────────
    if cr.emetteur and doc_key_upper:
        if str(cr.emetteur).strip().upper() in doc_key_upper:
            score += 3.0
            detail["emetteur"] = "+3.0"
        else:
            detail["emetteur"] = "0"

    # ── LOT ──────────────────────────────────────────────────────────────
    if cr.lot and gf_row.lot:
        if normalize_lot(cr.lot) == normalize_lot(gf_row.lot):
            score += 2.0
            detail["lot"] = "+2.0"
        else:
            detail["lot"] = "0"

    # ── INDICE ADJACENCE ─────────────────────────────────────────────────
    dist = _indice_distance(gf_row.indice, cr.indice)
    if dist != 999 and dist <= FUZZY_MAX_INDICE_DISTANCE:
        adj_score = 1.0 if dist <= 2 else 0.5
        score += adj_score
        detail["indice_adj"] = f"+{adj_score} (dist={dist})"
    else:
        detail["indice_adj"] = f"0 (dist={dist})"

    detail["total"] = round(score, 2)
    return score, detail


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
    # Log when gf_ref_date is non-empty but unparseable — helps detect GF data anomalies
    if gf_ref_date and _parse_any_date(gf_ref_date) is None:
        logger.debug(
            "GF row %s/%s: date_diff/date_recept '%s' is non-empty but unparseable — "
            "date proximity check skipped (fail-open)",
            gf_row.sheet_name, gf_row.row_number, gf_ref_date
        )
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
