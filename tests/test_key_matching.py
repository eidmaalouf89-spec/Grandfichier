"""
Tests for the 4-level key matching engine (processing/canonical.py + processing/matcher.py)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.canonical import (
    normalize_lot, normalize_numero, normalize_numero_stripped,
    build_ged_key, build_gf_key,
    build_level2_key, build_level3_key, build_level4_key,
)
from processing.models import CanonicalResponse, GFRow, GFApprobateur
from processing.matcher import GFIndex, MatchSummary, match_all


# ---------------------------------------------------------------------------
# Canonical key normalization tests
# ---------------------------------------------------------------------------

def test_normalize_lot_strips_prefix():
    assert normalize_lot("I003") == "003"
    assert normalize_lot("A031") == "031"
    assert normalize_lot("G003") == "003"
    assert normalize_lot("B041") == "041"


def test_normalize_lot_no_prefix():
    assert normalize_lot("003") == "003"
    assert normalize_lot("031") == "031"


def test_normalize_lot_empty():
    assert normalize_lot(None) == ""
    assert normalize_lot("") == ""


def test_normalize_numero_zero_pad():
    assert normalize_numero("28000") == "028000"
    assert normalize_numero("028000") == "028000"
    assert normalize_numero(28000) == "028000"


def test_normalize_numero_strips_trailing_alpha():
    assert normalize_numero("028000A") == "028000"
    assert normalize_numero("031005B") == "031005"


def test_normalize_numero_stripped_removes_leading_zeros():
    assert normalize_numero_stripped("028000") == "28000"
    assert normalize_numero_stripped("001000") == "1000"


def test_normalize_numero_empty():
    assert normalize_numero(None) == ""
    assert normalize_numero("") == ""


# ---------------------------------------------------------------------------
# Level 1: full composite key
# ---------------------------------------------------------------------------

def test_build_ged_key():
    key = build_ged_key({
        "affaire": "P17", "projet": "T2", "batiment": "GE",
        "phase": "EXE", "emetteur": "LGD", "specialite": "GOE",
        "lot": "I003", "type_doc": "NDC", "zone": "TZ",
        "niveau": "TX", "numero": "028000",
    })
    # Should be uppercase concatenation
    assert "P17" in key
    assert "NDC" in key
    assert "028000" in key or "28000" in key  # numero raw passed as-is


def test_build_gf_key_normalizes():
    assert build_gf_key("  p17t2geexelgdgoeg003ndctztn028000  ") == "P17T2GEEXELGDGOEG003NDCTZTN028000"
    assert build_gf_key(None) == ""


# ---------------------------------------------------------------------------
# Level 2: LOT + TYPE_DOC + NUMERO matching
# ---------------------------------------------------------------------------

def test_level2_key_lot_prefix_normalization():
    # Both should produce same key despite different LOT prefix
    k1 = build_level2_key("I003", "NDC", "028000")
    k2 = build_level2_key("G003", "NDC", "028000")
    assert k1 == k2


def test_level2_key_numero_padding():
    k1 = build_level2_key("003", "NDC", "28000")
    k2 = build_level2_key("003", "NDC", "028000")
    assert k1 == k2


# ---------------------------------------------------------------------------
# Level 3: TYPE_DOC + NUMERO
# ---------------------------------------------------------------------------

def test_level3_key():
    k = build_level3_key("NDC", "028000")
    assert k == "NDC028000"


# ---------------------------------------------------------------------------
# Level 4: NUMERO stripped
# ---------------------------------------------------------------------------

def test_level4_key():
    assert build_level4_key("028000") == "28000"
    assert build_level4_key("028000A") == "28000"


# ---------------------------------------------------------------------------
# GFIndex + match_all integration tests
# ---------------------------------------------------------------------------

def _make_gf_row(doc_key, lot, type_doc, numero, sheet="LOT TEST", row=10):
    return GFRow(
        sheet_name=sheet,
        row_number=row,
        document_key=doc_key,
        titre="Test",
        lot=lot,
        type_doc=type_doc,
        numero=numero,
        indice="A",
        niveau="R0",
        zone="TZ",
        ancien=False,
        visa_global="",
        observations="",
        approbateurs=[],
    )


def _make_cr(doc_key, lot, type_doc, numero):
    return CanonicalResponse(
        source_type="GED",
        source_file="test.xlsx",
        source_row_or_page="row 1",
        document_key=doc_key,
        lot=lot,
        type_doc=type_doc,
        numero=numero,
        indice="A",
        batiment="GE",
        zone="TZ",
        niveau="TX",
        emetteur="LGD",
        mission="0-Maître d'Oeuvre EXE",
        respondant="",
        raw_status="Validé sans observation",
        normalized_status="VSO",
        response_date="",
        deadline_date="",
        days_delta=None,
        comment="",
        attachments="",
    )


def test_level1_exact_match():
    gf_row = _make_gf_row("P17T2GEEXELGDGOEG003NDCTZTN028000", "G003", "NDC", "028000")
    idx = GFIndex([gf_row])

    cr = _make_cr("P17T2GEEXELGDGOEG003NDCTZTN028000", "I003", "NDC", "028000")
    result = idx.match(cr)

    assert result is not None
    assert cr.confidence == "EXACT"
    assert cr.match_strategy == "LEVEL_1_FULL_KEY"


def test_level2_lot_prefix_mismatch():
    """GED uses I003, GF uses G003 — should match at Level 2."""
    gf_row = _make_gf_row("P17T2GEEXELGDGOEG003NDCTZTN028000", "G003", "NDC", "028000")
    idx = GFIndex([gf_row])

    # CR with different full key but same LOT+TYPE+NUM
    cr = _make_cr("P17T2GEEXELGDGOEI003NDCTZTN028000", "I003", "NDC", "028000")
    result = idx.match(cr)

    assert result is not None
    assert cr.confidence in ("FUZZY", "PARTIAL", "EXACT")  # at least matched


def test_level4_numero_stripped():
    """Only numero matches (stripped of leading zeros)."""
    gf_row = _make_gf_row("SOMEKEY028000", "G003", "NDC", "028000")
    idx = GFIndex([gf_row])

    cr = _make_cr("DIFFERENTKEY028000", "X999", "XYZ", "28000")
    result = idx.match(cr)
    # Should match at level 4 (or 3)
    assert result is not None


def test_no_match_returns_none():
    gf_row = _make_gf_row("P17T2GEEXELGDGOEG003NDCTZTN028000", "G003", "NDC", "028000")
    idx = GFIndex([gf_row])

    cr = _make_cr("COMPLETELYDIFFERENTKEY", "Z999", "ZZZ", "999999")
    result = idx.match(cr)
    assert result is None
    assert cr.confidence == "UNMATCHED"


def test_match_summary_counts():
    gf_row = _make_gf_row("EXACTKEY", "003", "NDC", "028000")
    idx = GFIndex([gf_row])

    cr_matched = _make_cr("EXACTKEY", "I003", "NDC", "028000")
    cr_unmatched = _make_cr("NOTFOUND", "Z999", "ZZZ", "999999")

    summary = MatchSummary()
    matched, unmatched = match_all([cr_matched, cr_unmatched], idx, summary)
    assert summary.total_matched >= 1
    assert summary.total_unmatched >= 1
