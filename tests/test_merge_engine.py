"""
Tests for the merge engine (processing/merge_engine.py)
Verifies conflict detection and TAG_PRIORITY consolidation.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.models import CanonicalResponse, GFRow, GFApprobateur
from processing.config import resolve_worst_tag, TAG_PRIORITY
from processing.merge_engine import build_deliverables, load_source_priority
from processing.anomalies import AnomalyLogger

SOURCE_PRIORITY_PATH = Path(__file__).parent.parent / "data" / "source_priority.json"


def _make_cr(doc_key, mission, status, source_type="GED", sheet="LOT TEST", row=10):
    return CanonicalResponse(
        source_type=source_type,
        source_file="test.xlsx",
        source_row_or_page="row 1",
        document_key=doc_key,
        lot="G003",
        type_doc="NDC",
        numero="028000",
        indice="A",
        batiment="GE",
        zone="TZ",
        niveau="TX",
        emetteur="LGD",
        mission=mission,
        respondant="Test User",
        raw_status=status,
        normalized_status=status,
        response_date="2024-01-18",
        deadline_date="2024-01-20",
        days_delta=-2,
        comment="Test comment",
        attachments="",
        confidence="EXACT",
        match_strategy="LEVEL_1_FULL_KEY",
        gf_sheet=sheet,
        gf_row=row,
    )


def _make_gf_row(sheet="LOT TEST", row=10):
    return GFRow(
        sheet_name=sheet,
        row_number=row,
        document_key="TESTKEY",
        titre="Test Document",
        lot="G003",
        type_doc="NDC",
        numero="028000",
        indice="A",
        niveau="R0",
        zone="TZ",
        ancien=False,
        visa_global="",
        observations="",
        approbateurs=[],
    )


def test_resolve_worst_tag_def_wins():
    tags = ["VSO", "VAO", "DEF", "HM"]
    assert resolve_worst_tag(tags) == "DEF"


def test_resolve_worst_tag_ref_over_vao():
    tags = ["VAO", "REF", "VSO"]
    assert resolve_worst_tag(tags) == "REF"


def test_resolve_worst_tag_all_vso():
    tags = ["VSO", "VSO"]
    assert resolve_worst_tag(tags) == "VSO"


def test_resolve_worst_tag_hm_lowest():
    tags = ["HM"]
    assert resolve_worst_tag(tags) == "HM"


def test_resolve_worst_tag_empty():
    assert resolve_worst_tag([]) is None


def test_resolve_worst_tag_ignores_none_codes():
    tags = ["NONE", "VSO", "NONE"]
    assert resolve_worst_tag(tags) == "VSO"


def test_tag_priority_order():
    """DEF must have lower priority number than REF, which is lower than SUS, etc."""
    assert TAG_PRIORITY["DEF"] < TAG_PRIORITY["REF"]
    assert TAG_PRIORITY["REF"] < TAG_PRIORITY["SUS"]
    assert TAG_PRIORITY["SUS"] < TAG_PRIORITY["VAO"]
    assert TAG_PRIORITY["VAO"] < TAG_PRIORITY["VSO"]
    assert TAG_PRIORITY["VSO"] < TAG_PRIORITY["HM"]


def test_build_deliverables_groups_by_gf_row():
    sp = load_source_priority(SOURCE_PRIORITY_PATH)
    alog = AnomalyLogger()

    gf_row = _make_gf_row(sheet="LOT TEST", row=10)
    gf_lookup = {("LOT TEST", 10): gf_row}

    cr1 = _make_cr("KEY1", "0-Maître d'Oeuvre EXE", "VSO", sheet="LOT TEST", row=10)
    cr2 = _make_cr("KEY1", "0-BET Structure",        "VAO", sheet="LOT TEST", row=10)

    deliverables = build_deliverables([cr1, cr2], gf_lookup, sp, alog)
    assert len(deliverables) == 1
    drec = deliverables[0]
    assert len(drec.responses) == 2
    # Worst tag: VAO > VSO
    assert drec.consolidated_status == "VAO"


def test_build_deliverables_def_overrides():
    sp = load_source_priority(SOURCE_PRIORITY_PATH)
    alog = AnomalyLogger()

    gf_row = _make_gf_row()
    gf_lookup = {("LOT TEST", 10): gf_row}

    cr1 = _make_cr("KEY1", "0-Maître d'Oeuvre EXE", "VSO")
    cr2 = _make_cr("KEY1", "0-BET Structure",        "DEF")
    cr3 = _make_cr("KEY1", "0-Bureau de Contrôle",   "VAO")

    deliverables = build_deliverables([cr1, cr2, cr3], gf_lookup, sp, alog)
    assert deliverables[0].consolidated_status == "DEF"


def test_build_deliverables_multiple_rows():
    sp = load_source_priority(SOURCE_PRIORITY_PATH)
    alog = AnomalyLogger()

    gf_row1 = _make_gf_row(sheet="LOT A", row=10)
    gf_row2 = _make_gf_row(sheet="LOT A", row=11)
    gf_lookup = {("LOT A", 10): gf_row1, ("LOT A", 11): gf_row2}

    cr1 = _make_cr("KEY1", "0-Maître d'Oeuvre EXE", "VSO", sheet="LOT A", row=10)
    cr2 = _make_cr("KEY2", "0-Maître d'Oeuvre EXE", "REF", sheet="LOT A", row=11)

    deliverables = build_deliverables([cr1, cr2], gf_lookup, sp, alog)
    assert len(deliverables) == 2
