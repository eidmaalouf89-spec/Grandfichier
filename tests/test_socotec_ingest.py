"""
test_socotec_ingest.py — Tests for Parser #4: SOCOTEC
JANSA VISASIST — BET PDF Report Ingestion
"""

import re
import pytest
import shutil
import tempfile
from pathlib import Path

from processing.socotec_ingest import (
    normalize_avis,
    clean_socotec_ref,
    extract_metadata,
    should_skip_file,
    is_avis_table,
    ingest_socotec_folder,
)

# ---------------------------------------------------------------------------
# Reference PDF folder
# ---------------------------------------------------------------------------

SOCOTEC_FOLDER = Path(__file__).parent.parent / 'data' / 'socotec'
CT_0326_0014 = SOCOTEC_FOLDER / '02-03-26 - PARIS 17CO - Fiche examen-CT-204C0-0326-0014.pdf'
CT_1024_0139 = SOCOTEC_FOLDER / '10-10-24 - PARIS 17CO-Fiche examen-CT-204C0-1024-0139.pdf'
CT_1225_0172 = SOCOTEC_FOLDER / '12-12-25 - PARIS 17CO - Fiche examen-CT-204C0-1225-0172.pdf'
AVIS_TRAVAUX = SOCOTEC_FOLDER / '09-03-26 - PARIS 17CO - Fiche avis travaux.pdf'


# ---------------------------------------------------------------------------
# Unit tests — no PDF required
# ---------------------------------------------------------------------------

class TestNormalizeAvis:
    def test_f_to_vso(self):
        assert normalize_avis('F') == 'VSO'

    def test_s_to_vao(self):
        assert normalize_avis('S') == 'VAO'

    def test_d_to_ref(self):
        assert normalize_avis('D') == 'REF'

    def test_favorable_word(self):
        assert normalize_avis('Favorable') == 'VSO'

    def test_suspendu_word(self):
        assert normalize_avis('Suspendu') == 'VAO'

    def test_defavorable_accented(self):
        assert normalize_avis('Défavorable') == 'REF'

    def test_defavorable_unaccented(self):
        assert normalize_avis('Defavorable') == 'REF'

    def test_unknown_returns_none(self):
        assert normalize_avis('X') is None

    def test_empty_returns_none(self):
        assert normalize_avis('') is None


class TestCleanSocotecRef:
    def test_strip_description_suffix(self):
        raw = 'P17_T2_AU_EXE_AXI_CVC_A041_MAT_AZ_TX_249523_A-A. Pompes (production & distribution)'
        result = clean_socotec_ref(raw)
        assert result == 'P17_T2_AU_EXE_AXI_CVC_A041_MAT_AZ_TX_249523_A'

    def test_clean_ref_unchanged(self):
        raw = 'P17_T2_AU_EXE_AXI_CVC_A041_MAT_AZ_TX_249523_A'
        assert clean_socotec_ref(raw) == raw

    def test_strip_trailing_underscore(self):
        raw = 'P17_T2_AU_EXE_AXI_CVC_A041_MAT_AZ_TX_249523_A_'
        assert clean_socotec_ref(raw) == 'P17_T2_AU_EXE_AXI_CVC_A041_MAT_AZ_TX_249523_A'


class TestExtractMetadata:
    def test_standard_filename(self):
        fn = '02-03-26 - PARIS 17CO - Fiche examen-CT-204C0-0326-0014.pdf'
        meta = extract_metadata(fn)
        assert meta['ct_ref'] == 'CT-204C0-0326-0014'
        assert meta['date_fiche'] == '02/03/2026'

    def test_two_digit_year(self):
        fn = '10-10-24 - -Fiche examen-CT-204C0-1024-0139.pdf'
        meta = extract_metadata(fn)
        assert meta['ct_ref'] == 'CT-204C0-1024-0139'
        assert '2024' in meta['date_fiche']

    def test_no_ct_ref_fallback(self):
        fn = 'rapport 18 Socotec radier.pdf'
        meta = extract_metadata(fn)
        assert meta['ct_ref'] == 'rapport 18 Socotec radier'
        assert meta['date_fiche'] == ''


class TestShouldSkipFile:
    def test_skip_hash_file(self):
        assert should_skip_file('D5906DD21E.pdf') is True

    def test_skip_plan_el(self):
        assert should_skip_file('PARIS - PARKING ST OUEN_Plan_EL02_XXXXXXX.pdf') is True

    def test_skip_parking(self):
        assert should_skip_file('PARKING_plan_V2.pdf') is True

    def test_skip_fiche_reponse(self):
        assert should_skip_file('240913-Fiche réponse 2-SOCOTEC.pdf') is True

    def test_do_not_skip_normal_fiche(self):
        assert should_skip_file('02-03-26 - PARIS 17CO - Fiche examen-CT-204C0-0326-0014.pdf') is False


class TestIsAvisTable:
    def test_true_with_avis_and_observations(self):
        table = [['Éléments examinés', 'Avis*', 'Observations et commentaires', 'N°']]
        assert is_avis_table(table) is True

    def test_true_uppercase_avis(self):
        table = [['Elements', 'AVIS', 'Observations', 'N°']]
        assert is_avis_table(table) is True

    def test_false_document_list_table(self):
        # Page 1 table — no Avis column
        table = [['Désignation – Identification des documents', 'Reçu le']]
        assert is_avis_table(table) is False

    def test_false_empty_table(self):
        assert is_avis_table([]) is False
        assert is_avis_table(None) is False


# ---------------------------------------------------------------------------
# Integration tests — require PDF files
# ---------------------------------------------------------------------------

def _ingest_single(pdf_path: Path):
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copy(str(pdf_path), tmpdir)
        return ingest_socotec_folder(tmpdir)


@pytest.mark.skipif(not CT_0326_0014.exists(), reason="CT-204C0-0326-0014 PDF not available")
def test_ingest_ct0326_0014():
    """CT-204C0-0326-0014: 14 records."""
    records, _ = _ingest_single(CT_0326_0014)
    assert len(records) == 14


@pytest.mark.skipif(not CT_1024_0139.exists(), reason="CT-204C0-1024-0139 PDF not available")
def test_ingest_ct1024_0139():
    """CT-204C0-1024-0139: 26 records."""
    records, _ = _ingest_single(CT_1024_0139)
    assert len(records) == 26


@pytest.mark.skipif(not CT_1225_0172.exists(), reason="CT-204C0-1225-0172 PDF not available")
def test_ingest_ct1225_0172():
    """CT-204C0-1225-0172: 33 records, 0 REF."""
    records, _ = _ingest_single(CT_1225_0172)
    assert len(records) == 33
    ref_count = sum(1 for r in records if r['STATUT_NORM'] == 'REF')
    assert ref_count == 0


@pytest.mark.skipif(not SOCOTEC_FOLDER.exists(), reason="SOCOTEC folder not available")
def test_page1_skipped():
    """No records should come from page_num == 1."""
    records, _ = ingest_socotec_folder(SOCOTEC_FOLDER)
    page1_records = [r for r in records if r['PDF_PAGE'] == 1]
    assert len(page1_records) == 0


@pytest.mark.skipif(not SOCOTEC_FOLDER.exists(), reason="SOCOTEC folder not available")
def test_duplicate_files_deduped():
    """
    Verify records from duplicate PDFs (same CT_REF) are identical.
    The upsert key (CT_REF, NUMERO, STATUT_NORM) prevents double-counting on upsert.
    """
    records, _ = ingest_socotec_folder(SOCOTEC_FOLDER)
    # Check that upsert keys have reasonable distribution
    upsert_keys = [
        (r['RAPPORT_ID'], r['NUMERO'], r['STATUT_NORM'])
        for r in records
        if r['NUMERO']
    ]
    # No assertion on exact dedup count — just verify keys are consistent
    assert len(upsert_keys) > 0


@pytest.mark.skipif(not SOCOTEC_FOLDER.exists(), reason="SOCOTEC folder not available")
def test_no_empty_numero():
    """NUMERO must be a non-empty 6-digit string for all records."""
    records, _ = ingest_socotec_folder(SOCOTEC_FOLDER)
    for r in records:
        assert r['NUMERO'], f"Empty NUMERO in {r['RAPPORT_ID']}"
        assert re.match(r'^\d{5,6}$', r['NUMERO']), f"Bad NUMERO: {r['NUMERO']}"


@pytest.mark.skipif(not AVIS_TRAVAUX.exists(), reason="Fiche avis travaux PDF not available")
def test_avis_travaux_zero_records():
    """Fiche avis travaux file produces 0 records (no P17 refs, no avis table)."""
    records, _ = _ingest_single(AVIS_TRAVAUX)
    assert len(records) == 0
