"""
test_lesommer_ingest.py — Tests for Parser #1: Le Sommer Environnement
JANSA VISASIST — BET PDF Report Ingestion
"""

import pytest
from pathlib import Path

from processing.lesommer_ingest import (
    normalize_statut,
    reconstruct_truncated_ref,
    extract_numero,
    extract_indice_from_ref,
    ingest_lesommer_folder,
)

# ---------------------------------------------------------------------------
# Reference PDF folder (may not exist in CI — tests skip gracefully)
# ---------------------------------------------------------------------------

LS_FOLDER = Path(__file__).parent.parent / 'data' / 'lesommer'
CFO_PDF = LS_FOLDER / '260114_17CO_SYNTHESE_VISA_CFO_I-B-A-H.pdf'
CVC_PDF = LS_FOLDER / '260114_17CO_SYNTHESE_VISA_CVC_I-B-A-H.pdf'
PLB_PDF = LS_FOLDER / '260114_17CO_SYNTHESE_VISA_PLB_I-B-A-H.pdf'
CVC2_PDF = LS_FOLDER / '260310_17CO_SYNTHESE_VISA_CVC_I-B-A-H-CR_Synthese.pdf'
PLB2_PDF = LS_FOLDER / '260310_17CO_SYNTHESE_VISA_PLB_I-B-A-H-CR_Synthese.pdf'


# ---------------------------------------------------------------------------
# Unit tests — no PDF required
# ---------------------------------------------------------------------------

class TestNormalizeStatut:
    def test_zero_returns_none(self):
        assert normalize_statut('0') is None

    def test_one_returns_ref(self):
        assert normalize_statut('1') == 'REF'

    def test_two_returns_vao(self):
        assert normalize_statut('2') == 'VAO'

    def test_three_returns_vso(self):
        assert normalize_statut('3') == 'VSO'

    def test_tag_vao(self):
        assert normalize_statut('VAO') == 'VAO'

    def test_tag_ref(self):
        assert normalize_statut('REF') == 'REF'

    def test_tag_vso(self):
        assert normalize_statut('VSO') == 'VSO'

    def test_tag_vaob(self):
        assert normalize_statut('VAOB') == 'VAOB'

    def test_unknown_returns_none(self):
        assert normalize_statut('XYZ') is None

    def test_lowercase_vso(self):
        assert normalize_statut('vso') == 'VSO'


class TestReconstructTruncatedRef:
    def test_repair_truncated(self):
        col2 = 'P17_T2_BX_EXE_UTB_PLB_B042_SYQ_BZ_TX_15030'
        col3 = '4_B'
        result_col2, result_col3 = reconstruct_truncated_ref(col2, col3)
        assert result_col2 == col2 + col3
        assert result_col3 == ''

    def test_no_op_clean_pair(self):
        col2 = 'P17_T2_BX_EXE_UTB_PLB_B042_SYQ_BZ_TX_150304_B'
        col3 = 'Some product name'
        result_col2, result_col3 = reconstruct_truncated_ref(col2, col3)
        assert result_col2 == col2
        assert result_col3 == col3

    def test_no_op_non_p17(self):
        col2 = '149311'
        col3 = 'Pompe de circulation'
        result_col2, _ = reconstruct_truncated_ref(col2, col3)
        assert result_col2 == col2

    def test_no_op_trailing_underscore(self):
        # col2 ends with underscore → no truncation
        col2 = 'P17_T2_BX_EXE_UTB_PLB_B042_SYQ_BZ_TX_150304_'
        col3 = 'CONTINUATION'
        result_col2, _ = reconstruct_truncated_ref(col2, col3)
        assert result_col2 == col2


class TestExtractNumero:
    def test_from_full_p17_ref(self):
        ref = 'P17_T2_BX_EXE_UTB_PLB_B042_SYQ_BZ_TX_150304_B'
        assert extract_numero(ref) == '150304'

    def test_from_short_numeric(self):
        assert extract_numero('149311') == '149311'

    def test_five_digit(self):
        assert extract_numero('P17_T2_HO_EXE_LGD_GOE_H003_PLN_HZ_R0_02900_A') == '02900'

    def test_no_numero_returns_empty(self):
        assert extract_numero('UNKNOWN_REF') == ''


class TestExtractIndiceFromRef:
    def test_trailing_letter(self):
        assert extract_indice_from_ref('P17_T2_BX_EXE_UTB_PLB_B042_SYQ_BZ_TX_150304_B') == 'B'

    def test_trailing_a(self):
        assert extract_indice_from_ref('P17_T2_HO_EXE_LGD_GOE_H003_PLN_HZ_R0_328100_A') == 'A'

    def test_no_trailing_letter(self):
        assert extract_indice_from_ref('149311') == ''

    def test_strips_trailing_underscore(self):
        assert extract_indice_from_ref('P17_T2_BX_EXE_PLB_150304_B_') == 'B'


# ---------------------------------------------------------------------------
# Integration tests — require PDF files
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not CFO_PDF.exists(), reason="CFO PDF not available")
class TestIngestCFOFile:
    def test_record_count(self):
        records, _ = ingest_lesommer_folder(LS_FOLDER / 'cfo_only')
        # Expect 61 records from CFO file
        assert len(records) == 61

    def test_statut_distribution(self):
        records, _ = ingest_lesommer_folder(LS_FOLDER / 'cfo_only')
        ref_count = sum(1 for r in records if r['STATUT_NORM'] == 'REF')
        vao_count = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
        assert ref_count == 23
        assert vao_count == 38


@pytest.mark.skipif(not CVC_PDF.exists(), reason="CVC PDF not available")
def test_ingest_cvc_file():
    """Run on CVC reference PDF; assert 50 records, REF=5, VAO=6, VSO=39."""
    # Create a temp folder with only the CVC file
    import tempfile
    import shutil
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copy(str(CVC_PDF), tmpdir)
        records, _ = ingest_lesommer_folder(tmpdir)
    assert len(records) == 50
    ref_count = sum(1 for r in records if r['STATUT_NORM'] == 'REF')
    vao_count = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
    vso_count = sum(1 for r in records if r['STATUT_NORM'] == 'VSO')
    assert ref_count == 5
    assert vao_count == 6
    assert vso_count == 39


@pytest.mark.skipif(not PLB_PDF.exists(), reason="PLB PDF not available")
def test_ingest_plb_file():
    """Run on PLB reference PDF; assert 17 records."""
    import tempfile
    import shutil
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copy(str(PLB_PDF), tmpdir)
        records, _ = ingest_lesommer_folder(tmpdir)
    assert len(records) == 17


@pytest.mark.skipif(not LS_FOLDER.exists(), reason="LS folder not available")
def test_no_en_attente_records():
    """Verify no record has STATUT_NORM equal to '0' or None."""
    records, _ = ingest_lesommer_folder(LS_FOLDER)
    for r in records:
        assert r['STATUT_NORM'] not in (None, '0', 'None')
        assert r['STATUT_NORM'] in ('VSO', 'VAO', 'VAOB', 'REF', 'HM')


@pytest.mark.skipif(not LS_FOLDER.exists(), reason="LS folder not available")
def test_all_records_have_numero():
    """Verify NUMERO is a non-empty 5-or-6-digit string for every record."""
    records, _ = ingest_lesommer_folder(LS_FOLDER)
    import re
    for r in records:
        assert r['NUMERO'], f"Empty NUMERO in record: {r}"
        assert re.match(r'^\d{5,6}$', r['NUMERO']), f"Bad NUMERO: {r['NUMERO']}"


@pytest.mark.skipif(not LS_FOLDER.exists(), reason="LS folder not available")
def test_incremental_upsert_key():
    """
    Verify that the upsert key (RAPPORT_ID, NUMERO, INDICE, SECTION, TABLE_TYPE)
    is unique per record within a single file run.
    """
    import tempfile
    import shutil
    # Test with one file at a time to ensure uniqueness per file
    for pdf_path in sorted(LS_FOLDER.glob('*.pdf'))[:2]:
        with tempfile.TemporaryDirectory() as tmpdir:
            shutil.copy(str(pdf_path), tmpdir)
            records, _ = ingest_lesommer_folder(tmpdir)
        keys = [
            (r['RAPPORT_ID'], r['NUMERO'], r['INDICE'], r['SECTION'], r['TABLE_TYPE'])
            for r in records
        ]
        assert len(keys) == len(set(keys)), \
            f"Duplicate upsert keys in {pdf_path.name}: {len(keys) - len(set(keys))} duplicates"
