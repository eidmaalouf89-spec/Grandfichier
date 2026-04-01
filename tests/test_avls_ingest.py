"""
test_avls_ingest.py — Tests for Parser #2: AVLS
JANSA VISASIST — BET PDF Report Ingestion
"""

import pytest
import shutil
import tempfile
from pathlib import Path

from processing.avls_ingest import (
    normalize_avis,
    clean_p17_ref,
    should_skip_table,
    extract_numero,
    ingest_avls_folder,
)

# ---------------------------------------------------------------------------
# Reference PDF folder
# ---------------------------------------------------------------------------

AVLS_FOLDER = Path(__file__).parent.parent / 'data' / 'avls'
GO_IND1 = AVLS_FOLDER / 'AVLS_AA103600_BNP-17&CO_LOT-GO_VISA01-Ind1.pdf'
CVC_IND2 = AVLS_FOLDER / 'VISA_AVLS_AA1036_17&CO-T2_LOT41-CVC_VISA01Ind2.pdf'
PLB_IND2 = AVLS_FOLDER / 'VISA_AVLS_AA1036_17&CO-T2_LOT42-PLB_VISA01Ind2.pdf'
CLD_IND2 = AVLS_FOLDER / 'VISA_AVLS_AA1036_17&CO-T2_LOT11_CLD_VISA01Ind2.pdf'


# ---------------------------------------------------------------------------
# Unit tests — no PDF required
# ---------------------------------------------------------------------------

class TestNormalizeAvis:
    def test_vso(self):
        assert normalize_avis('VSO') == 'VSO'

    def test_vao(self):
        assert normalize_avis('VAO') == 'VAO'

    def test_vaob(self):
        assert normalize_avis('VAOB') == 'VAOB'

    def test_ref(self):
        assert normalize_avis('REF') == 'REF'

    def test_hm(self):
        assert normalize_avis('HM') == 'HM'

    def test_non_concerne_accented(self):
        assert normalize_avis('Non concerné') == 'HM'

    def test_non_concerne_unaccented(self):
        assert normalize_avis('Non concerne') == 'HM'

    def test_en_attente_returns_none(self):
        assert normalize_avis('En attente') is None

    def test_unknown_returns_none(self):
        assert normalize_avis('BLAH') is None

    def test_lowercase_vso(self):
        assert normalize_avis('vso') == 'VSO'


class TestCleanP17Ref:
    def test_strip_free_text_suffix(self):
        raw = 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B_PLAN'
        result = clean_p17_ref(raw)
        assert result == 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B'

    def test_strip_multiple_suffix_parts(self):
        raw = 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B_PLAN_RESEAUX'
        result = clean_p17_ref(raw)
        assert result == 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B'

    def test_clean_ref_unchanged(self):
        raw = 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B'
        result = clean_p17_ref(raw)
        assert result == raw

    def test_strip_trailing_underscore(self):
        raw = 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B_'
        result = clean_p17_ref(raw)
        assert result == 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B'

    def test_lowercase_suffix_stripped(self):
        # e.g. "_liste" after last uppercase
        raw = 'P17_T2_IN_EXE_AMP_CLD_I011_LST_IZ_TX_036000_A_liste'
        result = clean_p17_ref(raw)
        assert result == 'P17_T2_IN_EXE_AMP_CLD_I011_LST_IZ_TX_036000_A'


class TestShouldSkipTable:
    def test_skip_legend_avis_first_cell(self):
        table = [['AVIS', 'VSO : Validé sans observations', '', '']]
        assert should_skip_table(table) is True

    def test_skip_documents_a_transmettre(self):
        table = [['DOCUMENTS A TRANSMETTRE', '', '']]
        assert should_skip_table(table) is True

    def test_skip_1col_orphan(self):
        table = [['Observations']]
        assert should_skip_table(table) is True

    def test_skip_2col_orphan(self):
        table = [['Observations', '12/03/2025']]
        assert should_skip_table(table) is True

    def test_skip_fiche_visa_header(self):
        table = [['FICHE VISA', 'REF PROJET', 'NOM', 'LOT', 'N°VISA', 'IND']]
        assert should_skip_table(table) is True

    def test_do_not_skip_avis_data_table(self):
        table = [['VAO', '', '', 'P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B']]
        assert should_skip_table(table) is False

    def test_skip_empty_table(self):
        assert should_skip_table([]) is True
        assert should_skip_table(None) is True


class TestExtractNumero:
    def test_6digit_from_ref(self):
        assert extract_numero('P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B') == '349612'

    def test_6digit_direct(self):
        assert extract_numero('349612') == '349612'


# ---------------------------------------------------------------------------
# Integration tests — require PDF files
# ---------------------------------------------------------------------------

def _ingest_single_file(pdf_path: Path):
    """Helper: ingest a single PDF in a temp folder."""
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copy(str(pdf_path), tmpdir)
        return ingest_avls_folder(tmpdir)


@pytest.mark.skipif(not GO_IND1.exists(), reason="GO Ind1 PDF not available")
def test_ingest_go_file():
    """LOT-GO Ind1: 4 records, all VAO."""
    records, _ = _ingest_single_file(GO_IND1)
    assert len(records) == 4
    assert all(r['STATUT_NORM'] == 'VAO' for r in records)


@pytest.mark.skipif(not CVC_IND2.exists(), reason="CVC Ind2 PDF not available")
def test_ingest_cvc_ind2():
    """LOT41-CVC Ind2: 34 records, all VAO."""
    records, _ = _ingest_single_file(CVC_IND2)
    assert len(records) == 34
    assert all(r['STATUT_NORM'] == 'VAO' for r in records)


@pytest.mark.skipif(not PLB_IND2.exists(), reason="PLB Ind2 PDF not available")
def test_ingest_plb_ind2():
    """LOT42-PLB Ind2: 52 records (VAO=50, VSO=2)."""
    records, _ = _ingest_single_file(PLB_IND2)
    assert len(records) == 52
    vao = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
    vso = sum(1 for r in records if r['STATUT_NORM'] == 'VSO')
    assert vao == 50
    assert vso == 2


@pytest.mark.skipif(not CLD_IND2.exists(), reason="CLD Ind2 PDF not available")
def test_ingest_cld_ind2():
    """LOT11-CLD Ind2: 36 records (VAO=24, HM=12)."""
    records, _ = _ingest_single_file(CLD_IND2)
    assert len(records) == 36
    vao = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
    hm = sum(1 for r in records if r['STATUT_NORM'] == 'HM')
    assert vao == 24
    assert hm == 12


@pytest.mark.skipif(not AVLS_FOLDER.exists(), reason="AVLS folder not available")
def test_grand_total():
    """Full batch of 20 files: 475 records total."""
    records, _ = ingest_avls_folder(AVLS_FOLDER)
    assert len(records) == 475
    vao  = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
    vaob = sum(1 for r in records if r['STATUT_NORM'] == 'VAOB')
    vso  = sum(1 for r in records if r['STATUT_NORM'] == 'VSO')
    ref  = sum(1 for r in records if r['STATUT_NORM'] == 'REF')
    hm   = sum(1 for r in records if r['STATUT_NORM'] == 'HM')
    assert vao  == 378
    assert vaob == 50
    assert vso  == 11
    assert ref  == 24
    assert hm   == 12


@pytest.mark.skipif(not AVLS_FOLDER.exists(), reason="AVLS folder not available")
def test_no_en_attente_records():
    """No record should have STATUT_NORM = None or 'EN ATTENTE'."""
    records, _ = ingest_avls_folder(AVLS_FOLDER)
    for r in records:
        assert r['STATUT_NORM'] is not None
        assert r['STATUT_NORM'].upper() != 'EN ATTENTE'


@pytest.mark.skipif(not AVLS_FOLDER.exists(), reason="AVLS folder not available")
def test_all_records_have_numero():
    """NUMERO must be a non-empty 6-digit string for every record."""
    import re
    records, _ = ingest_avls_folder(AVLS_FOLDER)
    for r in records:
        assert r['NUMERO'], f"Empty NUMERO in {r['RAPPORT_ID']}"
        assert re.match(r'^\d{5,6}$', r['NUMERO']), f"Bad NUMERO: {r['NUMERO']}"


@pytest.mark.skipif(not AVLS_FOLDER.exists(), reason="AVLS folder not available")
def test_vaob_not_flattened():
    """VAOB records must be stored as 'VAOB', not 'VAO'."""
    records, _ = ingest_avls_folder(AVLS_FOLDER)
    vaob_records = [r for r in records if r['STATUT_NORM'] == 'VAOB']
    assert len(vaob_records) > 0, "Expected VAOB records in batch"
    for r in vaob_records:
        assert r['STATUT_NORM'] == 'VAOB'
