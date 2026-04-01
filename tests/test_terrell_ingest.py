"""
test_terrell_ingest.py — Tests for Parser #3: TERRELL
JANSA VISASIST — BET PDF Report Ingestion
"""

import re
import pytest
import shutil
import tempfile
from pathlib import Path

from processing.terrell_ingest import (
    reconstruct_p17_ref,
    extract_avis,
    is_terrell_data_table,
    is_drawing_file,
    parse_date,
    extract_fiche_ref,
    ingest_terrell_folder,
)

# ---------------------------------------------------------------------------
# Reference PDF folder
# ---------------------------------------------------------------------------

TERRELL_FOLDER = Path(__file__).parent.parent / 'data' / 'terrell'
FE003 = TERRELL_FOLDER / '2844-17&Co-TRS-FE003.pdf'
FE082 = TERRELL_FOLDER / '2844-17&Co-TRS-FE082.pdf'
FE041 = TERRELL_FOLDER / '2844-17&Co-TRS-FE041.pdf'
FE012 = TERRELL_FOLDER / '2844-17&Co-TRS-FE012.pdf'
DRAWING = TERRELL_FOLDER / 'P17_T2_HO_EXE_LGD_GOE_H003_ARM_HZ_R0_328129_A_Armatures.pdf'


# ---------------------------------------------------------------------------
# Helper: build a mock 19-col row
# ---------------------------------------------------------------------------

def make_row(projet='P17', tranche='T2', bat='HO', phase='EXE',
             emetteur='LGD', specialite='GOE', lot='H003', type_doc='PLN',
             zone='HZ', niveau='R0', numero='3 2 8 1 0 0', indice='A',
             vso='', vao='', ref_col='', hm='', obs='', designation='Plancher'):
    row = [''] * 19
    row[0]  = designation
    row[1]  = projet
    row[2]  = tranche
    row[3]  = bat
    row[4]  = phase
    row[5]  = emetteur
    row[6]  = specialite
    row[7]  = lot
    row[8]  = type_doc
    row[9]  = zone
    row[10] = niveau
    row[11] = numero
    row[12] = indice
    row[13] = ''
    row[14] = vso
    row[15] = vao
    row[16] = ref_col
    row[17] = hm
    row[18] = obs
    return row


# ---------------------------------------------------------------------------
# Unit tests — no PDF required
# ---------------------------------------------------------------------------

class TestReconstructP17Ref:
    def test_standard(self):
        row = make_row()
        ref = reconstruct_p17_ref(row)
        assert ref == 'P17_T2_HO_EXE_LGD_GOE_H003_PLN_HZ_R0_328100_A'

    def test_invalid_projet(self):
        row = make_row(projet='XXX')
        assert reconstruct_p17_ref(row) is None

    def test_invalid_numero(self):
        row = make_row(numero='TEJORP')
        assert reconstruct_p17_ref(row) is None

    def test_short_row(self):
        assert reconstruct_p17_ref(['P17', 'T2']) is None


class TestNumeroReconstruction:
    def test_6digits_with_spaces(self):
        row = make_row(numero='3 2 8 1 0 0', vso='X')
        ref = reconstruct_p17_ref(row)
        assert '328100' in ref

    def test_5digits(self):
        row = make_row(numero='0 2 9 0 0', vso='X')
        ref = reconstruct_p17_ref(row)
        assert '02900' in ref


class TestExtractAvis:
    def test_vso_col14(self):
        row = make_row(vso='X')
        assert extract_avis(row) == 'VSO'

    def test_vao_col15(self):
        row = make_row(vao='X')
        assert extract_avis(row) == 'VAO'

    def test_ref_col16(self):
        row = make_row(ref_col='X')
        assert extract_avis(row) == 'REF'

    def test_hm_col17(self):
        row = make_row(hm='X')
        assert extract_avis(row) == 'HM'

    def test_none_no_checkbox(self):
        row = make_row()
        assert extract_avis(row) is None

    def test_lowercase_x(self):
        row = make_row(vso='x')
        # 'x' is not 'X' uppercase — should return None per spec
        assert extract_avis(row) is None


class TestIsTerrellDataTable:
    def test_true_valid_19col(self):
        header = ['Désignation'] + [''] * 18
        table = [header, [''] * 19, make_row()]
        assert is_terrell_data_table(table) is True

    def test_false_wrong_col_count(self):
        header = ['Désignation'] + [''] * 10
        table = [header]
        assert is_terrell_data_table(table) is False

    def test_false_empty_table(self):
        assert is_terrell_data_table([]) is False

    def test_false_no_designation(self):
        header = ['SomeOtherHeader'] + [''] * 18
        table = [header]
        assert is_terrell_data_table(table) is False


class TestIsDrawingFile:
    def test_drawing_p17_t2_underscore(self):
        assert is_drawing_file('P17_T2_HO_EXE_LGD_GOE_H003_ARM_HZ_R0_328129_A.pdf') is True

    def test_drawing_p17_t2_hyphen(self):
        assert is_drawing_file('P17-T2_HO_EXE_drawing.pdf') is True

    def test_fiche_not_drawing(self):
        assert is_drawing_file('2844-17&Co-TRS-FE003.pdf') is False


class TestParseDate:
    def test_ged_with_date(self):
        date_str, source = parse_date('GED\nle22/01/2024')
        assert date_str == '22/01/2024'
        assert source == 'GED'

    def test_papier(self):
        date_str, source = parse_date('22/01/2024')
        assert date_str == '22/01/2024'
        assert source == 'PAPIER'

    def test_ged_no_date(self):
        date_str, source = parse_date('GED')
        assert date_str == ''
        assert source == 'GED'

    def test_le_prefix(self):
        date_str, source = parse_date('le22/01/2024')
        assert date_str == '22/01/2024'


class TestExtractFicheRef:
    def test_fe003(self):
        assert extract_fiche_ref('2844-17&Co-TRS-FE003.pdf') == 'FE003'

    def test_fe034_inda(self):
        result = extract_fiche_ref('2844-17&Co-TRS-FE034-indA.pdf')
        assert 'FE034' in result

    def test_fe072_a(self):
        result = extract_fiche_ref('2844-17&Co-TRS-FE072-A.pdf')
        assert 'FE072' in result


# ---------------------------------------------------------------------------
# Integration tests — require PDF files
# ---------------------------------------------------------------------------

def _ingest_single(pdf_path: Path):
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copy(str(pdf_path), tmpdir)
        return ingest_terrell_folder(tmpdir)


@pytest.mark.skipif(not FE003.exists(), reason="FE003 PDF not available")
def test_ingest_fe003():
    """FE003: 9 records, all VAO."""
    records, _ = _ingest_single(FE003)
    assert len(records) == 9
    assert all(r['STATUT_NORM'] == 'VAO' for r in records)


@pytest.mark.skipif(not FE082.exists(), reason="FE082 PDF not available")
def test_ingest_fe082():
    """FE082: 5 records, all REF."""
    records, _ = _ingest_single(FE082)
    assert len(records) == 5
    assert all(r['STATUT_NORM'] == 'REF' for r in records)


@pytest.mark.skipif(not FE041.exists(), reason="FE041 PDF not available")
def test_ingest_fe041():
    """FE041: 16 records (VAO=12, HM=4)."""
    records, _ = _ingest_single(FE041)
    assert len(records) == 16
    vao = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
    hm  = sum(1 for r in records if r['STATUT_NORM'] == 'HM')
    assert vao == 12
    assert hm == 4


@pytest.mark.skipif(not FE012.exists(), reason="FE012 PDF not available")
def test_ingest_fe012_38pages():
    """FE012: only 1 record from page 1, not garbage from pages 2–38."""
    records, _ = _ingest_single(FE012)
    # Should only produce records from page 1 (other pages have garbled text)
    assert len(records) >= 1
    assert all(r['PDF_PAGE'] == 1 for r in records)


@pytest.mark.skipif(not DRAWING.exists(), reason="Drawing PDF not available")
def test_drawing_file_skipped():
    """Drawing PDF: 0 records, 1 entry in skipped."""
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copy(str(DRAWING), tmpdir)
        records, skipped = ingest_terrell_folder(tmpdir)
    assert len(records) == 0
    assert len(skipped) == 1
    assert 'drawing' in skipped[0]['reason'].lower()


@pytest.mark.skipif(not TERRELL_FOLDER.exists(), reason="TERRELL folder not available")
def test_grand_total():
    """Full batch: 213 records total (VSO=27, VAO=162, REF=9, HM=15)."""
    records, _ = ingest_terrell_folder(TERRELL_FOLDER)
    assert len(records) == 213
    vso = sum(1 for r in records if r['STATUT_NORM'] == 'VSO')
    vao = sum(1 for r in records if r['STATUT_NORM'] == 'VAO')
    ref = sum(1 for r in records if r['STATUT_NORM'] == 'REF')
    hm  = sum(1 for r in records if r['STATUT_NORM'] == 'HM')
    assert vso == 27
    assert vao == 162
    assert ref == 9
    assert hm == 15


@pytest.mark.skipif(not TERRELL_FOLDER.exists(), reason="TERRELL folder not available")
def test_no_empty_numero():
    """NUMERO must be a non-empty 5-or-6-digit string for every record."""
    records, _ = ingest_terrell_folder(TERRELL_FOLDER)
    for r in records:
        assert r['NUMERO'], f"Empty NUMERO in {r['RAPPORT_ID']}"
        assert re.match(r'^\d{5,6}$', r['NUMERO']), f"Bad NUMERO: {r['NUMERO']}"


@pytest.mark.skipif(not TERRELL_FOLDER.exists(), reason="TERRELL folder not available")
def test_header_rows_skipped():
    """No record should have DESIGNATION == 'Désignation' or reversed header text."""
    records, _ = ingest_terrell_folder(TERRELL_FOLDER)
    designations = [r['DESIGNATION'] for r in records]
    assert 'Désignation' not in designations
    assert 'TEJORP' not in designations
    # Also no record from sub-header
    for r in records:
        assert r.get('NUMERO', '') != 'TEJORP'
