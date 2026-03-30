"""
Tests for GrandFichier reader (processing/grandfichier_reader.py)
Tests use minimal in-memory Excel workbooks.
"""
import sys
from pathlib import Path
import tempfile
import openpyxl

sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.grandfichier_reader import read_grandfichier, _detect_variant
from processing.config import (
    GF_HEADER_ROW, GF_APPROBATEUR_ROW, GF_SUBHEADER_ROW, GF_DATA_START_ROW,
    GF_COL_VARIANT_A, GF_COL_VARIANT_B,
)


def _make_minimal_gf_variant_a(rows: list[list]) -> Path:
    """Create a minimal GrandFichier Variant A sheet (with Zone column)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "LOT TEST"

    # Rows 1-6: title/legend (blank here)
    for _ in range(6):
        ws.append([])

    # Row 7: headers — Variant A
    ws.append([
        "DOCUMENT", "TITRE", "Date diffusion", "LOT", "TYPE DOC",
        "Niv", "Zone", "N° Doc", "IND", "Type", "ANCIEN",
        "N°BDX", "Date réception", "non reçu papier",
        "DATE CONTRACTUELLE VISA SYNTHESE",
        "VISA GLOBAL",
        "MOEX GEMO", "", "", "ARCHI MOX", "", "", "BC SOCOTEC", "", "",
        "OBSERVATIONS",
    ])

    # Row 8: approbateur names
    ws.append([
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "MOEX GEMO", "", "", "ARCHI MOX", "", "", "BC SOCOTEC", "", "",
        "OBSERVATIONS",
    ])

    # Row 9: sub-headers
    ws.append([
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "DATE", "N°", "STATUT", "DATE", "N°", "STATUT", "DATE", "N°", "STATUT",
        "",
    ])

    # Row 10+: data
    for row in rows:
        ws.append(row)

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    wb.save(tmp.name)
    tmp.close()
    return Path(tmp.name)


def _data_row(doc_key="P17T2GETEST001", titre="Test doc", lot="G003",
              type_doc="NDC", numero="028000", indice="A",
              niv="R0", zone="TZ", visa="VSO"):
    """Build a data row aligned with Variant A header."""
    return [
        doc_key, titre, "01/01/2024", lot, type_doc,
        niv, zone, numero, indice, "PDF", "",     # ANCIEN = empty
        "", "", "",
        "15/01/2024",
        visa,
        "15/01/2024", "", "VSO",     # MOEX GEMO group
        "16/01/2024", "", "VAO",     # ARCHI MOX group
        "17/01/2024", "", "HM",      # BC SOCOTEC group
        "",                           # OBSERVATIONS
    ]


def test_reader_basic():
    path = _make_minimal_gf_variant_a([_data_row()])
    try:
        rows, meta = read_grandfichier(path)
        assert len(rows) == 1
        row = rows[0]
        assert row.document_key == "P17T2GETEST001"
        assert row.titre == "Test doc"
        assert row.lot == "G003"
        assert row.type_doc == "NDC"
        assert row.numero == "028000"
        assert row.indice == "A"
    finally:
        path.unlink(missing_ok=True)


def test_reader_detects_approbateurs():
    path = _make_minimal_gf_variant_a([_data_row()])
    try:
        rows, meta = read_grandfichier(path)
        row = rows[0]
        assert len(row.approbateurs) >= 2
        appro_names = [a.name for a in row.approbateurs]
        assert any("MOEX" in n for n in appro_names)
    finally:
        path.unlink(missing_ok=True)


def test_reader_reads_approbateur_current_values():
    path = _make_minimal_gf_variant_a([_data_row()])
    try:
        rows, meta = read_grandfichier(path)
        row = rows[0]
        moex_appro = next((a for a in row.approbateurs if "MOEX" in a.name), None)
        assert moex_appro is not None
        assert moex_appro.current_statut == "VSO"
    finally:
        path.unlink(missing_ok=True)


def test_reader_skips_blank_doc_key_rows():
    rows_data = [_data_row(), [None] * 27]  # second row is blank
    path = _make_minimal_gf_variant_a(rows_data)
    try:
        rows, _ = read_grandfichier(path)
        assert len(rows) == 1
    finally:
        path.unlink(missing_ok=True)


def test_reader_ancien_flag():
    row = _data_row()
    row[10] = 1   # ANCIEN = 1
    path = _make_minimal_gf_variant_a([row])
    try:
        rows, _ = read_grandfichier(path)
        assert rows[0].ancien is True
    finally:
        path.unlink(missing_ok=True)


def test_reader_meta_includes_sheet():
    path = _make_minimal_gf_variant_a([_data_row()])
    try:
        rows, meta = read_grandfichier(path)
        assert "LOT TEST" in meta
        assert meta["LOT TEST"]["row_count"] == 1
    finally:
        path.unlink(missing_ok=True)
