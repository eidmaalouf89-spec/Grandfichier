"""
Tests for GED ingestion (processing/ged_ingest.py)
Tests use a minimal in-memory Excel workbook to avoid needing real GED files.
"""
import sys
from pathlib import Path
import tempfile
import openpyxl

sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.ged_ingest import ingest_ged
from processing.statuses import load_status_map

STATUS_MAP_PATH = Path(__file__).parent.parent / "data" / "status_map.json"


def _make_minimal_ged_excel(rows: list[list]) -> Path:
    """Create a minimal GED Excel file in a temp dir and return its path."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Vue détaillée des documents"

    # Row 1: ignored
    ws.append(["ignored"])
    # Row 2: headers
    ws.append([
        "Chemin", "Identifiant", "AFFAIRE", "PROJET", "BATIMENT", "PHASE",
        "EMETTEUR", "SPECIALITE", "LOT", "TYPE DE DOC", "ZONE", "NIVEAU",
        "NUMERO", "INDICE", "Libellé", "Description", "Format", "Version créée par",
        "Date prévisionnelle", "Date de dépôt effectif", "Écart dépôt", "Version",
        "Dernière modification", "Taille (Mo)", "Statut final",
        "Mission", "Répondant", "Date limite", "Réponse donnée le",
        "Écart réponse", "Réponse", "Commentaire", "Pièces jointes",
        "Type de réponse", "Mission associée",
    ])
    # Rows 3+: data
    for row in rows:
        ws.append(row)

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    wb.save(tmp.name)
    tmp.close()
    return Path(tmp.name)


def _base_row():
    """Return a complete valid GED data row."""
    return [
        "/path/to/doc",       # 0 chemin
        "12345",              # 1 identifiant
        "P17",                # 2 affaire
        "T2",                 # 3 projet
        "GE",                 # 4 batiment
        "EXE",                # 5 phase
        "LGD",                # 6 emetteur
        "GOE",                # 7 specialite
        "I003",               # 8 lot
        "NDC",                # 9 type_doc
        "TZ",                 # 10 zone
        "TX",                 # 11 niveau
        "028000",             # 12 numero
        "A",                  # 13 indice
        "Titre du doc",       # 14 libelle
        "",                   # 15 description
        "PDF",                # 16 format
        "User Name",          # 17 version créée par
        "01/01/2024",         # 18 date prev
        "15/01/2024",         # 19 date dépôt
        -5,                   # 20 écart dépôt
        "1",                  # 21 version
        "2024-01-15",         # 22 dernière modif
        0.5,                  # 23 taille
        "Validé",             # 24 statut final
        "0-Maître d'Oeuvre EXE",  # 25 mission
        "Jean Dupont",        # 26 répondant
        "20/01/2024",         # 27 date limite
        "18/01/2024",         # 28 réponse le
        2,                    # 29 écart réponse
        "Validé avec observation",  # 30 réponse
        "Commentaire test",   # 31 commentaire
        "pj.pdf",             # 32 pièces jointes
        "Normal",             # 33 type réponse
        "",                   # 34 mission associée
    ]


def test_ingest_basic_row():
    sm = load_status_map(STATUS_MAP_PATH)
    path = _make_minimal_ged_excel([_base_row()])
    try:
        records, skipped = ingest_ged(path, sm)
        assert len(records) == 1
        rec = records[0]
        assert rec.source_type == "GED"
        assert rec.lot == "I003"
        assert rec.type_doc == "NDC"
        assert rec.numero == "028000"
        assert rec.indice == "A"
        assert rec.mission == "0-Maître d'Oeuvre EXE"
        assert rec.normalized_status == "VAO"
        assert rec.comment == "Commentaire test"
    finally:
        path.unlink(missing_ok=True)


def test_ingest_skips_rows_without_mission():
    sm = load_status_map(STATUS_MAP_PATH)
    row = _base_row()
    row[25] = ""   # empty mission
    path = _make_minimal_ged_excel([row])
    try:
        records, skipped = ingest_ged(path, sm)
        assert len(records) == 0
    finally:
        path.unlink(missing_ok=True)


def test_ingest_skips_empty_rows():
    sm = load_status_map(STATUS_MAP_PATH)
    empty_row = [None] * 35
    path = _make_minimal_ged_excel([empty_row])
    try:
        records, _ = ingest_ged(path, sm)
        assert len(records) == 0
    finally:
        path.unlink(missing_ok=True)


def test_ingest_multiple_rows():
    sm = load_status_map(STATUS_MAP_PATH)
    row1 = _base_row()
    row2 = _base_row()
    row2[30] = "Validé sans observation"
    row2[25] = "0-BET Structure"
    path = _make_minimal_ged_excel([row1, row2])
    try:
        records, _ = ingest_ged(path, sm)
        assert len(records) == 2
        assert records[0].normalized_status == "VAO"
        assert records[1].normalized_status == "VSO"
    finally:
        path.unlink(missing_ok=True)


def test_ingest_response_date_parsed():
    sm = load_status_map(STATUS_MAP_PATH)
    row = _base_row()
    row[28] = "18/01/2024"
    path = _make_minimal_ged_excel([row])
    try:
        records, _ = ingest_ged(path, sm)
        assert records[0].response_date == "2024-01-18"
    finally:
        path.unlink(missing_ok=True)
