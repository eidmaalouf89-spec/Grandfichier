"""
lesommer_ingest.py — Parser #1: Le Sommer Environnement (AMO HQE)
JANSA VISASIST — BET PDF Report Ingestion
Version 1.0 — April 2026

Public API:
    ingest_lesommer_folder(folder_path) -> (records, skipped)
"""

import re
import logging
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Statut normalisation
# ---------------------------------------------------------------------------

STATUT_MAP = {
    '0': None,   # En attente → skip
    '1': 'REF',
    '2': 'VAO',
    '3': 'VSO',
}


def normalize_statut(raw: str) -> str | None:
    """Normalise a raw statut value to the canonical set or None (skip)."""
    raw = str(raw).strip()
    if raw in STATUT_MAP:
        return STATUT_MAP[raw]
    m = re.match(r'^(VSO|VAO|VAOB|REF|HM)$', raw, re.IGNORECASE)
    return m.group(1).upper() if m else None


# ---------------------------------------------------------------------------
# Ref helpers
# ---------------------------------------------------------------------------

def extract_numero(ref: str) -> str:
    """Extract 6-digit (or 5-digit) NUMERO from a P17 ref or short numeric."""
    m = re.search(r'(\d{5,6})', ref)
    return m.group(1) if m else ''


def extract_indice_from_ref(ref: str) -> str:
    """Extract trailing single uppercase letter from a P17 ref (e.g. _B → 'B')."""
    m = re.search(r'_([A-Z])$', ref.rstrip('_'))
    return m.group(1) if m else ''


def reconstruct_truncated_ref(col2: str, col3: str) -> tuple[str, str]:
    """
    Repair truncated P17 refs caused by A3-page column overflow in pdfplumber.

    If col2 starts with P17_T2_ and does not end with '_' and col3 has no
    space in its first 10 chars → col3 is a continuation fragment.
    """
    if (col2.startswith('P17_T2_')
            and not col2.endswith('_')
            and col3
            and ' ' not in col3[:10]):
        return col2 + col3, ''
    return col2, col3


# ---------------------------------------------------------------------------
# Table detection helpers
# ---------------------------------------------------------------------------

def _row_text(row) -> str:
    return ' '.join(str(c).strip() if c else '' for c in row)


def is_luminaires_table(table: list) -> bool:
    """Detect LUMINAIRES_NDC table: header has 'Référence FT' AND 'Etat visa'."""
    for row in table[:5]:
        text = _row_text(row)
        if 'Référence FT' in text and 'Etat visa' in text:
            return True
        if 'erence FT' in text.lower() and 'tat visa' in text.lower():
            return True
    return False


def is_visa_main_table(table: list) -> bool:
    """Detect CVC/PLB VISA table: header has 'Date visa' AND 'Statut'."""
    for row in table[:5]:
        text = _row_text(row)
        if 'Date visa' in text and 'Statut' in text:
            return True
        if 'date visa' in text.lower() and 'statut' in text.lower():
            return True
    return False


# ---------------------------------------------------------------------------
# Section & LOT detection
# ---------------------------------------------------------------------------

SECTION_RE = re.compile(
    r'Revue LOT\s+(\w+)\s*[-–]\s*(BUREAUX|AUBERGE|HOTEL|INFRA)',
    re.IGNORECASE
)


def detect_lot_and_section(page_text: str, prev_lot: str, prev_section: str) -> tuple[str, str]:
    """
    Extract LOT_TYPE and SECTION from page header text.
    Carries forward previous values if the page is a continuation (no header).
    """
    m = SECTION_RE.search(page_text)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    return prev_lot, prev_section


# ---------------------------------------------------------------------------
# Spatial extraction — LUMINAIRES_NDC (CFO pages)
# ---------------------------------------------------------------------------

P17_RE = re.compile(r'P17_T2_\S+')
STAT_RE = re.compile(r'\b(VSO|VAO|VAOB|REF|HM)\b', re.IGNORECASE)


def extract_spatial_luminaires(page, lot: str, section: str, page_num: int, filename: str) -> list[dict]:
    """
    Bypass extract_tables() for LUMINAIRES_NDC pages.
    Use word coordinates to match P17 refs to their nearest statut tag.
    """
    Y_TOL = 3

    def y_band(y):
        return round(y / Y_TOL) * Y_TOL

    words = page.extract_words()
    if not words:
        logger.warning("Page %d: no text extracted in %s — may be scanned image", page_num, filename)
        return []

    bands: dict[int, list] = {}
    for w in words:
        bands.setdefault(y_band(w['top']), []).append(w)

    lines = {
        b: ' '.join(w['text'] for w in sorted(ws, key=lambda w: w['x0']))
        for b, ws in bands.items()
    }

    refs_per_band: dict[int, list[str]] = {}
    statuts_per_band: dict[int, str] = {}

    for b, line in lines.items():
        refs = P17_RE.findall(line)
        if refs:
            refs_per_band[b] = refs
        m = STAT_RE.search(line)
        if m:
            statuts_per_band[b] = m.group(1).upper()

    records = []
    for ref_band, refs in sorted(refs_per_band.items()):
        if not statuts_per_band:
            continue
        best_band = min(statuts_per_band, key=lambda sb: abs(sb - ref_band))
        if abs(best_band - ref_band) >= 12:
            continue
        statut_norm = statuts_per_band[best_band]
        for ref in refs:
            ref_clean = ref.rstrip('_')
            records.append({
                'SOURCE': 'LE_SOMMER',
                'LOT_TYPE': lot,
                'RAPPORT_ID': Path(filename).stem,
                'SECTION': section,
                'TABLE_TYPE': 'LUMINAIRES_NDC',
                'REF_DOC': ref_clean,
                'NUMERO': extract_numero(ref_clean),
                'INDICE': extract_indice_from_ref(ref_clean),
                'STATUT_NORM': statut_norm,
                'DATE_VISA': '',
                'COMMENTAIRE': '',
                'PDF_PAGE': page_num,
            })
    return records


# ---------------------------------------------------------------------------
# Table extraction — CVC_VISA and PLB_VISA
# ---------------------------------------------------------------------------

def extract_visa_main(table: list, lot: str, section: str, page_num: int,
                      filename: str, table_type: str) -> list[dict]:
    """
    Extract records from a CVC_VISA or PLB_VISA table.

    Column layout (8 cols):
    [0] Elément | [1] Exigence | [2] Réf doc | [3] Nom produit
    [4] Indice  | [5] Date visa | [6] Statut | [7] Commentaires
    """
    # Find header row index
    hdr_idx = None
    for i, row in enumerate(table[:6]):
        text = _row_text(row)
        if 'Date visa' in text and 'Statut' in text:
            hdr_idx = i
            break
    if hdr_idx is None:
        return []

    records = []
    for row in table[hdr_idx + 1:]:
        if not row or len(row) < 7:
            continue

        col2 = str(row[2]).strip() if row[2] else ''
        col3 = str(row[3]).strip() if len(row) > 3 and row[3] else ''
        col4 = str(row[4]).strip() if len(row) > 4 and row[4] else ''
        col5 = str(row[5]).strip() if len(row) > 5 and row[5] else ''
        col6 = str(row[6]).strip() if len(row) > 6 and row[6] else ''
        col7 = str(row[7]).strip() if len(row) > 7 and row[7] else ''

        # PLB truncation repair
        if table_type == 'PLB_VISA':
            col2, col3 = reconstruct_truncated_ref(col2, col3)

        ref_doc_raw = col2
        indice_raw = col4
        date_visa = col5
        statut_raw = col6
        commentaire = col7

        # Skip empty / cross-ref / placeholder refs
        if not ref_doc_raw or ref_doc_raw.startswith('Voir ') or ref_doc_raw == '-':
            continue

        # Normalise statut
        statut_norm = normalize_statut(statut_raw)
        if statut_norm is None:
            continue

        # Clean date
        if date_visa == 'X':
            date_visa = ''

        # Clean indice
        indice = indice_raw if re.match(r'^[A-Z]$', indice_raw) and indice_raw != 'X' else ''

        # Split multi-value cells
        ref_parts = ref_doc_raw.split('\n')

        for ref_part in ref_parts:
            ref_part = ref_part.strip()
            if not ref_part:
                continue

            if table_type == 'PLB_VISA' or ref_part.startswith('P17_T2_'):
                # P17 full ref
                ref_clean = ref_part.rstrip('_')
                numero = extract_numero(ref_clean)
                # Try to get indice from ref if not from column
                ind_from_ref = extract_indice_from_ref(ref_clean)
                ind_final = ind_from_ref if ind_from_ref else indice
            elif re.match(r'^\d{5,6}$', ref_part):
                # Short numeric ref (CVC)
                ref_clean = ref_part
                numero = ref_part
                ind_final = indice
            else:
                continue

            if not numero:
                continue

            records.append({
                'SOURCE': 'LE_SOMMER',
                'LOT_TYPE': lot,
                'RAPPORT_ID': Path(filename).stem,
                'SECTION': section,
                'TABLE_TYPE': table_type,
                'REF_DOC': ref_clean,
                'NUMERO': numero,
                'INDICE': ind_final,
                'STATUT_NORM': statut_norm,
                'DATE_VISA': date_visa,
                'COMMENTAIRE': commentaire[:200],
                'PDF_PAGE': page_num,
            })

    return records


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_lesommer_folder(
    folder_path: str | Path,
) -> tuple[list[dict], list[dict]]:
    """
    Process all Le Sommer PDF reports in a folder.

    Returns:
        records : list[dict]  — one dict per extracted avis (schema per spec §5)
        skipped : list[dict]  — {"file": str, "page": int, "reason": str}
    """
    folder_path = Path(folder_path)
    pdf_files = sorted(folder_path.glob('*.pdf'))
    all_records: list[dict] = []
    skipped: list[dict] = []

    for pdf_path in pdf_files:
        filename = pdf_path.name
        logger.info("Processing Le Sommer file: %s", filename)
        current_lot, current_section = 'UNKNOWN', 'UNKNOWN'

        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    try:
                        text = page.extract_text() or ''
                        current_lot, current_section = detect_lot_and_section(
                            text, current_lot, current_section
                        )
                        tables = page.extract_tables()
                        spatial_done = False

                        for table in tables:
                            if not table:
                                continue
                            if is_luminaires_table(table) and not spatial_done:
                                recs = extract_spatial_luminaires(
                                    page, current_lot, current_section,
                                    page_num, filename
                                )
                                all_records.extend(recs)
                                spatial_done = True
                            elif is_visa_main_table(table):
                                table_type = 'CVC_VISA' if current_lot == 'CVC' else 'PLB_VISA'
                                recs = extract_visa_main(
                                    table, current_lot, current_section,
                                    page_num, filename, table_type
                                )
                                all_records.extend(recs)

                    except Exception as e:
                        logger.warning("Page %d of %s: error — %s", page_num, filename, e)
                        skipped.append({'file': filename, 'page': page_num, 'reason': str(e)})

        except Exception as e:
            logger.warning("Failed to open %s: %s", filename, e)
            skipped.append({'file': filename, 'page': 0, 'reason': str(e)})

    logger.info(
        "Le Sommer ingest complete: %d records from %d files, %d skipped",
        len(all_records), len(pdf_files), len(skipped)
    )
    return all_records, skipped
