"""
terrell_ingest.py — Parser #3: TERRELL (BET Structure GOE)
JANSA VISASIST — BET PDF Report Ingestion
Version 1.0 — April 2026

Public API:
    ingest_terrell_folder(folder_path) -> (records, skipped)
"""

import re
import logging
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

def is_drawing_file(filename: str) -> bool:
    """Return True if the PDF is a document/drawing, not a fiche exam."""
    stem = Path(filename).stem
    return stem.startswith('P17_T2_') or stem.startswith('P17-T2_')


def extract_fiche_ref(filename: str) -> str:
    """
    Extract fiche reference from filename.

    Examples:
      "2844-17&Co-TRS-FE003.pdf"        → "FE003"
      "2844-17&Co-TRS-FE034-indA.pdf"   → "FE034-IND A" (uppercased)
      "2844-17&Co-TRS-FE072-A.pdf"      → "FE072-A"
    """
    m = re.search(r'(FE\d+(?:-(?:ind)?[A-Z])?)', filename, re.IGNORECASE)
    return m.group(1).upper() if m else Path(filename).stem


# ---------------------------------------------------------------------------
# Table detection
# ---------------------------------------------------------------------------

def is_terrell_data_table(table: list) -> bool:
    """
    Return True if this table is a Terrell 19-column avis table.
    Detection: must have exactly 19 columns AND first row must contain
    'Désignation' or similar.
    """
    if not table or len(table[0]) != 19:
        return False
    row0_text = ' '.join(str(c) for c in table[0] if c)
    return ('Désignation' in row0_text
            or 'Designation' in row0_text
            or 'DESIGNAT' in row0_text.upper()
            or 'signation' in row0_text)


# ---------------------------------------------------------------------------
# P17 ref reconstruction
# ---------------------------------------------------------------------------

def reconstruct_p17_ref(row_clean: list) -> str | None:
    """
    Reconstruct a full P17 reference from the 19-column Terrell table row.
    Returns None if the row does not contain a valid P17 document.
    """
    if len(row_clean) < 13:
        return None

    projet     = row_clean[1].strip()
    tranche    = row_clean[2].strip()
    bat        = row_clean[3].strip()
    phase      = row_clean[4].strip()
    emetteur   = row_clean[5].strip()
    specialite = row_clean[6].strip()
    lot        = row_clean[7].strip()
    type_doc   = row_clean[8].strip()
    zone       = row_clean[9].strip()
    niveau     = row_clean[10].strip()
    numero_raw = row_clean[11].strip()
    indice     = row_clean[12].strip()

    # Validate P17
    if projet.upper() != 'P17':
        return None

    # Reconstruct NUMERO: remove all spaces from the digit sequence
    numero = re.sub(r'\s+', '', numero_raw)
    if not re.match(r'^\d{5,6}$', numero):
        return None

    # Build full ref
    ref = f"{projet}_{tranche}_{bat}_{phase}_{emetteur}_{specialite}_{lot}_{type_doc}_{zone}_{niveau}_{numero}_{indice}"
    return ref


# ---------------------------------------------------------------------------
# Avis extraction
# ---------------------------------------------------------------------------

AVIS_COLS = {14: 'VSO', 15: 'VAO', 16: 'REF', 17: 'HM'}


def extract_avis(row_clean: list) -> str | None:
    """
    Find which checkbox column (14–17) has exactly 'X' (uppercase only, per spec).
    Returns the avis name or None if no checkbox is marked.

    Note: lowercase 'x' is NOT a valid checkbox indicator per spec.
    Unicode checkmarks (✓/✗) are also accepted.
    """
    for col_idx, avis_name in AVIS_COLS.items():
        if col_idx < len(row_clean):
            val = row_clean[col_idx].strip()
            if val in ('X', '✓', '✗'):
                return avis_name
    return None


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_date(date_raw: str) -> tuple[str, str]:
    """
    Extract date and source from col 13.

    Examples:
      "GED\\nle22/01/2024"  → ("22/01/2024", "GED")
      "le22/01/2024"        → ("22/01/2024", "GED")
      "22/01/2024"          → ("22/01/2024", "PAPIER")
      "GED"                 → ("", "GED")
    """
    date_source = 'GED' if 'GED' in date_raw.upper() else 'PAPIER'
    m = re.search(r'(\d{2}/\d{2}/\d{4})', date_raw)
    date_str = m.group(1) if m else ''
    return date_str, date_source


# ---------------------------------------------------------------------------
# Row parser
# ---------------------------------------------------------------------------

def parse_terrell_row(row: list, fiche_ref: str, page_num: int) -> dict | None:
    """
    Parse one data row from a 19-column Terrell table.
    Returns a record dict or None if the row should be skipped.
    """
    cleaned = [str(c).strip() if c else '' for c in row]

    ref_doc = reconstruct_p17_ref(cleaned)
    if not ref_doc:
        return None

    avis = extract_avis(cleaned)
    if not avis:
        return None

    numero = re.sub(r'\s+', '', cleaned[11]) if len(cleaned) > 11 else ''
    date_raw = cleaned[13] if len(cleaned) > 13 else ''
    date_str, date_source = parse_date(date_raw)
    observations = cleaned[18][:200] if len(cleaned) > 18 else ''

    return {
        'SOURCE': 'TERRELL',
        'RAPPORT_ID': fiche_ref,
        'DESIGNATION': cleaned[0],
        'REF_DOC': ref_doc,
        'NUMERO': numero,
        'BAT': cleaned[3] if len(cleaned) > 3 else '',
        'EMETTEUR': cleaned[5] if len(cleaned) > 5 else '',
        'SPECIALITE': cleaned[6] if len(cleaned) > 6 else '',
        'LOT': cleaned[7] if len(cleaned) > 7 else '',
        'TYPE_DOC': cleaned[8] if len(cleaned) > 8 else '',
        'ZONE': cleaned[9] if len(cleaned) > 9 else '',
        'NIVEAU': cleaned[10] if len(cleaned) > 10 else '',
        'INDICE': cleaned[12] if len(cleaned) > 12 else '',
        'DATE_RECEPT': date_str,
        'DATE_SOURCE': date_source,
        'STATUT_NORM': avis,
        'OBSERVATIONS': observations,
        'PDF_PAGE': page_num,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_terrell_folder(
    folder_path: str | Path,
) -> tuple[list[dict], list[dict]]:
    """
    Process all TERRELL PDF Fiches d'Examen in a folder.

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

        # Skip drawing/document files (P17_T2_... filenames)
        if is_drawing_file(filename):
            logger.warning("Skipping drawing file (not a fiche): %s", filename)
            skipped.append({'file': filename, 'page': 0, 'reason': 'drawing file, not a fiche'})
            continue

        fiche_ref = extract_fiche_ref(filename)
        logger.info("Processing Terrell fiche: %s → %s", filename, fiche_ref)

        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()
                    for table in tables:
                        if not is_terrell_data_table(table):
                            continue
                        # Skip rows 0 (header) and 1 (reversed sub-header)
                        for row in table[2:]:
                            rec = parse_terrell_row(row, fiche_ref, page_num)
                            if rec:
                                all_records.append(rec)

        except Exception as e:
            logger.warning("Failed to process %s: %s", filename, e)
            skipped.append({'file': filename, 'page': 0, 'reason': str(e)})

    logger.info(
        "Terrell ingest complete: %d records from %d files, %d skipped",
        len(all_records), len(pdf_files), len(skipped)
    )
    return all_records, skipped
