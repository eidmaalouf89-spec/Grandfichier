"""
avls_ingest.py — Parser #2: AVLS (BET Acoustique / Vibrations)
JANSA VISASIST — BET PDF Report Ingestion
Version 1.0 — April 2026

Public API:
    ingest_avls_folder(folder_path) -> (records, skipped)
"""

import re
import logging
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Avis normalisation
# ---------------------------------------------------------------------------

AVIS_NORM = {
    'VSO': 'VSO',
    'VAO': 'VAO',
    'VAOB': 'VAOB',
    'REF': 'REF',
    'HM': 'HM',
    'NON CONCERNÉ': 'HM',
    'NON CONCERNE': 'HM',
}


def normalize_avis(raw: str) -> str | None:
    """Normalise raw avis tag to canonical value, or None if unknown/skip."""
    cleaned = raw.strip().upper()
    # Normalise accented form
    cleaned = cleaned.replace('\u00c9', 'E').replace('\u00e9', 'e').upper()
    return AVIS_NORM.get(cleaned)


# ---------------------------------------------------------------------------
# P17 ref helpers
# ---------------------------------------------------------------------------

P17_RE = re.compile(r'P17_T2_\S+')


def clean_p17_ref(raw_ref: str) -> str:
    """
    Strip trailing underscores and free-text suffixes from AVLS P17 refs.

    Input:  "P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B_PLAN_RESEAUX"
    Output: "P17_T2_HO_EXE_AXI_CVC_H041_PLN_HZ_R2_349612_B"

    Strategy: find the standard P17 structure up to _NUMERO_INDICE.
    """
    raw = raw_ref.rstrip('_')
    # Try strict P17 structure match: ends with _NUMERO_LETTER
    m = re.match(
        r'(P17_T2_\w+_\d{5,6}_[A-Z])(?:_.*)?$',
        raw
    )
    if m:
        return m.group(1)
    # Fallback: strip anything after the last _[SINGLE_UPPERCASE_LETTER]
    m2 = re.search(r'(.+_[A-Z])(?:_.*)?$', raw)
    if m2:
        return m2.group(1)
    return raw


def extract_numero(ref: str) -> str:
    """Extract 6-digit (or 5-digit) NUMERO from a P17 ref."""
    m = re.search(r'(\d{5,6})', ref)
    return m.group(1) if m else ''


# ---------------------------------------------------------------------------
# Metadata extraction from page 1
# ---------------------------------------------------------------------------

def extract_fiche_metadata(page) -> dict:
    """
    Parse the first 2 tables on page 1 to get report metadata.

    Returns: {lot_label, lot_num, n_visa, indice, date_fiche, reviewer}

    Handles both 6-col (old LOT-GO) and 7-col (new LOT-CVC/PLB) header layouts
    by reading from the right: last col = DATE, second-to-last = IND, etc.
    """
    tables = page.extract_tables()
    metadata = {
        'lot_label': '', 'lot_num': '', 'n_visa': '',
        'indice': '', 'date_fiche': '', 'reviewer': ''
    }

    # Table 1: header row with lot/visa/date info
    if tables:
        t1 = tables[0]
        for row in t1:
            row_clean = [str(c).strip() if c else '' for c in row]
            row_text = ' '.join(row_clean)
            # Extract date
            date_m = re.search(r'(\d{2}/\d{2}/\d{4})', row_text)
            if date_m:
                metadata['date_fiche'] = date_m.group(1)
            # Parse columns from right: ...[LOT_NUM?], LOT, N°VISA, IND, DATE
            if len(row_clean) >= 6:
                metadata['indice'] = row_clean[-2].strip()
                metadata['n_visa'] = row_clean[-3].strip()
                metadata['lot_label'] = row_clean[-4].strip()
                if len(row_clean) >= 7:
                    metadata['lot_num'] = row_clean[-5].strip()

    # Table 2: Objet / VISA établi par / Destinataires
    if tables and len(tables) >= 2:
        for row in tables[1]:
            row_clean = [str(c).strip() if c else '' for c in row]
            if len(row_clean) >= 2:
                col0 = row_clean[0]
                if 'VISA' in col0.upper() and ('tabli' in col0 or 'tablie' in col0 or 'par' in col0.lower()):
                    reviewer_text = row_clean[1]
                    # Extract name before email / parenthesis
                    name_m = re.match(r'^([^(\n]+)', reviewer_text)
                    if name_m:
                        metadata['reviewer'] = name_m.group(1).strip()

    return metadata


# ---------------------------------------------------------------------------
# Table skip detection
# ---------------------------------------------------------------------------

SKIP_SIGNALS = [
    'FICHE VISA', 'REF PROJET',
    'GENERALITES', 'RAPPELS',
    'DOCUMENTS A TRANSMETTRE',
    'Objet :',
]


def _table_has_p17_refs(table: list) -> bool:
    """Return True if any cell in the table contains a P17 ref."""
    for row in table:
        for cell in row:
            if cell and P17_RE.search(str(cell)):
                return True
    return False


def should_skip_table(table: list) -> bool:
    """
    Return True if the table is metadata, legend, or footer — not an avis block.
    """
    if not table:
        return True
    ncols = len(table[0]) if table else 0
    # 1-col or 2-col: orphan observation fragments
    if ncols < 3:
        return True
    # Check first row text against known skip signals
    row0_text = ' '.join(str(c).strip() if c else '' for c in table[0])
    for signal in SKIP_SIGNALS:
        if signal.upper() in row0_text.upper():
            return True
    # Legend block: first cell = "AVIS" with no P17 refs anywhere in the table.
    # Data tables on page 3+ may also have "AVIS" as their column header —
    # do NOT skip those (they contain real P17 refs).
    first_cell = str(table[0][0]).strip().upper() if table[0] else ''
    if first_cell == 'AVIS' and not _table_has_p17_refs(table):
        return True
    return False


# ---------------------------------------------------------------------------
# Avis block parsing
# ---------------------------------------------------------------------------

AVIS_START_RE = re.compile(
    r'^(VSO|VAO|VAOB|REF|HM|Non concern[eé])\b',
    re.IGNORECASE
)


def parse_avis_table(
    table: list,
    metadata: dict,
    page_num: int,
    filename: str,
    initial_avis: str | None = None,
) -> tuple[list[dict], str | None]:
    """
    Parse an avis block table.
    Each block: col0 has avis tag on first row, subsequent rows have ''.
    Last column (or any column) may contain P17 refs.

    Args:
        initial_avis: carry-over avis state from the previous table on the
                      same page (handles blocks that span a table boundary).

    Returns:
        (records, current_avis) — current_avis is the last active avis tag,
        which the caller should pass as initial_avis for the next table call.
    """
    records = []
    current_avis = initial_avis

    for row in table:
        row_clean = [str(c).strip() if c else '' for c in row]

        # Check col0 for avis tag
        col0 = row_clean[0] if row_clean else ''
        avis_m = AVIS_START_RE.match(col0)
        if avis_m:
            raw_avis = avis_m.group(1)
            current_avis = normalize_avis(raw_avis)

        if current_avis is None:
            continue

        # Scan ALL cells in this row for P17 refs
        for cell_text in row_clean:
            refs = P17_RE.findall(cell_text)
            for ref in refs:
                ref_clean = clean_p17_ref(ref)
                if not ref_clean:
                    continue
                numero = extract_numero(ref_clean)
                records.append({
                    'SOURCE': 'AVLS',
                    'RAPPORT_ID': Path(filename).stem,
                    'LOT_LABEL': metadata['lot_label'],
                    'LOT_NUM': metadata['lot_num'],
                    'N_VISA': metadata['n_visa'],
                    'INDICE': metadata['indice'],
                    'DATE_FICHE': metadata['date_fiche'],
                    'REVIEWER': metadata['reviewer'],
                    'REF_DOC': ref_clean,
                    'NUMERO': numero,
                    'STATUT_NORM': current_avis,
                    'PDF_PAGE': page_num,
                })

    return records, current_avis


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_avls_folder(
    folder_path: str | Path,
) -> tuple[list[dict], list[dict]]:
    """
    Process all AVLS PDF reports in a folder.

    Returns:
        records : list[dict]  — one dict per extracted avis (schema per spec §6)
        skipped : list[dict]  — {"file": str, "page": int, "reason": str}
    """
    folder_path = Path(folder_path)
    pdf_files = sorted(folder_path.glob('*.pdf'))
    all_records: list[dict] = []
    skipped: list[dict] = []

    for pdf_path in pdf_files:
        filename = pdf_path.name
        logger.info("Processing AVLS file: %s", filename)

        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                if not pdf.pages:
                    logger.info("No pages in %s — skipping", filename)
                    continue

                # Step 1: metadata from page 1
                metadata = extract_fiche_metadata(pdf.pages[0])

                file_records = 0
                # Step 2: parse all pages for avis data.
                # current_avis is carried across tables within a file so that
                # avis blocks split by a table boundary don't lose their tag.
                current_avis: str | None = None
                for page_num, page in enumerate(pdf.pages, 1):
                    tables = page.extract_tables()
                    for table in tables:
                        if should_skip_table(table):
                            continue
                        recs, current_avis = parse_avis_table(
                            table, metadata, page_num, filename,
                            initial_avis=current_avis,
                        )
                        all_records.extend(recs)
                        file_records += len(recs)

                if file_records == 0:
                    logger.info("No avis records found in %s", filename)

        except Exception as e:
            logger.warning("Failed to process %s: %s", filename, e)
            skipped.append({'file': filename, 'page': 0, 'reason': str(e)})

    logger.info(
        "AVLS ingest complete: %d records from %d files, %d skipped",
        len(all_records), len(pdf_files), len(skipped)
    )
    return all_records, skipped
