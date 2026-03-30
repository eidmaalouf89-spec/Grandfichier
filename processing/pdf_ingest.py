"""
JANSA GrandFichier Updater — PDF report ingestion (V1)

Accepts a folder of PDF files and attempts to parse reviewer responses.
Uses deterministic pattern matching (pdfplumber) — NO AI/LLM.

Strategy per PDF:
- Extract text from all pages
- Try to match known patterns (document reference, response tag, date, reviewer)
- HIGH CONFIDENCE parse → emit CanonicalResponse
- LOW CONFIDENCE / AMBIGUOUS → emit parse_warnings, still return record with raw_data
- TOTAL FAILURE → emit AnomalyRecord via caller (returned in skipped list)

Known limitations (V1):
- Pattern list must be validated against real PDF report files
- Different reviewers may use different formats — extend PDF_PATTERNS as needed
- Multi-page PDFs: first match wins per pattern
"""
import re
import logging
from pathlib import Path
from typing import Optional

from processing.models import CanonicalResponse
from processing.dates import parse_date, date_to_str
from processing.statuses import get_normalized_code

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pattern library (tune against real PDFs)
# ---------------------------------------------------------------------------

# Document reference: e.g. "P17T2GEEXELGDGOEG003NDCTZTN028000"
# or shorter variants like "NDC-028000" or "028000"
_PAT_DOC_REF = re.compile(
    r"(?:P17T2\w+|[A-Z]{3,5}-?\d{6}|\b\d{6}\b)",
    re.IGNORECASE,
)

# Response tags (French)
_PAT_RESPONSE = re.compile(
    r"(valid[eé]\s+sans\s+observation"
    r"|valid[eé]\s+avec\s+observation"
    r"|refus[eé]"
    r"|d[eé]favorable"
    r"|hors\s+mission"
    r"|suspendu"
    r"|annul[eé]"
    r"|favorable)",
    re.IGNORECASE,
)

# Date patterns: dd/mm/yyyy or dd-mm-yyyy
_PAT_DATE = re.compile(r"\b(\d{2}[/\-]\d{2}[/\-]\d{4})\b")

# Indice/revision: single uppercase letter after "indice" or "révision"
_PAT_INDICE = re.compile(r"(?:indice|r[eé]vision)\s*[:\-]?\s*([A-Z])\b", re.IGNORECASE)

# Status tag map for raw string → status_map key normalization
_RESPONSE_NORMALIZE = {
    "validé sans observation":  "Validé sans observation",
    "valide sans observation":  "Validé sans observation",
    "validé avec observation":  "Validé avec observation",
    "valide avec observation":  "Validé avec observation",
    "refusé":                   "Refusé",
    "refuse":                   "Refusé",
    "défavorable":              "Défavorable",
    "defavorable":              "Défavorable",
    "hors mission":             "Hors Mission",
    "suspendu":                 "Suspendu",
    "annulé":                   "Annulé",
    "annule":                   "Annulé",
    "favorable":                "Favorable",
}


def ingest_pdf_folder(
    folder_path: str | Path,
    status_map: dict,
) -> tuple[list[CanonicalResponse], list[dict]]:
    """
    Process all PDFs in a folder. Returns:
      - list[CanonicalResponse]: successfully parsed records
      - list[dict]: failed/skipped files with reason

    Args:
        folder_path: path to folder containing PDF report files
        status_map: loaded status_map.json dict
    """
    folder_path = Path(folder_path)
    if not folder_path.exists() or not folder_path.is_dir():
        logger.error("PDF folder not found or not a directory: %s", folder_path)
        return [], []

    pdf_files = sorted(folder_path.glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in: %s", folder_path)
        return [], []

    logger.info("Processing %d PDF files from: %s", len(pdf_files), folder_path)

    try:
        import pdfplumber
    except ImportError:
        logger.error(
            "pdfplumber not installed. Run: pip install pdfplumber --break-system-packages"
        )
        return [], [{"file": str(f), "reason": "pdfplumber not installed"} for f in pdf_files]

    records: list[CanonicalResponse] = []
    skipped: list[dict] = []

    for pdf_path in pdf_files:
        try:
            rec, skip_info = _parse_single_pdf(pdf_path, status_map, pdfplumber)
            if rec is not None:
                records.append(rec)
            if skip_info:
                skipped.append(skip_info)
        except Exception as e:
            logger.warning("PDF parse error for '%s': %s", pdf_path.name, e)
            skipped.append({"file": pdf_path.name, "reason": str(e)})

    logger.info(
        "PDF ingest complete: %d records parsed, %d skipped/failed",
        len(records), len(skipped),
    )
    return records, skipped


def _parse_single_pdf(
    pdf_path: Path,
    status_map: dict,
    pdfplumber,
) -> tuple[Optional[CanonicalResponse], Optional[dict]]:
    """
    Parse a single PDF file. Returns (CanonicalResponse or None, skip_info or None).
    """
    source_file = pdf_path.name
    full_text = ""
    pages_text = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            pages_text.append(txt)
            full_text += "\n" + txt

    if not full_text.strip():
        logger.warning("PDF '%s': no text extracted (possibly scanned image)", source_file)
        return None, {"file": source_file, "reason": "no text extracted — may be scanned image"}

    parse_warnings = []

    # --- Try to extract document reference ---
    doc_refs = _PAT_DOC_REF.findall(full_text)
    document_key = doc_refs[0].upper() if doc_refs else ""
    if not document_key:
        parse_warnings.append("Could not extract document reference from PDF text")

    # --- Try to extract response tag ---
    response_match = _PAT_RESPONSE.search(full_text)
    raw_status = ""
    normalized_status = "NONE"
    if response_match:
        raw_found = response_match.group(0).strip().lower()
        raw_status = _RESPONSE_NORMALIZE.get(raw_found, raw_found)
        normalized_status = get_normalized_code(raw_status, status_map)
    else:
        parse_warnings.append("Could not extract response tag from PDF text")

    # --- Try to extract date ---
    date_matches = _PAT_DATE.findall(full_text)
    response_date = ""
    for dm in date_matches:
        d = parse_date(dm.replace("-", "/"))
        if d:
            response_date = date_to_str(d)
            break
    if not response_date:
        parse_warnings.append("Could not extract response date from PDF text")

    # --- Try to extract indice ---
    indice_match = _PAT_INDICE.search(full_text)
    indice = indice_match.group(1).upper() if indice_match else ""

    # Try to extract numero from document_key or filename
    numero = _extract_numero_from_ref(document_key) or _extract_numero_from_filename(source_file)

    # Determine confidence
    confidence_score = sum([
        bool(document_key),
        bool(raw_status),
        bool(response_date),
    ])
    confidence = "PARTIAL" if confidence_score >= 2 else "UNMATCHED"

    source_page = f"page 1"  # simplified — page detection can be enhanced later

    rec = CanonicalResponse(
        source_type="REPORT",
        source_file=source_file,
        source_row_or_page=source_page,
        document_key=document_key,
        lot="",       # PDF extraction doesn't reliably give LOT — matcher uses doc_key
        type_doc="",
        numero=numero,
        indice=indice,
        batiment="",
        zone="",
        niveau="",
        emetteur="",
        mission="",   # reviewer identity may be in PDF filename or header — V1 leaves blank
        respondant="",
        raw_status=raw_status,
        normalized_status=normalized_status,
        response_date=response_date,
        deadline_date="",
        days_delta=None,
        comment=full_text[:500].strip(),  # store first 500 chars as raw comment
        attachments="",
        confidence=confidence,
        parse_warnings=parse_warnings,
    )

    skip_info = None
    if confidence == "UNMATCHED":
        skip_info = {
            "file": source_file,
            "reason": "low confidence parse",
            "warnings": parse_warnings,
        }
        logger.warning(
            "PDF '%s': low confidence parse — %s", source_file, "; ".join(parse_warnings)
        )

    return rec, skip_info


def _extract_numero_from_ref(doc_key: str) -> str:
    """Try to extract a 6-digit numero from a document key."""
    m = re.search(r"\b(\d{6})\b", doc_key)
    return m.group(1) if m else ""


def _extract_numero_from_filename(filename: str) -> str:
    """Try to extract a 6-digit numero from a PDF filename."""
    m = re.search(r"(\d{6})", Path(filename).stem)
    return m.group(1) if m else ""
