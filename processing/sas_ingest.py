"""
JANSA GrandFichier Updater — SAS extract ingestion (V1 — Structured Placeholder)

SAS (Service d'Approbation et de Synthèse) is a separate conformity check system.
Its export format has NOT yet been provided. This module is a structured placeholder
that documents the expected schema and will emit clear warnings if the format
does not match expectations.

To activate:
1. Obtain a real SAS extract (Excel or CSV)
2. Inspect column names
3. Update SAS_EXPECTED_COLUMNS below with actual column names
4. Implement _parse_sas_row() to map columns to CanonicalResponse fields

IMPORTANT: Do NOT attempt to guess or impute SAS values. If the format does not
match, log AnomalyRecord(PARSE_FAILURE) for each row and return empty list.
"""
import logging
from pathlib import Path
from typing import Optional

from processing.models import CanonicalResponse
from processing.dates import parse_date, date_to_str
from processing.statuses import get_normalized_code

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Expected SAS column names (UPDATE WHEN REAL FORMAT IS KNOWN)
# ---------------------------------------------------------------------------
SAS_EXPECTED_COLUMNS = [
    # Placeholder — replace with real column names from SAS export
    "reference_document",   # document reference / composite key
    "lot",
    "type_doc",
    "numero",
    "indice",
    "reponse",              # conformity check result
    "date_reponse",
    "verificateur",         # checker identity
    "observations",
]

SAS_MISSION_NAME = "0-SAS"   # GED mission name equivalent for SAS records


def ingest_sas(
    file_path: str | Path,
    status_map: dict,
) -> tuple[list[CanonicalResponse], list[dict]]:
    """
    Parse a SAS extract and return list[CanonicalResponse].

    V1 PLACEHOLDER: This function logs a warning and returns an empty list
    until the real SAS format is confirmed and this module is implemented.

    Args:
        file_path: path to the SAS extract file (.xlsx or .csv)
        status_map: loaded status_map.json dict

    Returns:
        (records, skipped_rows)
    """
    file_path = Path(file_path)
    logger.warning(
        "SAS ingest: module is a V1 placeholder. "
        "File '%s' was provided but cannot be parsed until the SAS format is confirmed. "
        "See processing/sas_ingest.py for implementation instructions.",
        file_path.name,
    )

    # Try to at least open the file to validate it exists and is readable
    if not file_path.exists():
        logger.error("SAS file not found: %s", file_path)
        return [], []

    suffix = file_path.suffix.lower()
    if suffix not in (".xlsx", ".xls", ".csv"):
        logger.error(
            "SAS file format '%s' not supported. Expected .xlsx, .xls, or .csv", suffix
        )
        return [], []

    # Attempt to read column names and warn about expected vs actual
    try:
        actual_columns = _peek_columns(file_path, suffix)
        if actual_columns:
            missing = [c for c in SAS_EXPECTED_COLUMNS if c not in actual_columns]
            if missing:
                logger.warning(
                    "SAS file columns do not match expected schema. "
                    "Missing expected columns: %s. "
                    "Actual columns found: %s",
                    missing, actual_columns,
                )
            else:
                logger.info(
                    "SAS file columns look compatible with expected schema. "
                    "Implement _parse_sas_row() to activate ingestion."
                )
    except Exception as e:
        logger.warning("Could not peek SAS file columns: %s", e)

    return [], []


def _peek_columns(file_path: Path, suffix: str) -> list[str]:
    """Try to read the header row of the SAS file without full parsing."""
    if suffix == ".csv":
        with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
            header_line = f.readline()
        return [c.strip() for c in header_line.split(",")]
    else:
        import openpyxl
        wb = openpyxl.load_workbook(str(file_path), data_only=True, read_only=True)
        ws = wb.active
        headers = []
        for row in ws.iter_rows(min_row=1, max_row=1):
            headers = [str(c.value).strip() for c in row if c.value is not None]
            break
        wb.close()
        return headers


def _parse_sas_row(
    row_data: dict,
    source_file: str,
    source_row: str,
    status_map: dict,
) -> Optional[CanonicalResponse]:
    """
    Parse a single SAS row dict into a CanonicalResponse.

    PLACEHOLDER — implement once real SAS format is known.
    Returns None if the row cannot be parsed.
    """
    # TODO: implement when real SAS format confirmed
    # Example structure:
    #   doc_key = build_ged_key({...}) or use reference_document directly
    #   norm_status = get_normalized_code(row_data.get("reponse"), status_map)
    #   ...
    return None
