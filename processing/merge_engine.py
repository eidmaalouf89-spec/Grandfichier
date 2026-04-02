"""
JANSA GrandFichier Updater — Merge engine (V1)

Consolidates matched CanonicalResponse records into DeliverableRecords.
Applies source priority rules and detects conflicts.

V2.0 change: VISA GLOBAL is no longer computed here as worst-tag.
MOEX responses are passed through unchanged; the writer resolves them
directly into the VISA GLOBAL column using mission_map group lookup.
"""
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional

from processing.models import CanonicalResponse, DeliverableRecord, AnomalyRecord
from processing.dates import str_to_date, compare_dates
from processing.anomalies import AnomalyLogger

logger = logging.getLogger(__name__)


def load_source_priority(path: Path) -> dict:
    """Load source_priority.json. Returns field → ordered list of source types."""
    with open(path, "r", encoding="utf-8") as f:
        sp = json.load(f)
    sp.pop("_meta", None)
    sp.pop("_version", None)
    sp.pop("_notes", None)
    return sp


def _source_rank(source_type: str, priority_list: list[str]) -> int:
    """Return priority rank (lower = higher priority). Unknown sources get low priority."""
    try:
        return priority_list.index(source_type)
    except ValueError:
        return len(priority_list) + 99


# NOTE: _pick_best_response and _get_field_value are defined here for reference
# but are NOT called by build_deliverables(). Source priority resolution happens
# in grandfichier_writer.py. Kept here for documentation / future use.
def _pick_best_response(
    responses: list[CanonicalResponse],
    field: str,
    source_priority: dict,
    anomaly_logger: AnomalyLogger,
) -> Optional[CanonicalResponse]:
    """
    From a list of CanonicalResponse records all for the same (document, mission),
    select the best one for a given field according to source priority rules.
    Logs STATUS_CONFLICT anomaly if two sources at the same priority level conflict.
    Returns the best CanonicalResponse.
    """
    if not responses:
        return None
    if len(responses) == 1:
        return responses[0]

    priority_list = source_priority.get(field, ["GED", "SAS", "REPORT"])

    # Sort by source priority
    sorted_responses = sorted(
        responses,
        key=lambda r: _source_rank(r.source_type, priority_list),
    )

    best = sorted_responses[0]
    second = sorted_responses[1] if len(sorted_responses) > 1 else None

    if second:
        # Check if there's a conflict between top two at same priority level
        best_rank = _source_rank(best.source_type, priority_list)
        second_rank = _source_rank(second.source_type, priority_list)

        best_val = _get_field_value(best, field)
        second_val = _get_field_value(second, field)

        if best_val != second_val and best_rank == second_rank:
            anomaly_logger.log_status_conflict(
                source_type=best.source_type,
                source_file=best.source_file,
                source_row=best.source_row_or_page,
                document_key=best.document_key,
                field=field,
                value_a=best_val,
                source_a=best.source_type,
                value_b=second_val,
                source_b=second.source_type,
            )

    return best


# NOTE: See comment above _pick_best_response — not called by build_deliverables().
def _get_field_value(cr: CanonicalResponse, field: str) -> str:
    """Extract the relevant field value from a CanonicalResponse."""
    mapping = {
        "status":         cr.normalized_status,
        "response_date":  cr.response_date,
        "comment":        cr.comment,
        "date_reception": cr.response_date,
        "numero_bdx":     cr.attachments,
    }
    return mapping.get(field, "")


def build_deliverables(
    matched_pairs: list,  # list of (GFRow, list[CanonicalResponse])
    gf_rows_by_sheet_row: dict,
    source_priority: dict,
    anomaly_logger: AnomalyLogger,
) -> list[DeliverableRecord]:
    """
    Convert (GFRow, responses) pairs into DeliverableRecords for the writer.
    Each DeliverableRecord = one GF row + all its GED responses.

    Returns list of DeliverableRecord, one per matched GrandFichier row.
    """
    from processing.models import GFRow
    deliverables: list[DeliverableRecord] = []
    total_responses = 0

    for gf_row, responses in matched_pairs:
        if not responses:
            continue
        rep = responses[0]
        drec = DeliverableRecord(
            document_key=gf_row.document_key,
            lot=gf_row.lot,
            type_doc=gf_row.type_doc,
            numero=gf_row.numero,
            indice=gf_row.indice,
            titre=gf_row.titre,
            gf_sheet=gf_row.sheet_name,
            gf_row=gf_row.row_number,
            responses=responses,
            conflicts=[],
            consolidated_status="",
        )
        deliverables.append(drec)
        total_responses += len(responses)

    logger.info("Merge engine: %d deliverable records built from %d matched responses",
                len(deliverables), total_responses)
    return deliverables
