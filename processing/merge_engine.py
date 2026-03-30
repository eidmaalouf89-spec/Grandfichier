"""
JANSA GrandFichier Updater — Merge engine (V1)

Consolidates matched CanonicalResponse records into DeliverableRecords.
Applies source priority rules and detects conflicts.
"""
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional

from processing.models import CanonicalResponse, DeliverableRecord, AnomalyRecord
from processing.config import resolve_worst_tag
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
    matched_records: list[CanonicalResponse],
    gf_rows_by_key: dict[str, "GFRow"],  # document_key → GFRow
    source_priority: dict,
    anomaly_logger: AnomalyLogger,
) -> list[DeliverableRecord]:
    """
    Group matched CanonicalResponse records by (gf_sheet + gf_row) to form DeliverableRecords.
    Apply merge logic: select best response per (document, mission, field).

    Returns list of DeliverableRecord, one per unique GrandFichier row.
    """
    # Group by (sheet, row)
    by_gf_row: dict[tuple[str, int], list[CanonicalResponse]] = defaultdict(list)
    for cr in matched_records:
        key = (cr.gf_sheet, cr.gf_row)
        by_gf_row[key].append(cr)

    deliverables: list[DeliverableRecord] = []

    for (sheet, row_num), responses in by_gf_row.items():
        if not responses:
            continue

        # Representative record for document metadata
        rep = responses[0]

        # Compute consolidated status (worst-case across all responses)
        all_statuses = [cr.normalized_status for cr in responses if cr.normalized_status]
        consolidated = resolve_worst_tag(all_statuses) or "NONE"

        # Collect any conflicts logged during merge
        conflicts: list[AnomalyRecord] = []

        # For comments: collect all unique non-empty comments
        # (merge engine doesn't write directly — writer does append logic)

        drec = DeliverableRecord(
            document_key=rep.document_key,
            lot=rep.lot,
            type_doc=rep.type_doc,
            numero=rep.numero,
            indice=rep.indice,
            titre="",   # set by grandfichier_writer from GFRow.titre
            gf_sheet=sheet,
            gf_row=row_num,
            responses=responses,
            conflicts=conflicts,
            consolidated_status=consolidated,
        )
        deliverables.append(drec)

    logger.info("Merge engine: %d deliverable records built from %d matched responses",
                len(deliverables), len(matched_records))
    return deliverables
