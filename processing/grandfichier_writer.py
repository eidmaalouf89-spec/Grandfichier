"""
JANSA GrandFichier Updater — GrandFichier workbook writer (V1)

Applies updates to a GrandFichier workbook (in memory) based on DeliverableRecords.
Produces SourceEvidence records for every field written.
Preserves existing formatting.

Update rules:
- STATUT: overwrite with normalized tag from highest-priority source
- DATE: overwrite if source has newer/non-empty value
- N° (ref number): update if source provides a non-empty value
- VISA GLOBAL: recompute from worst-case TAG_PRIORITY across all approbateurs for this row
- OBSERVATIONS: append new comments — DO NOT overwrite existing
- ANCIEN rows: update normally but flag in evidence export

Status tracking column (added after OBSERVATIONS):
- Header "MISE À JOUR" written once in row 7 per modified sheet
- Existing rows that were updated: "Edited" written in this column
- New rows (GED but not in GF): "New" written in this column
"""
import csv
import json
import logging
import shutil
from pathlib import Path
from typing import Optional

import openpyxl

from processing.models import DeliverableRecord, CanonicalResponse, SourceEvidence, GFRow
from processing.config import resolve_worst_tag, TAG_PRIORITY, GF_DATA_START_ROW, GF_HEADER_ROW
from processing.dates import str_to_date, compare_dates
from processing.actors import resolve_gf_approbateur
from processing.anomalies import AnomalyLogger

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_date(iso_str: str) -> str:
    """Convert ISO date string to dd/mm/yyyy for GrandFichier display."""
    if not iso_str:
        return ""
    try:
        from datetime import date
        d = date.fromisoformat(iso_str[:10])
        return d.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return iso_str


def _is_newer(new_iso: str, existing_str: str) -> bool:
    """
    Return True if new_iso date is newer than existing_str.
    existing_str may be in any parseable format.
    If existing is empty, new is always newer.
    """
    if not new_iso:
        return False
    if not existing_str:
        return True
    from processing.dates import parse_date, date_to_str
    new_d = str_to_date(new_iso)
    existing_d = parse_date(existing_str)
    return compare_dates(new_d, existing_d) > 0


# ---------------------------------------------------------------------------
# Status column helper
# ---------------------------------------------------------------------------

# Tracks which sheets have already received the "MISE À JOUR" header so we
# write it at most once per workbook save, regardless of how many rows are touched.
_status_header_sheets: set[str] = set()


def _get_status_col(ws, obs_col: Optional[int]) -> int:
    """
    Return the 1-based column index for the 'MISE À JOUR' tracking column.
    It always lives immediately to the right of the OBSERVATIONS column.
    If obs_col is unknown we fall back to max_column + 1.
    Writes the column header in GF_HEADER_ROW the first time a given sheet
    is touched within this process run.
    """
    status_col = (obs_col + 1) if obs_col else ((ws.max_column or 1) + 1)

    # Write header once per sheet per run
    sheet_key = f"{id(ws)}:{ws.title}"
    if sheet_key not in _status_header_sheets:
        header_cell = ws.cell(row=GF_HEADER_ROW, column=status_col)
        if not header_cell.value:
            header_cell.value = "MISE À JOUR"
        _status_header_sheets.add(sheet_key)

    return status_col


# ---------------------------------------------------------------------------
# Main writer
# ---------------------------------------------------------------------------

def apply_updates(
    source_grandfichier: str | Path,
    deliverables: list[DeliverableRecord],
    gf_rows_by_sheet_row: dict[tuple[str, int], GFRow],
    mission_map: dict,
    source_priority: dict,
    anomaly_logger: AnomalyLogger,
    output_path: str | Path,
    unmatched_records: list = None,
    gf_rows: list = None,
) -> tuple[list[SourceEvidence], int]:
    """
    Open the GrandFichier workbook, apply all updates, and save to output_path.

    Args:
        source_grandfichier: path to the original GrandFichier .xlsx
        deliverables: list of DeliverableRecord (from merge_engine)
        gf_rows_by_sheet_row: dict (sheet_name, row_number) → GFRow (from reader)
        mission_map: loaded mission_map.json
        source_priority: loaded source_priority.json
        anomaly_logger: AnomalyLogger instance
        output_path: where to save the updated workbook
        unmatched_records: list of CanonicalResponse for NEW_DOCUMENT rows (optional)
        gf_rows: full list of GFRow for lot→sheet routing (optional)

    Returns:
        (evidence_records, fields_updated_count)
    """
    source_grandfichier = Path(source_grandfichier)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Copy original to output path to preserve formatting
    shutil.copy2(str(source_grandfichier), str(output_path))
    logger.info("Copied GrandFichier to: %s", output_path)

    # Reset per-run sheet header cache
    _status_header_sheets.clear()

    wb = openpyxl.load_workbook(str(output_path))
    evidence: list[SourceEvidence] = []
    fields_updated = 0

    # Track which (sheet_name, row_num) pairs were actually modified so we can
    # write "Edited" in the MISE À JOUR column after all updates are applied.
    from collections import defaultdict
    edited_rows: dict[str, set[int]] = defaultdict(set)

    for drec in deliverables:
        sheet_name = drec.gf_sheet
        row_num = drec.gf_row

        if sheet_name not in wb.sheetnames:
            logger.warning("Sheet '%s' not found in workbook — skipping row %d", sheet_name, row_num)
            continue

        ws = wb[sheet_name]
        gf_key = (sheet_name, row_num)
        gf_row = gf_rows_by_sheet_row.get(gf_key)

        if gf_row is None:
            logger.warning("GFRow not found for (%s, %d)", sheet_name, row_num)
            continue

        is_ancien = gf_row.ancien
        gf_appro_names = [a.name for a in gf_row.approbateurs]

        # Group responses by mission
        by_mission: dict[str, list[CanonicalResponse]] = {}
        for cr in drec.responses:
            m = cr.mission
            by_mission.setdefault(m, []).append(cr)

        # For each approbateur in this GF row, find matching responses
        updated_statuts: list[str] = []

        for appro in gf_row.approbateurs:
            # Find GED mission(s) that map to this approbateur
            matching_responses: list[CanonicalResponse] = []

            for ged_mission, responses in by_mission.items():
                gf_name, found = resolve_gf_approbateur(
                    ged_mission, mission_map, gf_appro_names
                )
                if found and gf_name.lower() == appro.name.lower():
                    matching_responses.extend(responses)

            if not matching_responses:
                if appro.current_statut:
                    updated_statuts.append(appro.current_statut)
                continue

            # Pick best response per field using source priority
            priority_list_status = source_priority.get("status", ["GED", "SAS", "REPORT"])
            priority_list_date   = source_priority.get("response_date", ["GED", "SAS", "REPORT"])

            best_status_cr = _pick_best_by_priority(matching_responses, "normalized_status", priority_list_status)
            best_date_cr   = _pick_best_by_priority(matching_responses, "response_date", priority_list_date)

            # ---- Update STATUT ----
            if best_status_cr and best_status_cr.normalized_status and best_status_cr.normalized_status != "NONE":
                new_statut = best_status_cr.normalized_status
                old_statut = appro.current_statut

                if new_statut != old_statut:
                    ws.cell(row=row_num, column=appro.col_statut + 1).value = new_statut
                    ev = SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name=f"STATUT ({appro.name})",
                        old_value=old_statut,
                        new_value=new_statut,
                        source_type=best_status_cr.source_type,
                        source_file=best_status_cr.source_file,
                        source_row_or_page=best_status_cr.source_row_or_page,
                        update_reason=f"GED normalized status ({best_status_cr.match_strategy})",
                    )
                    evidence.append(ev)
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)
                    if is_ancien:
                        ev.update_reason += " [ANCIEN row]"
                updated_statuts.append(new_statut)
            else:
                if appro.current_statut:
                    updated_statuts.append(appro.current_statut)

            # ---- Update DATE ----
            if best_date_cr and best_date_cr.response_date:
                old_date = appro.current_date
                if _is_newer(best_date_cr.response_date, old_date):
                    new_date_display = _fmt_date(best_date_cr.response_date)
                    ws.cell(row=row_num, column=appro.col_date + 1).value = new_date_display
                    evidence.append(SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name=f"DATE ({appro.name})",
                        old_value=old_date,
                        new_value=new_date_display,
                        source_type=best_date_cr.source_type,
                        source_file=best_date_cr.source_file,
                        source_row_or_page=best_date_cr.source_row_or_page,
                        update_reason="GED response date is newer",
                    ))
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)

        # ---- Recompute VISA GLOBAL ----
        if updated_statuts:
            new_visa = resolve_worst_tag(updated_statuts)
            if new_visa and new_visa != gf_row.visa_global:
                col_map = _get_col_map(ws, gf_row)
                if col_map and "visa_global" in col_map:
                    ws.cell(row=row_num, column=col_map["visa_global"] + 1).value = new_visa
                    evidence.append(SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name="VISA GLOBAL",
                        old_value=gf_row.visa_global,
                        new_value=new_visa,
                        source_type="COMPUTED",
                        source_file="",
                        source_row_or_page="",
                        update_reason=f"Worst-case TAG_PRIORITY from {len(updated_statuts)} approbateurs",
                    ))
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)

        # ---- Append OBSERVATIONS ----
        _append_observations(
            ws, gf_row, drec.responses, row_num, sheet_name, evidence, fields_updated
        )

    # ---- Write "Edited" in MISE À JOUR column for all modified existing rows ----
    if edited_rows:
        from processing.grandfichier_reader import _detect_variant, _find_observations_col, _read_approbateurs
        for sheet_name, row_set in edited_rows.items():
            if sheet_name not in wb.sheetnames:
                continue
            ws = wb[sheet_name]
            _, col_map = _detect_variant(ws)
            appros = _read_approbateurs(ws, col_map, ws.max_column or 60)
            obs_col = _find_observations_col(ws, appros, col_map, ws.max_column or 60)
            status_col = _get_status_col(ws, obs_col)
            for row_num in row_set:
                ws.cell(row=row_num, column=status_col).value = "Edited"
        logger.info("Wrote 'Edited' for %d rows across %d sheets",
                    sum(len(v) for v in edited_rows.values()), len(edited_rows))

    # ---- Append new documents with "New" in MISE À JOUR column ----
    if unmatched_records and gf_rows:
        new_appended = append_new_documents(wb, unmatched_records, gf_rows, evidence)
        fields_updated += new_appended

    wb.save(str(output_path))
    logger.info(
        "Writer complete: %d fields updated, %d evidence records, saved to %s",
        fields_updated, len(evidence), output_path,
    )
    return evidence, fields_updated


def _pick_best_by_priority(
    responses: list[CanonicalResponse],
    attr: str,
    priority_list: list[str],
) -> Optional[CanonicalResponse]:
    """Pick the response with the highest-priority source that has a non-empty value for attr."""
    candidates = [r for r in responses if getattr(r, attr, "")]
    if not candidates:
        return None
    return min(candidates, key=lambda r: _source_rank(r.source_type, priority_list))


def _source_rank(source_type: str, priority_list: list[str]) -> int:
    try:
        return priority_list.index(source_type)
    except ValueError:
        return len(priority_list) + 99


def _get_col_map(ws, gf_row: GFRow) -> Optional[dict]:
    """Re-detect variant and return col_map for a GFRow."""
    from processing.grandfichier_reader import _detect_variant
    try:
        _, col_map = _detect_variant(ws)
        return col_map
    except Exception:
        return None


def _append_observations(
    ws, gf_row: GFRow,
    responses: list[CanonicalResponse],
    row_num: int, sheet_name: str,
    evidence: list[SourceEvidence],
    fields_updated_ref,
) -> None:
    """
    Append new comments from responses to the OBSERVATIONS column.
    Never overwrites existing text — only appends new unique comments.
    """
    from processing.grandfichier_reader import _find_observations_col, _detect_variant
    _, col_map = _detect_variant(ws)
    appros = gf_row.approbateurs
    obs_col = _find_observations_col(ws, appros, col_map, ws.max_column or 60)

    if not obs_col:
        return

    existing_obs = gf_row.observations or ""
    new_comments = []

    for cr in responses:
        if cr.comment and cr.comment not in existing_obs:
            new_comments.append(f"[{cr.source_type} {cr.source_row_or_page}] {cr.comment}")

    if new_comments:
        separator = "\n---\n" if existing_obs else ""
        new_obs = existing_obs + separator + "\n".join(new_comments)
        ws.cell(row=row_num, column=obs_col).value = new_obs
        evidence.append(SourceEvidence(
            sheet_name=sheet_name,
            row_number=row_num,
            column_name="OBSERVATIONS",
            old_value=existing_obs[:80] + "..." if len(existing_obs) > 80 else existing_obs,
            new_value=f"[appended {len(new_comments)} comment(s)]",
            source_type="MIXED",
            source_file="",
            source_row_or_page="",
            update_reason="New comments appended from GED/REPORT sources",
        ))


# ---------------------------------------------------------------------------
# New document appender
# ---------------------------------------------------------------------------

def append_new_documents(
    wb: openpyxl.Workbook,
    unmatched_records: list["CanonicalResponse"],
    gf_rows: list["GFRow"],
    evidence: list[SourceEvidence],
) -> int:
    """
    For each NEW_DOCUMENT (GED record with no GrandFichier match), append a new row
    to the correct sheet and write "New" in the OBSERVATIONS (last) column.

    Deduplication: one row per unique document_key × indice combination.
    Multiple mission responses for the same document are collapsed into one row.

    Sheet routing: matched by LOT code — uses the existing GF rows to build
    a lot → sheet_name mapping. Unroutable documents are skipped (logged).

    Returns count of rows appended.
    """
    from collections import defaultdict
    from processing.grandfichier_reader import _detect_variant, _find_observations_col

    # Build lot → sheet_name mapping from existing GF rows
    lot_to_sheet: dict[str, str] = {}
    for gf_row in gf_rows:
        if gf_row.lot and gf_row.sheet_name not in ("", None):
            lot_to_sheet[gf_row.lot.upper()] = gf_row.sheet_name

    # Deduplicate: group by (document_key, indice) → pick one representative + collect missions
    doc_groups: dict[tuple, list] = defaultdict(list)
    for cr in unmatched_records:
        key = (cr.document_key, cr.indice)
        doc_groups[key].append(cr)

    appended = 0

    for (doc_key, indice), responses in doc_groups.items():
        rep = responses[0]  # representative record for document fields

        # Find destination sheet via LOT
        sheet_name = lot_to_sheet.get(rep.lot.upper(), "")
        if not sheet_name:
            # Try normalized lot (strip prefix)
            from processing.canonical import normalize_lot
            lot_norm = normalize_lot(rep.lot)
            for existing_lot, sname in lot_to_sheet.items():
                if normalize_lot(existing_lot) == lot_norm:
                    sheet_name = sname
                    break

        if not sheet_name or sheet_name not in wb.sheetnames:
            logger.debug(
                "New document %s: cannot route to sheet (lot='%s') — skipped",
                doc_key, rep.lot,
            )
            continue

        ws = wb[sheet_name]
        _, col_map = _detect_variant(ws)

        # Find OBSERVATIONS column, then derive MISE À JOUR column (one to the right)
        from processing.grandfichier_reader import _read_approbateurs
        appros = _read_approbateurs(ws, col_map, ws.max_column or 60)
        obs_col = _find_observations_col(ws, appros, col_map, ws.max_column or 60)
        status_col = _get_status_col(ws, obs_col)

        # Append after last data row
        next_row = (ws.max_row or GF_DATA_START_ROW) + 1

        # Write document identification fields
        ws.cell(row=next_row, column=col_map["document"] + 1).value = doc_key
        if rep.type_doc:
            ws.cell(row=next_row, column=col_map["type_doc"] + 1).value = rep.type_doc
        if rep.lot:
            ws.cell(row=next_row, column=col_map["lot"] + 1).value = rep.lot
        if rep.numero:
            ws.cell(row=next_row, column=col_map["numero"] + 1).value = rep.numero
        if indice:
            ws.cell(row=next_row, column=col_map["indice"] + 1).value = indice

        # Write "New" in the dedicated MISE À JOUR column (NOT in OBSERVATIONS)
        ws.cell(row=next_row, column=status_col).value = "New"

        # Record evidence
        evidence.append(SourceEvidence(
            sheet_name=sheet_name,
            row_number=next_row,
            column_name="MISE À JOUR",
            old_value="",
            new_value="New",
            source_type="GED",
            source_file=rep.source_file,
            source_row_or_page=rep.source_row_or_page,
            update_reason="New submittal appended from GED — not previously in GrandFichier",
        ))
        appended += 1

    logger.info("append_new_documents: %d new rows appended across sheets", appended)
    return appended


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

def export_evidence_csv(evidence: list[SourceEvidence], path: Path) -> None:
    """Write evidence_export.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "sheet_name", "gf_row", "column_name", "old_value", "new_value",
            "source_type", "source_file", "source_row", "update_reason"
        ])
        writer.writeheader()
        for ev in evidence:
            writer.writerow(ev.to_dict())
    logger.info("Evidence export written: %s (%d rows)", path, len(evidence))


def export_match_summary_csv(summary_rows: list[dict], path: Path) -> None:
    """Write match_summary.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["match_level", "count", "percentage"])
        writer.writeheader()
        writer.writerows(summary_rows)
    logger.info("Match summary written: %s", path)
