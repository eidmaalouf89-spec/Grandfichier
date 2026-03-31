"""
JANSA GrandFichier Updater — GrandFichier workbook writer (V2)

Applies updates to a GrandFichier workbook (in memory) based on DeliverableRecords.
Produces SourceEvidence records for every field written.
Preserves existing formatting.

Update rules (V2):
- VISA GLOBAL: written from MOEX mission response verbatim (NOT worst-tag computed)
- STATUT/DATE per approbateur: resolved via mission_map group → GF row 8 approbateur name
- Never overwrite existing data with empty / pending (PATCH 5)
- OBSERVATIONS: append new comments from PDF source only — DO NOT overwrite
- Groups with no GF column (MOEX SAS, BET VRD, etc.): logged as NO_GF_COLUMN, not written

Status tracking column (added after OBSERVATIONS):
- Header "MISE À JOUR" written once in row 7 per modified sheet
- Existing rows that were updated: "Edited" written in this column
- New rows (GED but not in GF): "New" written in this column

New rows also get "AJOUT_AUTO_GED" in the SOURCE column if present.
"""
import csv
import logging
import os
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Optional

import openpyxl

from processing.models import DeliverableRecord, CanonicalResponse, SourceEvidence, GFRow, GFApprobateur
from processing.config import GF_DATA_START_ROW, GF_HEADER_ROW
from processing.dates import str_to_date, compare_dates
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
    """Return True if new_iso date is newer than existing_str."""
    if not new_iso:
        return False
    if not existing_str:
        return True
    from processing.dates import parse_date
    new_d = str_to_date(new_iso)
    existing_d = parse_date(existing_str)
    return compare_dates(new_d, existing_d) > 0


def should_update(old_value: str, new_value: str, new_status: str = "") -> bool:
    """
    Return True only if the update adds real information (PATCH 5).
    - Never overwrite with empty
    - Never overwrite existing data with pending/no-response status
    """
    effective = new_status or new_value
    if not effective:
        return False
    if effective in ("EN_ATTENTE", "NONE", "") and old_value:
        return False
    return True


# ---------------------------------------------------------------------------
# Mission group resolution helpers (PATCH 3)
# ---------------------------------------------------------------------------

def _get_mission_group(mission: str, mission_map: dict) -> str:
    """Resolve GED mission name → group name via ged_to_group."""
    return mission_map.get("ged_to_group", {}).get(str(mission).strip(), "")


def _resolve_appro_for_group(
    group: str,
    mission_map: dict,
    gf_approbateurs: list[GFApprobateur],
) -> Optional[GFApprobateur]:
    """
    Find the GFApprobateur object for a given mission group on a specific row.
    Uses group_to_gf_appro variants matched case-insensitively against
    the approbateur names already parsed from row 8.
    """
    candidates = mission_map.get("group_to_gf_appro", {}).get(group, [])
    if not candidates:
        return None

    appro_lower = {a.name.lower(): a for a in gf_approbateurs}
    for candidate in candidates:
        c_lower = candidate.lower()
        if c_lower in appro_lower:
            return appro_lower[c_lower]
        # Partial / contains fallback
        for a_lower, appro_obj in appro_lower.items():
            if c_lower in a_lower or a_lower in c_lower:
                return appro_obj
    return None


# ---------------------------------------------------------------------------
# Status column helper (MISE À JOUR)
# ---------------------------------------------------------------------------

_status_header_sheets: set[str] = set()


def _get_status_col(ws, obs_col: Optional[int]) -> int:
    """
    Return the 1-based column index for the 'MISE À JOUR' tracking column
    (immediately to the right of OBSERVATIONS). Writes the header in row 7
    the first time a given sheet is touched within this process run.
    """
    status_col = (obs_col + 1) if obs_col else ((ws.max_column or 1) + 1)
    sheet_key = f"{id(ws)}:{ws.title}"
    if sheet_key not in _status_header_sheets:
        header_cell = ws.cell(row=GF_HEADER_ROW, column=status_col)
        if not header_cell.value:
            header_cell.value = "MISE À JOUR"
        _status_header_sheets.add(sheet_key)
    return status_col


# Per-sheet column cache — populated once per workbook open (keyed by sheet title)
_visa_global_col_cache: dict[str, int] = {}
_date_contrat_col_cache: dict[str, int] = {}


def _find_visa_global_col(ws, col_map: dict) -> int:
    """
    Find the VISA GLOBAL column (1-indexed) by scanning row 7 for the header text.
    Falls back to col_map["visa_global"] + 1 if not found.
    Cached per sheet to avoid re-scanning on every row.
    """
    key = ws.title
    if key not in _visa_global_col_cache:
        for c in range(1, (ws.max_column or 25) + 1):
            val = str(ws.cell(row=GF_HEADER_ROW, column=c).value or "").replace('\n', ' ').strip().upper()
            if "VISA" in val and "GLOBAL" in val:
                _visa_global_col_cache[key] = c
                break
        else:
            _visa_global_col_cache[key] = col_map.get("visa_global", 14) + 1
    return _visa_global_col_cache[key]


def _find_date_contrat_col(ws, visa_global_col: int) -> int:
    """
    Find the DATE CONTRACTUELLE VISA SYNTHESE column (1-indexed) by scanning row 7.
    Falls back to visa_global_col - 1 if not found.
    Cached per sheet.
    """
    key = ws.title
    if key not in _date_contrat_col_cache:
        for c in range(1, visa_global_col + 1):
            val = str(ws.cell(row=GF_HEADER_ROW, column=c).value or "").replace('\n', ' ').strip().upper()
            if "DATE CONTRACTUELLE" in val or "VISA SYNTHESE" in val:
                _date_contrat_col_cache[key] = c
                break
        else:
            _date_contrat_col_cache[key] = visa_global_col - 1
    return _date_contrat_col_cache[key]


def _find_source_col(ws, obs_col: Optional[int], max_col: int) -> Optional[int]:
    """
    Find the SOURCE column (tagged AJOUT_AUTO_GED for new rows).
    Searches row 7 for a 'SOURCE' header near the end of the sheet.
    """
    search_from = (obs_col or max_col) - 2
    for c in range(max(search_from, 1), max_col + 5):
        val = str(ws.cell(row=GF_HEADER_ROW, column=c).value or "").strip().upper()
        if val == "SOURCE":
            return c
    return None


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
    pdf_only: bool = False,
) -> tuple[list[SourceEvidence], int]:
    """
    Open the GrandFichier workbook, apply all updates, and save to output_path.

    Args:
        source_grandfichier: path to the original GrandFichier .xlsx
        deliverables: list of DeliverableRecord (from merge_engine)
        gf_rows_by_sheet_row: dict (sheet_name, row_number) → GFRow
        mission_map: loaded mission_map.json (v2.0 bidirectional format)
        source_priority: loaded source_priority.json
        anomaly_logger: AnomalyLogger instance
        output_path: where to save the updated workbook
        unmatched_records: list of CanonicalResponse for NEW_DOCUMENT rows (optional)
        gf_rows: full list of GFRow for lot→sheet routing (optional)
        pdf_only: if True, only update OBSERVATIONS (PDF pass)

    Returns:
        (evidence_records, fields_updated_count)
    """
    source_grandfichier = Path(source_grandfichier)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(str(source_grandfichier), str(output_path))
    logger.info("Copied GrandFichier to: %s", output_path)

    _status_header_sheets.clear()
    _visa_global_col_cache.clear()
    _date_contrat_col_cache.clear()

    wb = openpyxl.load_workbook(str(output_path))
    evidence: list[SourceEvidence] = []
    fields_updated = 0

    # Pre-build mission_map lookups
    no_gf_col_groups: set[str] = set(mission_map.get("no_gf_column", []))
    special_groups: dict[str, str] = mission_map.get("special_groups", {})

    # Track which (sheet, row) pairs were actually modified for MISE À JOUR column
    edited_rows: dict[str, set[int]] = defaultdict(set)

    for drec in deliverables:
        sheet_name = drec.gf_sheet
        row_num = drec.gf_row

        if sheet_name not in wb.sheetnames:
            logger.warning("Sheet '%s' not found — skipping row %d", sheet_name, row_num)
            continue

        ws = wb[sheet_name]
        gf_key = (sheet_name, row_num)
        gf_row = gf_rows_by_sheet_row.get(gf_key)

        if gf_row is None:
            logger.warning("GFRow not found for (%s, %d)", sheet_name, row_num)
            continue

        from processing.grandfichier_reader import _detect_variant
        _, col_map = _detect_variant(ws)

        is_ancien = gf_row.ancien

        # ── Group responses by mission (deduplicate: keep best per mission×field) ──
        by_mission: dict[str, list[CanonicalResponse]] = defaultdict(list)
        for cr in drec.responses:
            by_mission[cr.mission].append(cr)

        priority_status = source_priority.get("status", ["GED", "SAS", "REPORT"])
        priority_date   = source_priority.get("response_date", ["GED", "SAS", "REPORT"])

        moex_responses: list[CanonicalResponse] = []

        for mission, responses in by_mission.items():
            group = _get_mission_group(mission, mission_map)

            # ── MOEX: route to VISA GLOBAL ──
            if special_groups.get(group) == "VISA_GLOBAL":
                moex_responses.extend(responses)
                continue

            # ── Skip (MOEX SAS): do nothing ──
            if special_groups.get(group) == "SKIP":
                continue

            # ── No GF column: log anomaly and skip ──
            if group in no_gf_col_groups:
                for cr in responses:
                    anomaly_logger.log_no_gf_column(
                        source_file=cr.source_file,
                        source_row=cr.source_row_or_page,
                        document_key=cr.document_key,
                        mission_name=mission,
                        group_name=group,
                        status=cr.normalized_status,
                        response_date=cr.response_date,
                        comment=cr.comment,
                    )
                continue

            # ── PDF-only pass: only OBSERVATIONS ──
            if pdf_only:
                _append_observations_from_responses(
                    ws, gf_row, responses, row_num, sheet_name, evidence
                )
                continue

            # ── Find approbateur columns for this group ──
            appro = _resolve_appro_for_group(group, mission_map, gf_row.approbateurs)
            if appro is None:
                if group:
                    logger.debug(
                        "Sheet '%s' row %d: group '%s' (mission '%s') — no matching approbateur column",
                        sheet_name, row_num, group, mission,
                    )
                continue

            # Pick best response for status and date
            best_status = _pick_best_by_priority(responses, "normalized_status", priority_status)
            best_date   = _pick_best_by_priority(responses, "response_date", priority_date)

            # ── Write STATUT ──
            if best_status and best_status.normalized_status:
                new_s = best_status.normalized_status
                old_s = appro.current_statut
                if should_update(old_s, new_s, new_s) and new_s != old_s:
                    ws.cell(row=row_num, column=appro.col_statut + 1).value = new_s
                    ev = SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name=f"STATUT ({appro.name})",
                        old_value=old_s,
                        new_value=new_s,
                        source_type=best_status.source_type,
                        source_file=best_status.source_file,
                        source_row_or_page=best_status.source_row_or_page,
                        update_reason=f"GED group '{group}' → appro '{appro.name}' ({best_status.match_strategy})",
                    )
                    if is_ancien:
                        ev.update_reason += " [ANCIEN row]"
                    evidence.append(ev)
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)

            # ── Write DATE ──
            if best_date and best_date.response_date:
                old_d = appro.current_date
                if should_update(old_d, best_date.response_date) and _is_newer(best_date.response_date, old_d):
                    new_d_display = _fmt_date(best_date.response_date)
                    ws.cell(row=row_num, column=appro.col_date + 1).value = new_d_display
                    evidence.append(SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name=f"DATE ({appro.name})",
                        old_value=old_d,
                        new_value=new_d_display,
                        source_type=best_date.source_type,
                        source_file=best_date.source_file,
                        source_row_or_page=best_date.source_row_or_page,
                        update_reason="GED response date is newer",
                    ))
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)

        # ── Write VISA GLOBAL + DATE CONTRACTUELLE VISA SYNTHESE from MOEX response ──
        if moex_responses and not pdf_only:
            # Dynamically locate both columns from row 7 header text (robust across all variants)
            visa_col       = _find_visa_global_col(ws, col_map)
            date_cont_col  = _find_date_contrat_col(ws, visa_col)

            best_moex_s = _pick_best_by_priority(moex_responses, "normalized_status", priority_status)
            best_moex_d = _pick_best_by_priority(moex_responses, "response_date",     priority_date)

            # VISA GLOBAL — MOEX status verbatim
            if best_moex_s and best_moex_s.normalized_status:
                new_vg  = best_moex_s.normalized_status
                old_vg  = str(ws.cell(row=row_num, column=visa_col).value or "")
                if should_update(old_vg, new_vg, new_vg) and new_vg != old_vg:
                    ws.cell(row=row_num, column=visa_col).value = new_vg
                    evidence.append(SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name="VISA GLOBAL",
                        old_value=old_vg,
                        new_value=new_vg,
                        source_type=best_moex_s.source_type,
                        source_file=best_moex_s.source_file,
                        source_row_or_page=best_moex_s.source_row_or_page,
                        update_reason="MOEX response — VISA GLOBAL verbatim (not computed)",
                    ))
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)

            # DATE CONTRACTUELLE VISA SYNTHESE — MOEX response date
            if best_moex_d and best_moex_d.response_date:
                new_dc  = _fmt_date(best_moex_d.response_date)
                old_dc  = str(ws.cell(row=row_num, column=date_cont_col).value or "")
                if should_update(old_dc, new_dc) and _is_newer(best_moex_d.response_date, old_dc):
                    ws.cell(row=row_num, column=date_cont_col).value = new_dc
                    evidence.append(SourceEvidence(
                        sheet_name=sheet_name,
                        row_number=row_num,
                        column_name="DATE CONTRACTUELLE VISA SYNTHESE",
                        old_value=old_dc,
                        new_value=new_dc,
                        source_type=best_moex_d.source_type,
                        source_file=best_moex_d.source_file,
                        source_row_or_page=best_moex_d.source_row_or_page,
                        update_reason="MOEX response date",
                    ))
                    fields_updated += 1
                    edited_rows[sheet_name].add(row_num)

        # ── Append OBSERVATIONS (GED pass only — PDF appends handled separately) ──
        if not pdf_only:
            _append_observations_from_responses(
                ws, gf_row, drec.responses, row_num, sheet_name, evidence
            )

    # ── Write "Edited" in MISE À JOUR column ──
    if edited_rows:
        from processing.grandfichier_reader import _detect_variant, _find_observations_col, _read_approbateurs
        for sname, row_set in edited_rows.items():
            if sname not in wb.sheetnames:
                continue
            ws = wb[sname]
            _, col_map = _detect_variant(ws)
            appros = _read_approbateurs(ws, col_map, ws.max_column or 60)
            obs_col = _find_observations_col(ws, appros, col_map, ws.max_column or 60)
            status_col = _get_status_col(ws, obs_col)
            for row_num in row_set:
                ws.cell(row=row_num, column=status_col).value = "Edited"
        logger.info("Wrote 'Edited' for %d rows across %d sheets",
                    sum(len(v) for v in edited_rows.values()), len(edited_rows))

    # ── Append new documents (unmatched GED records) ──
    if unmatched_records and gf_rows:
        new_appended = append_new_documents(wb, unmatched_records, gf_rows, evidence)
        fields_updated += new_appended

    # Save via /tmp to avoid corrupting FUSE state (writing a large xlsx directly
    # to the FUSE-mounted filesystem breaks subsequent open() calls in this process).
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as _tmp:
        _tmp_xlsx = _tmp.name
    wb.save(_tmp_xlsx)
    shutil.copy2(_tmp_xlsx, str(output_path))
    os.remove(_tmp_xlsx)
    logger.info(
        "Writer complete: %d fields updated, %d evidence records, saved to %s",
        fields_updated, len(evidence), output_path,
    )
    return evidence, fields_updated


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

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
    from processing.grandfichier_reader import _detect_variant
    try:
        _, col_map = _detect_variant(ws)
        return col_map
    except Exception:
        return None


def _append_observations_from_responses(
    ws,
    gf_row: GFRow,
    responses: list[CanonicalResponse],
    row_num: int,
    sheet_name: str,
    evidence: list[SourceEvidence],
) -> None:
    """
    Append new comments from responses to the OBSERVATIONS column.
    Never overwrites existing text — only appends new unique comments.
    Respects should_update: skips if no new comment content.
    """
    from processing.grandfichier_reader import _find_observations_col, _detect_variant, _read_approbateurs
    _, col_map = _detect_variant(ws)
    appros = gf_row.approbateurs
    obs_col = _find_observations_col(ws, appros, col_map, ws.max_column or 60)

    if not obs_col:
        return

    existing_obs = gf_row.observations or ""
    new_comments = [
        f"[{cr.source_type} {cr.source_row_or_page}] {cr.comment}"
        for cr in responses
        if cr.comment and cr.comment not in existing_obs
    ]

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
    to the correct sheet. Writes:
    - Document identification fields (cols 0–9, respecting variant layout)
    - "New" in the MISE À JOUR column
    - "AJOUT_AUTO_GED" in the SOURCE column if present (PATCH 6)

    Deduplication: one row per unique document_key × indice combination.
    Sheet routing: matched by LOT code from existing GF rows.
    Returns count of rows appended.
    """
    from processing.grandfichier_reader import _detect_variant, _find_observations_col, _read_approbateurs
    from processing.canonical import normalize_numero, is_same_sas_ref_document

    # V3.1 PATCH 11 POINT 2: Build SAS REF index from gf_rows for pre-check before new row creation
    # normalized NUMERO → list of GFRow objects that carry SAS REF
    sas_ref_by_numero: dict[str, list] = {}
    for _r in gf_rows:
        if _r.has_sas_ref:
            _num = normalize_numero(_r.numero)
            if _num:
                sas_ref_by_numero.setdefault(_num, []).append(_r)

    # Build lot → sheet_name mapping
    lot_to_sheet: dict[str, str] = {}
    for gf_row in gf_rows:
        if gf_row.lot and gf_row.sheet_name:
            lot_to_sheet[gf_row.lot.upper()] = gf_row.sheet_name

    # Deduplicate by (document_key, indice)
    doc_groups: dict[tuple, list] = defaultdict(list)
    for cr in unmatched_records:
        doc_groups[(cr.document_key, cr.indice)].append(cr)

    appended = 0

    for (doc_key, indice), responses in doc_groups.items():
        rep = responses[0]

        # Route to sheet by LOT
        sheet_name = lot_to_sheet.get(rep.lot.upper() if rep.lot else "", "")
        if not sheet_name:
            from processing.canonical import normalize_lot
            lot_norm = normalize_lot(rep.lot)
            for existing_lot, sname in lot_to_sheet.items():
                if normalize_lot(existing_lot) == lot_norm:
                    sheet_name = sname
                    break

        if not sheet_name or sheet_name not in wb.sheetnames:
            logger.debug("New document %s: cannot route to sheet (lot='%s') — skipped",
                         doc_key, rep.lot)
            continue

        # V3.1 PATCH 11 POINT 2: SAS REF guard — don't create new row if same refused doc
        ged_numero_norm = normalize_numero(rep.numero)
        sas_candidates = sas_ref_by_numero.get(ged_numero_norm, [])
        if sas_candidates:
            if any(is_same_sas_ref_document(rep, sr) for sr in sas_candidates):
                logger.debug(
                    "SAS REF guard: doc %s/%s matches existing SAS REF row — no new row created",
                    doc_key, indice,
                )
                continue  # same refused doc — skip new row creation
            else:
                logger.info(
                    "SAS REF new submittal: doc %s/%s shares NUMERO with SAS REF row "
                    "but is different doc — new row will be created",
                    doc_key, indice,
                )

        ws = wb[sheet_name]
        _, col_map = _detect_variant(ws)
        appros = _read_approbateurs(ws, col_map, ws.max_column or 60)
        obs_col = _find_observations_col(ws, appros, col_map, ws.max_column or 60)
        status_col = _get_status_col(ws, obs_col)

        # Find SOURCE column for AJOUT_AUTO_GED tag (PATCH 6)
        source_col = _find_source_col(ws, obs_col, ws.max_column or 60)

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

        # Write "New" in MISE À JOUR column
        ws.cell(row=next_row, column=status_col).value = "New"

        # Write "AJOUT_AUTO_GED" in SOURCE column if found (PATCH 6)
        if source_col:
            ws.cell(row=next_row, column=source_col).value = "AJOUT_AUTO_GED"

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
    """Write evidence_export.csv via temp file to avoid FUSE write issues."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     encoding="utf-8", newline="") as tmp:
        tmp_path = tmp.name
        writer = csv.DictWriter(tmp, fieldnames=[
            "sheet_name", "gf_row", "column_name", "old_value", "new_value",
            "source_type", "source_file", "source_row", "update_reason"
        ])
        writer.writeheader()
        for ev in evidence:
            writer.writerow(ev.to_dict())
    shutil.copy2(tmp_path, str(path))
    os.remove(tmp_path)
    logger.info("Evidence export written: %s (%d rows)", path, len(evidence))


def export_match_summary_csv(summary_rows: list[dict], path: Path) -> None:
    """Write match_summary.csv via temp file to avoid FUSE write issues."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     encoding="utf-8", newline="") as tmp:
        tmp_path = tmp.name
        writer = csv.DictWriter(tmp, fieldnames=["match_level", "count", "percentage"])
        writer.writeheader()
        writer.writerows(summary_rows)
    shutil.copy2(tmp_path, str(path))
    os.remove(tmp_path)
    logger.info("Match summary written: %s", path)
