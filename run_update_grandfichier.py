#!/usr/bin/env python3
"""
JANSA GrandFichier Updater — Main entrypoint (V1)

Usage:
    python run_update_grandfichier.py \\
        --ged input/ged/ged_dump.xlsx \\
        --grandfichier input/grandfichier/GrandFichier.xlsx \\
        --output output/

    Optional:
        --sas input/sas/sas_extract.xlsx
        --reports input/reports/
        --loglevel DEBUG|INFO|WARNING
"""
import argparse
import logging
import sys
from pathlib import Path


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _check_file(path: str, label: str) -> Path:
    p = Path(path)
    if not p.exists():
        print(f"ERROR: {label} file not found: {p}", file=sys.stderr)
        sys.exit(1)
    return p


def _check_dir(path: str, label: str) -> Path:
    p = Path(path)
    if not p.is_dir():
        print(f"ERROR: {label} folder not found: {p}", file=sys.stderr)
        sys.exit(1)
    return p


def main():
    parser = argparse.ArgumentParser(
        description="JANSA GrandFichier Updater V1 — batch consolidation engine"
    )
    parser.add_argument("--ged",           required=True,  help="Path to GED dump Excel file")
    parser.add_argument("--grandfichier",  required=True,  help="Path to GrandFichier Excel file")
    parser.add_argument("--output",        required=True,  help="Output directory")
    parser.add_argument("--sas",           required=False, help="Path to SAS extract file (optional)")
    parser.add_argument("--reports",       required=False, help="Path to PDF reports folder (optional)")
    parser.add_argument(
        "--bet-reports",
        required=False,
        help=(
            "Path to the BET reports root folder. "
            "Must contain subfolders: 'AMO HQE', 'BET Acoustique AVLS', "
            "'BET Structure TERRELL', 'socotec'. "
            "If provided, Step 8 (BET backfill) will run after the GED pass."
        )
    )
    parser.add_argument("--loglevel",      default="INFO", help="Logging level (default: INFO)")
    args = parser.parse_args()

    _setup_logging(args.loglevel)
    logger = logging.getLogger("run_update_grandfichier")

    # ---- Validate inputs ----
    ged_path = _check_file(args.ged, "GED dump").resolve()
    gf_path  = _check_file(args.grandfichier, "GrandFichier").resolve()
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    sas_path     = Path(args.sas).resolve()     if args.sas     else None
    reports_path = Path(args.reports).resolve() if args.reports else None

    bet_reports_path = Path(args.bet_reports).resolve() if args.bet_reports else None
    if bet_reports_path and not bet_reports_path.is_dir():
        print(f"ERROR: --bet-reports folder not found: {bet_reports_path}", file=sys.stderr)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("JANSA GrandFichier Updater V1")
    logger.info("  GED:           %s", ged_path)
    logger.info("  GrandFichier:  %s", gf_path)
    logger.info("  Output:        %s", output_dir)
    logger.info("  SAS:           %s", sas_path or "(not provided)")
    logger.info("  Reports:       %s", reports_path or "(not provided)")
    logger.info("  BET Reports:   %s", bet_reports_path or "(not provided)")
    logger.info("=" * 60)

    # ---- Import processing modules ----
    # (imports here so errors surface after argument validation)
    from processing.config import (
        ACTOR_MAP_PATH, STATUS_MAP_PATH, MISSION_MAP_PATH, SOURCE_PRIORITY_PATH,
    )
    from processing.statuses import load_status_map
    from processing.actors import load_actor_map, load_mission_map
    from processing.anomalies import AnomalyLogger
    from processing.ged_ingest import ingest_ged
    from processing.sas_ingest import ingest_sas
    from processing.pdf_ingest import ingest_pdf_folder
    from processing.grandfichier_reader import read_grandfichier
    from processing.matcher import GEDNumeroIndex, MatchSummary, lookup_ged_for_gf
    from processing.merge_engine import build_deliverables, load_source_priority
    from processing.grandfichier_writer import (
        apply_updates, export_evidence_csv, export_match_summary_csv,
        export_orphan_ged, export_orphan_summary,
    )
    from processing.bet_backfill import backfill_bet_reports

    # ---- Load config maps ----
    logger.info("Loading config maps...")
    status_map      = load_status_map(STATUS_MAP_PATH)
    actor_map       = load_actor_map(ACTOR_MAP_PATH)
    mission_map     = load_mission_map(MISSION_MAP_PATH)
    source_priority = load_source_priority(SOURCE_PRIORITY_PATH)

    anomaly_log = AnomalyLogger()

    # ---- Step a: Ingest GED ----
    logger.info("Step 1/7: Ingesting GED dump...")
    ged_records, ged_skipped = ingest_ged(ged_path, status_map)
    ged_total_ingested = len(ged_records)
    logger.info("  GED records: %d ingested", ged_total_ingested)

    # ---- Step 1b: MOEX-only filter (PATCH 3.0) ----
    # Only process documents that have at least one MOEX mission response.
    # Documents reviewed only by BET/BC (no MOEX) are skipped entirely.
    from processing.actors import MOEX_MISSIONS
    logger.info("Step 1b/7: Applying MOEX-only filter...")
    moex_docs = set()
    for r in ged_records:
        if r.mission in MOEX_MISSIONS:
            moex_docs.add((r.numero, r.indice))

    ged_records_skipped = [r for r in ged_records if (r.numero, r.indice) not in moex_docs]
    ged_records = [r for r in ged_records if (r.numero, r.indice) in moex_docs]

    # Log skipped records as NOT_MOEX_RESPONSIBILITY
    _logged_not_moex = set()
    for r in ged_records_skipped:
        key = (r.numero, r.indice)
        if key not in _logged_not_moex:
            anomaly_log.log_not_moex_responsibility(
                source_file=r.source_file,
                source_row=r.source_row_or_page,
                document_key=r.document_key,
                numero=r.numero,
                indice=r.indice,
                mission=r.mission,
            )
            _logged_not_moex.add(key)

    logger.info(
        "  MOEX filter: %d records kept (%d unique docs), %d records skipped (%d unique docs)",
        len(ged_records), len(moex_docs),
        len(ged_records_skipped), len(_logged_not_moex),
    )

    # ---- Step b: SAS (disabled — manual process) ----
    sas_records = []
    if sas_path:
        logger.warning(
            "Step 2/7: SAS processing is manual — skipping. "
            "The --sas flag is accepted but SAS records are not ingested in V2."
        )
    else:
        logger.info("Step 2/7: SAS extract not provided — skipping")

    # ---- Step c: Ingest PDF reports (optional) ----
    pdf_records = []
    if reports_path:
        logger.info("Step 3/7: Ingesting PDF reports from %s...", reports_path)
        pdf_records, pdf_skipped = ingest_pdf_folder(reports_path, status_map)
        logger.info("  PDF records: %d ingested, %d skipped", len(pdf_records), len(pdf_skipped))
        for skip in pdf_skipped:
            anomaly_log.log_parse_failure(
                source_type="REPORT",
                source_file=skip.get("file", ""),
                source_row="",
                document_key="",
                description=skip.get("reason", "unknown"),
                raw_data=skip,
            )
    else:
        logger.info("Step 3/7: PDF reports folder not provided — skipping")

    # ---- Step d: Read GrandFichier (master) ----
    logger.info("Step 4/7: Reading GrandFichier...")
    gf_rows, sheet_meta = read_grandfichier(gf_path)
    logger.info("  GrandFichier rows: %d across %d sheets", len(gf_rows), len(sheet_meta))

    # Build lookup dict for writer
    gf_rows_by_sheet_row = {(r.sheet_name, r.row_number): r for r in gf_rows}

    from datetime import datetime as _dt
    _ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"run_{_ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    output_gf_path       = run_dir / "updated_grandfichier.xlsx"
    output_evidence_path = run_dir / "evidence_export.csv"
    output_anomaly_path  = run_dir / "anomaly_log.json"
    output_match_path    = run_dir / "match_summary.csv"
    orphan_path          = run_dir / "orphan_ged_documents.xlsx"
    orphan_summary_path  = run_dir / "orphan_summary.xlsx"

    # ---- Step 5/7: Index GED records by NUMERO ----
    logger.info("Step 5/7: Indexing GED records by NUMERO...")
    ged_index = GEDNumeroIndex(ged_records)

    # ---- Step 6/7: For each GF row, find matching GED responses ----
    logger.info("Step 6/7: Looking up GED responses for each GF row...")
    match_summary = MatchSummary()
    matched_gf_rows, unmatched_gf_rows, orphan_ged_docs = lookup_ged_for_gf(
        gf_rows=gf_rows,
        ged_index=ged_index,
        match_summary=match_summary,
        anomaly_logger=anomaly_log,
    )
    match_summary.log_summary()

    # ---- Step 7a/7: Build deliverables and apply GED updates ----
    logger.info("Step 7a/7: Applying GED updates to GrandFichier...")
    deliverables = build_deliverables(
        matched_gf_rows, gf_rows_by_sheet_row, source_priority, anomaly_log
    )
    evidence_records, fields_updated = apply_updates(
        source_grandfichier=gf_path,
        deliverables=deliverables,
        gf_rows_by_sheet_row=gf_rows_by_sheet_row,
        mission_map=mission_map,
        source_priority=source_priority,
        anomaly_logger=anomaly_log,
        output_path=output_gf_path,
        pdf_only=False,
    )

    # ---- Step 7b/7: Export orphan GED docs (in GED but NOT in GF) ----
    logger.info("Step 7b/7: Exporting orphan GED documents...")
    export_orphan_ged(orphan_ged_docs, ged_records, orphan_path)
    export_orphan_summary(orphan_ged_docs, ged_records, orphan_summary_path)

    # ---- PDF pass (if provided) — append OBSERVATIONS only ----
    if pdf_records:
        logger.info("Step PDF/7: Matching PDF records to GrandFichier rows...")
        pdf_ged_index = GEDNumeroIndex(pdf_records)
        matched_pdf, _, _ = lookup_ged_for_gf(
            gf_rows=gf_rows,
            ged_index=pdf_ged_index,
            match_summary=MatchSummary(),
            anomaly_logger=anomaly_log,
        )
        deliverables_pdf = build_deliverables(
            matched_pdf, gf_rows_by_sheet_row, source_priority, anomaly_log
        )
        logger.info("  PDF deliverable records: %d", len(deliverables_pdf))
        evidence_pdf, fields_pdf = apply_updates(
            source_grandfichier=output_gf_path,
            deliverables=deliverables_pdf,
            gf_rows_by_sheet_row=gf_rows_by_sheet_row,
            mission_map=mission_map,
            source_priority=source_priority,
            anomaly_logger=anomaly_log,
            output_path=output_gf_path,
            pdf_only=True,
        )
        evidence_records.extend(evidence_pdf)
        fields_updated += fields_pdf
    else:
        logger.info("Step PDF/7: No PDF records — skipping PDF pass")

    # ---- Step 7c/7: BET PDF ingest (if --bet-reports provided) ----
    # Parses BET PDF reports and writes them into the RAPPORT_* sheets
    # of the updated_grandfichier.xlsx produced by Step 7a.
    if bet_reports_path:
        logger.info("Step 7c/7: Ingesting BET PDF reports from %s...", bet_reports_path)
        from run_bet_ingest import run_bet_ingest_to_workbook
        bet_ingest_stats = run_bet_ingest_to_workbook(
            reports_root=bet_reports_path,
            gf_path=output_gf_path,
        )
        logger.info("Step 7c/7: BET ingest complete: %s", bet_ingest_stats)
    else:
        logger.info("Step 7c/7: --bet-reports not provided — skipping BET ingest")

    # ---- Step 8/7: BET Report Backfill ----
    # Back-fills BET consultant columns in LOT sheets from the RAPPORT_* sheets.
    # Prerequisites: Step 7a (GED pass) AND Step 7c (BET ingest) must have run.
    bet_evidence: list = []
    bet_fields_updated = 0
    _rapport_sheets: list = []

    if bet_reports_path:
        logger.info("Step 8/7: Running BET backfill...")

        # VERIFICATION 1: Check that RAPPORT_* sheets exist in the output workbook
        import openpyxl as _opxl
        _wb_check = _opxl.load_workbook(str(output_gf_path), read_only=True)
        _rapport_sheets = [s for s in _wb_check.sheetnames if s.startswith('RAPPORT_')]
        _wb_check.close()

        if not _rapport_sheets:
            logger.error(
                "Step 8: RAPPORT_* sheets not found in %s — "
                "BET ingest (Step 7c) may have failed. Skipping BET backfill.",
                output_gf_path
            )
        else:
            logger.info(
                "Step 8: Found RAPPORT_* sheets: %s — proceeding with backfill",
                _rapport_sheets
            )

            # VERIFICATION 2: Check that gf_rows were loaded (Temps 1 must have run)
            if not gf_rows:
                logger.error(
                    "Step 8: gf_rows is empty — GrandFichier was not read. "
                    "Skipping BET backfill."
                )
            else:
                try:
                    bet_evidence, bet_fields_updated = backfill_bet_reports(
                        gf_workbook_path=output_gf_path,
                        gf_rows=gf_rows,
                        anomaly_logger=anomaly_log,
                        output_path=output_gf_path,   # overwrite in place
                    )
                    evidence_records.extend(bet_evidence)
                    fields_updated += bet_fields_updated
                    logger.info(
                        "Step 8: BET backfill complete — %d fields updated", bet_fields_updated
                    )
                except Exception as _step8_exc:
                    import traceback as _tb
                    logger.error("Step 8: BET backfill CRASHED — %s", _step8_exc)
                    logger.error("Step 8 traceback:\n%s", _tb.format_exc())
                    print(f"ERROR Step 8 BET backfill crashed: {_step8_exc}", flush=True)
                    print(_tb.format_exc(), flush=True)
    else:
        logger.info("Step 8/7: --bet-reports not provided — skipping BET backfill")

    # ---- Write outputs ----
    export_evidence_csv(evidence_records, output_evidence_path)
    anomaly_log.export_json(output_anomaly_path)
    export_match_summary_csv(match_summary.to_rows(), output_match_path)

    # ---- Print summary ----
    anomaly_counts = anomaly_log.counts_by_type()
    total_anomalies = sum(anomaly_counts.values())

    print("\n" + "=" * 60)
    print("JANSA GrandFichier Updater V1 — RUN COMPLETE")
    print("=" * 60)
    print(f"  GED records ingested (raw):   {ged_total_ingested:>6}")
    print(f"  GED records (MOEX-filtered):  {len(ged_records):>6}")
    print(f"  GED records skipped (no MOEX):{len(ged_records_skipped):>6}")
    print(f"  SAS records ingested:         {len(sas_records):>6}")
    print(f"  PDF records ingested:         {len(pdf_records):>6}")
    print(f"  GrandFichier rows (master):   {len(gf_rows):>6}")
    print(f"  GF rows matched to GED:       {match_summary.total_matched:>6}")
    print(f"  GF rows with no GED data:     {match_summary.total_unmatched:>6}")
    print(f"  GF rows skipped (OLD sheet):  {match_summary._counts.get('GF_OLD_SHEET_SKIP', 0):>6}")
    print(f"  Orphan GED documents:         {len(orphan_ged_docs):>6}")
    print(f"  Fields updated:               {fields_updated:>6}")
    print(f"  BET fields updated (Step 8):  {bet_fields_updated:>6}")
    if bet_reports_path:
        print(f"  RAPPORT_* sheets in output:   {len(_rapport_sheets):>6}")
    print(f"  Total anomalies:              {total_anomalies:>6}")
    if anomaly_counts:
        for atype, count in sorted(anomaly_counts.items()):
            print(f"    {atype:<30} {count:>4}")
    print("=" * 60)
    print("Outputs:")
    print(f"  Updated GrandFichier: {output_gf_path}")
    print(f"  Evidence export:      {output_evidence_path}")
    print(f"  Anomaly log:          {output_anomaly_path}")
    print(f"  Match summary:        {output_match_path}")
    print(f"  Orphan GED docs:      {orphan_path}")
    print(f"  Orphan summary:       {orphan_summary_path}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
