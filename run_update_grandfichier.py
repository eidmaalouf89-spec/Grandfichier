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
    parser.add_argument("--loglevel",      default="INFO", help="Logging level (default: INFO)")
    args = parser.parse_args()

    _setup_logging(args.loglevel)
    logger = logging.getLogger("run_update_grandfichier")

    # ---- Validate inputs ----
    ged_path = _check_file(args.ged, "GED dump")
    gf_path  = _check_file(args.grandfichier, "GrandFichier")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    sas_path     = Path(args.sas)     if args.sas     else None
    reports_path = Path(args.reports) if args.reports else None

    logger.info("=" * 60)
    logger.info("JANSA GrandFichier Updater V1")
    logger.info("  GED:           %s", ged_path)
    logger.info("  GrandFichier:  %s", gf_path)
    logger.info("  Output:        %s", output_dir)
    logger.info("  SAS:           %s", sas_path or "(not provided)")
    logger.info("  Reports:       %s", reports_path or "(not provided)")
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
    from processing.matcher import GFIndex, MatchSummary, match_all
    from processing.merge_engine import build_deliverables, load_source_priority
    from processing.grandfichier_writer import (
        apply_updates, export_evidence_csv, export_match_summary_csv
    )

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
    logger.info("  GED records: %d ingested", len(ged_records))

    # ---- Step b: Ingest SAS (optional) ----
    sas_records = []
    if sas_path:
        logger.info("Step 2/7: Ingesting SAS extract...")
        sas_records, _ = ingest_sas(sas_path, status_map)
        logger.info("  SAS records: %d ingested", len(sas_records))
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

    # ---- Step d: Read GrandFichier ----
    logger.info("Step 4/7: Reading GrandFichier...")
    gf_rows, sheet_meta = read_grandfichier(gf_path)
    logger.info("  GrandFichier rows: %d across %d sheets", len(gf_rows), len(sheet_meta))

    # Build lookup dict for writer
    gf_rows_by_sheet_row = {(r.sheet_name, r.row_number): r for r in gf_rows}

    # ---- Step e: Match canonical responses to GrandFichier rows ----
    logger.info("Step 5/7: Matching records to GrandFichier rows...")
    all_canonical = ged_records + sas_records + pdf_records
    gf_index = GFIndex(gf_rows)
    match_summary = MatchSummary()

    matched_records, unmatched_records = match_all(all_canonical, gf_index, match_summary)

    # Log unmatched as anomalies
    for cr in unmatched_records:
        anomaly_log.log_unmatched_ged(
            source_file=cr.source_file,
            source_row=cr.source_row_or_page,
            document_key=cr.document_key,
            raw_data={"lot": cr.lot, "type_doc": cr.type_doc,
                      "numero": cr.numero, "indice": cr.indice,
                      "mission": cr.mission},
        )

    match_summary.log_summary()

    # ---- Step f: Consolidate into DeliverableRecords ----
    logger.info("Step 6/7: Consolidating records by deliverable...")
    deliverables = build_deliverables(
        matched_records, gf_rows_by_sheet_row, source_priority, anomaly_log
    )
    logger.info("  Deliverable records: %d", len(deliverables))

    # ---- Step g: Apply updates to GrandFichier ----
    logger.info("Step 7/7: Applying updates to GrandFichier...")
    output_gf_path       = output_dir / "updated_grandfichier.xlsx"
    output_evidence_path = output_dir / "evidence_export.csv"
    output_anomaly_path  = output_dir / "anomaly_log.json"
    output_match_path    = output_dir / "match_summary.csv"

    evidence_records, fields_updated = apply_updates(
        source_grandfichier=gf_path,
        deliverables=deliverables,
        gf_rows_by_sheet_row=gf_rows_by_sheet_row,
        mission_map=mission_map,
        source_priority=source_priority,
        anomaly_logger=anomaly_log,
        output_path=output_gf_path,
    )

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
    print(f"  GED records ingested:         {len(ged_records):>6}")
    print(f"  SAS records ingested:         {len(sas_records):>6}")
    print(f"  PDF records ingested:         {len(pdf_records):>6}")
    print(f"  GrandFichier rows indexed:    {len(gf_rows):>6}")
    print(f"  Records matched:              {match_summary.total_matched:>6}")
    print(f"  Records unmatched:            {match_summary.total_unmatched:>6}")
    print(f"  Fields updated:               {fields_updated:>6}")
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
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
