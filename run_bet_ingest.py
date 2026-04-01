#!/usr/bin/env python3
"""
run_bet_ingest.py — BET PDF Report Ingestion Orchestration Script
JANSA VISASIST — P17&CO Tranche 2
Version 1.0 — April 2026

Usage:
    python run_bet_ingest.py \
        --gf        path/to/Grandfichier_2.xlsx \
        --lesommer  path/to/lesommer_reports/ \
        --avls      path/to/avls_reports/ \
        --terrell   path/to/terrell_reports/ \
        --socotec   path/to/socotec_reports/

All 4 folder arguments are optional — if omitted, that consultant is skipped.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from processing.lesommer_ingest import ingest_lesommer_folder
from processing.avls_ingest import ingest_avls_folder
from processing.terrell_ingest import ingest_terrell_folder
from processing.socotec_ingest import ingest_socotec_folder
from processing.bet_gf_writer import write_bet_reports_to_gf


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging() -> Path:
    """Configure console + file logging. Returns path to log file."""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'bet_ingest_{timestamp}.log'

    fmt = '%(asctime)s [%(levelname)s] %(name)s — %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_file), encoding='utf-8'),
        ]
    )
    return log_file


# ---------------------------------------------------------------------------
# Summary table printer
# ---------------------------------------------------------------------------

def print_summary(results: dict) -> None:
    """Print a formatted summary table to stdout."""
    COL = {
        'consultant': 16,
        'records':    9,
        'inserted':   10,
        'updated':    9,
        'noop':       7,
        'skipped':    15,
    }

    def row(*vals):
        cols = [COL['consultant'], COL['records'], COL['inserted'],
                COL['updated'], COL['noop'], COL['skipped']]
        return '  '.join(str(v).ljust(c) for v, c in zip(vals, cols))

    sep = '-' * (sum(COL.values()) + 2 * (len(COL) - 1))
    print()
    print(sep)
    print(row('Consultant', 'Records', 'Inserted', 'Updated', 'No-op', 'Skipped files'))
    print(sep)
    for consultant, data in results.items():
        print(row(
            consultant,
            data.get('record_count', 0),
            data.get('inserted', 0),
            data.get('updated', 0),
            data.get('noop', 0),
            data.get('skipped_count', 0),
        ))
    print(sep)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description='Ingest BET consultant PDF reports into the GrandFichier workbook.'
    )
    parser.add_argument('--gf',       required=True, help='Path to GrandFichier .xlsx file')
    parser.add_argument('--lesommer', default=None,  help='Folder with Le Sommer PDF reports')
    parser.add_argument('--avls',     default=None,  help='Folder with AVLS PDF reports')
    parser.add_argument('--terrell',  default=None,  help='Folder with TERRELL PDF reports')
    parser.add_argument('--socotec',  default=None,  help='Folder with SOCOTEC PDF reports')
    args = parser.parse_args()

    log_file = setup_logging()
    logger = logging.getLogger('run_bet_ingest')
    logger.info("=== BET Ingest started ===")
    logger.info("GrandFichier: %s", args.gf)
    logger.info("Log file: %s", log_file)

    gf_path = Path(args.gf)
    if not gf_path.exists():
        logger.error("GrandFichier not found: %s", gf_path)
        return 1

    # -----------------------------------------------------------------------
    # Ingest each consultant
    # -----------------------------------------------------------------------
    records_by_consultant: dict[str, list[dict]] = {}
    all_skipped: dict[str, list[dict]] = {}
    summary: dict[str, dict] = {}

    CONSULTANTS = [
        ('lesommer', args.lesommer, ingest_lesommer_folder, 'LE_SOMMER'),
        ('avls',     args.avls,     ingest_avls_folder,     'AVLS'),
        ('terrell',  args.terrell,  ingest_terrell_folder,  'TERRELL'),
        ('socotec',  args.socotec,  ingest_socotec_folder,  'SOCOTEC'),
    ]

    for key, folder, ingest_fn, label in CONSULTANTS:
        if not folder:
            logger.info("Skipping %s (no folder provided)", label)
            records_by_consultant[key] = []
            summary[label] = {'record_count': 0, 'skipped_count': 0,
                               'inserted': 0, 'updated': 0, 'noop': 0}
            continue

        folder_path = Path(folder)
        if not folder_path.exists():
            logger.warning("%s folder not found: %s — skipping", label, folder_path)
            records_by_consultant[key] = []
            summary[label] = {'record_count': 0, 'skipped_count': 0,
                               'inserted': 0, 'updated': 0, 'noop': 0}
            continue

        logger.info("--- Ingesting %s from %s ---", label, folder_path)
        records, skipped = ingest_fn(folder_path)
        records_by_consultant[key] = records
        all_skipped[key] = skipped

        logger.info(
            "%s: %d records extracted, %d files/pages skipped",
            label, len(records), len(skipped)
        )
        summary[label] = {
            'record_count': len(records),
            'skipped_count': len(skipped),
            'inserted': 0, 'updated': 0, 'noop': 0,
        }

    # -----------------------------------------------------------------------
    # Write to GrandFichier
    # -----------------------------------------------------------------------
    total_records = sum(len(v) for v in records_by_consultant.values())
    if total_records == 0:
        logger.warning("No records to write — nothing to do.")
        return 0

    logger.info("Writing %d total records to GrandFichier...", total_records)
    sheet_stats = write_bet_reports_to_gf(gf_path, records_by_consultant)

    # Merge sheet stats back into summary
    SHEET_TO_LABEL = {
        'RAPPORT_LE_SOMMER': 'LE_SOMMER',
        'RAPPORT_AVLS':      'AVLS',
        'RAPPORT_TERRELL':   'TERRELL',
        'RAPPORT_SOCOTEC':   'SOCOTEC',
    }
    for sheet_name, stats in sheet_stats.items():
        label = SHEET_TO_LABEL.get(sheet_name, sheet_name)
        if label in summary:
            summary[label].update(stats)

    # -----------------------------------------------------------------------
    # Print summary
    # -----------------------------------------------------------------------
    print_summary(summary)

    # -----------------------------------------------------------------------
    # Write skipped files JSON
    # -----------------------------------------------------------------------
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    date_str = datetime.now().strftime('%Y%m%d')
    skipped_path = log_dir / f'bet_ingest_skipped_{date_str}.json'
    with open(skipped_path, 'w', encoding='utf-8') as f:
        json.dump(all_skipped, f, ensure_ascii=False, indent=2)
    logger.info("Skipped files log: %s", skipped_path)

    logger.info("=== BET Ingest complete ===")
    return 0


if __name__ == '__main__':
    sys.exit(main())
