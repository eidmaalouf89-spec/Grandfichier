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

    # Per-consultant deduplication key functions (applied in-memory before GF write)
    DEDUP_KEY_FN = {
        'lesommer': lambda r: f"{r.get('RAPPORT_ID','')}|{r.get('NUMERO','')}|{r.get('INDICE','')}|{r.get('SECTION','')}|{r.get('TABLE_TYPE','')}",
        'avls':     lambda r: f"{r.get('RAPPORT_ID','')}|{r.get('REF_DOC','')}|{r.get('LOT_LABEL','')}|{r.get('N_VISA','')}",
        'terrell':  lambda r: f"{r.get('RAPPORT_ID','')}|{r.get('NUMERO','')}|{r.get('INDICE','')}|{r.get('BAT','')}|{r.get('LOT','')}",
        'socotec':  lambda r: f"{r.get('RAPPORT_ID','')}|{r.get('NUMERO','')}|{r.get('STATUT_NORM','')}",
    }

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

        # In-memory deduplication by upsert key before writing to GrandFichier
        seen: set[str] = set()
        deduped: list[dict] = []
        for r in records:
            k = DEDUP_KEY_FN[key](r)
            if k not in seen:
                seen.add(k)
                deduped.append(r)
        if len(deduped) < len(records):
            logger.info(
                "%s: removed %d duplicates (%d → %d unique)",
                label, len(records) - len(deduped), len(records), len(deduped),
            )
        records = deduped

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



# ---------------------------------------------------------------------------
# Importable entry point — called by run_update_grandfichier.py Step 7c
# ---------------------------------------------------------------------------

def run_bet_ingest_to_workbook(
    reports_root: "Path | str",
    gf_path: "Path | str",
) -> dict:
    """
    Importable entry point: ingest all BET PDF reports from reports_root
    and write their RAPPORT_* sheets into the GrandFichier workbook at gf_path.

    Expected subfolder structure under reports_root:
        AMO HQE/                ← Le Sommer PDFs
        BET Acoustique AVLS/    ← AVLS PDFs
        BET Structure TERRELL/  ← Terrell PDFs
        socotec/                ← SOCOTEC PDFs

    Returns: dict with per-consultant ingest stats.
    """
    import logging as _logging
    from pathlib import Path as _Path

    _logger = _logging.getLogger('run_bet_ingest_to_workbook')
    reports_root = _Path(reports_root)
    gf_path = _Path(gf_path)

    # Define subfolder paths — these match the input/reports/ structure
    SUBFOLDER_MAP = {
        'lesommer': reports_root / 'AMO HQE',
        'avls':     reports_root / 'BET Acoustique AVLS',
        'terrell':  reports_root / 'BET Structure TERRELL',
        'socotec':  reports_root / 'socotec',
    }

    INGEST_FUNCS = {
        'lesommer': ingest_lesommer_folder,
        'avls':     ingest_avls_folder,
        'terrell':  ingest_terrell_folder,
        'socotec':  ingest_socotec_folder,
    }

    records_by_consultant: dict[str, list[dict]] = {}
    ingest_stats: dict[str, dict] = {}

    for consultant_key, folder_path in SUBFOLDER_MAP.items():
        if not folder_path.exists():
            _logger.warning(
                "[%s] Folder not found: %s — skipping", consultant_key, folder_path
            )
            records_by_consultant[consultant_key] = []
            ingest_stats[consultant_key] = {
                'records': 0, 'skipped': 0, 'status': 'FOLDER_MISSING'
            }
            continue

        pdf_files = list(folder_path.glob('*.pdf'))
        if not pdf_files:
            _logger.info(
                "[%s] No PDF files in %s — skipping", consultant_key, folder_path
            )
            records_by_consultant[consultant_key] = []
            ingest_stats[consultant_key] = {
                'records': 0, 'skipped': 0, 'status': 'NO_PDFS'
            }
            continue

        _logger.info(
            "[%s] Ingesting %d PDFs from %s...",
            consultant_key, len(pdf_files), folder_path
        )
        try:
            records, skipped = INGEST_FUNCS[consultant_key](folder_path)
            records_by_consultant[consultant_key] = records
            ingest_stats[consultant_key] = {
                'records': len(records),
                'skipped': len(skipped),
                'status': 'OK',
            }
            _logger.info(
                "[%s] Ingested %d records, %d skipped",
                consultant_key, len(records), len(skipped)
            )
        except Exception as e:
            _logger.error("[%s] Ingest failed: %s", consultant_key, e)
            records_by_consultant[consultant_key] = []
            ingest_stats[consultant_key] = {
                'records': 0, 'skipped': 0, 'status': f'ERROR: {e}'
            }

    # Write all RAPPORT_* sheets into the workbook
    _logger.info("Writing RAPPORT_* sheets into workbook: %s", gf_path)
    write_stats = write_bet_reports_to_gf(gf_path, records_by_consultant)
    ingest_stats['_write_stats'] = write_stats

    return ingest_stats