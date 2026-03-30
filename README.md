# JANSA GrandFichier Updater — V1

## Purpose

A deterministic batch tool that ingests multiple data sources (GED dump, SAS extract, PDF reports)
and updates the **GrandFichier** consolidation workbook automatically, with full traceability and
anomaly logging.

This is **not** a dashboard, not a backlog cockpit, and not a UI.
It is a clean, minimal, extensible consolidation engine.

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Place input files

| Folder | What to put there |
|--------|-------------------|
| `input/ged/` | AxeoBIM GED dump Excel export (single `.xlsx`) |
| `input/sas/` | SAS conformity check extract (`.xlsx` or `.csv`) — optional |
| `input/reports/` | PDF reviewer reports folder — optional |
| `input/grandfichier/` | Current GrandFichier workbook (single `.xlsx`) |

### 3. Run

```bash
python run_update_grandfichier.py \
  --ged input/ged/ged_dump.xlsx \
  --grandfichier input/grandfichier/GrandFichier.xlsx \
  --output output/

# With optional sources:
python run_update_grandfichier.py \
  --ged input/ged/ged_dump.xlsx \
  --sas input/sas/sas_extract.xlsx \
  --reports input/reports/ \
  --grandfichier input/grandfichier/GrandFichier.xlsx \
  --output output/
```

### 4. Check outputs

| File | Description |
|------|-------------|
| `output/updated_grandfichier.xlsx` | Updated GrandFichier workbook |
| `output/evidence_export.csv` | One row per updated field — full traceability |
| `output/anomaly_log.json` | All anomalies detected (unmatched, conflicts, parse errors) |
| `output/match_summary.csv` | Match counts per cascade level |

---

## Architecture Overview

```
GED dump (xlsx)     ──► ged_ingest.py    ─┐
SAS extract         ──► sas_ingest.py    ─┤──► canonical.py / matcher.py ──► merge_engine.py
PDF reports         ──► pdf_ingest.py    ─┘             │                          │
                                                         │                          ▼
GrandFichier (xlsx) ──► grandfichier_reader.py ─────────►  grandfichier_writer.py ──► outputs
```

All data is normalized to `CanonicalResponse` before matching.
Every update is recorded in `SourceEvidence`.
Every mismatch is logged in `AnomalyRecord`.

---

## Key Configuration Files

| File | Purpose |
|------|---------|
| `data/actor_map.json` | GED mission name → normalized actor entry |
| `data/status_map.json` | Raw status string → normalized code (VSO/VAO/REF/DEF/HM…) |
| `data/mission_map.json` | GED mission name → GrandFichier approbateur display name |
| `data/source_priority.json` | Field-level source priority (GED > SAS > REPORT or custom) |

---

## Design Principles

- **Deterministic**: same inputs → same outputs, always
- **Traceable**: every written value links back to its source row/file
- **Anomaly-first**: anything ambiguous is logged, never silently skipped
- **No AI in core logic**: PDF parsing uses pattern matching; no LLM calls
- **Config-driven**: business rules live in JSON, not hardcoded

---

## Running Tests

```bash
python -m pytest tests/ -v
```

---

## Extending V1

- Add new source types by implementing a new `*_ingest.py` that returns `list[CanonicalResponse]`
- Extend `data/mission_map.json` to map new GED missions to GrandFichier approbateurs
- Adjust `data/source_priority.json` to change which source wins per field
- All matching logic is in `processing/matcher.py` — the 4-level cascade is explicit and auditable
