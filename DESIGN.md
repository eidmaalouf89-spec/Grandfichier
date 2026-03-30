# DESIGN — JANSA GrandFichier Updater V1

---

## A. Confirmed Folder Tree

```
GRANDFICHIER_UPDATER/
  data/
    actor_map.json          # copied + adapted from OLD (51 actors, extended)
    status_map.json         # copied + adapted from OLD
    mission_map.json        # NEW: GED mission ↔ GF approbateur mapping
    source_priority.json    # NEW: field-level source priority config
  input/
    ged/                    # place GED dump Excel here
    sas/                    # place SAS extract here (optional)
    reports/                # place PDF reports here (optional)
    grandfichier/           # place current GrandFichier here
  output/                   # updated GrandFichier + evidence + anomalies go here
  processing/
    __init__.py
    config.py               # column mappings, constants, paths, TAG_PRIORITY
    models.py               # CanonicalResponse, SourceEvidence, AnomalyRecord, DeliverableRecord
    dates.py                # date parsing and delay computation (adapted from OLD)
    statuses.py             # status normalization + tag priority logic (adapted from OLD)
    actors.py               # actor name normalization and map loading (adapted from OLD)
    canonical.py            # key building, key normalization utilities
    ged_ingest.py           # GED dump → list[CanonicalResponse]
    sas_ingest.py           # SAS extract → list[CanonicalResponse] (structured placeholder V1)
    pdf_ingest.py           # PDF reports → list[CanonicalResponse] (pattern-based placeholder V1)
    matcher.py              # 4-level cascading key matching engine
    merge_engine.py         # consolidate canonical records by deliverable, detect conflicts
    grandfichier_reader.py  # parse GrandFichier sheets into structured records
    grandfichier_writer.py  # apply updates to GrandFichier workbook, produce evidence + anomalies
    anomalies.py            # anomaly logging utilities
  tests/
    test_status_normalization.py
    test_key_matching.py
    test_ged_ingest.py
    test_merge_engine.py
    test_grandfichier_reader.py
  run_update_grandfichier.py  # single entrypoint
  requirements.txt
  README.md
  DESIGN.md (this file)
```

---

## B. Files Copied / Adapted from OLD Repo

| OLD file | Action | What was adapted |
|----------|--------|-----------------|
| `processing/config.py` | Adapted | Kept TAG_PRIORITY, MISSION_GROUPING, date constants. Removed cockpit/backlog constants. Added GrandFichier-specific column constants. |
| `processing/models.py` | Rebuilt | WorkflowRow replaced by CanonicalResponse/SourceEvidence/AnomalyRecord/DeliverableRecord. |
| `processing/dates.py` | Copied | Minimal adaptation — same parse_date/parse_delay logic. |
| `processing/actors.py` | Copied | Unchanged — load_actor_map, resolve_actor. |
| `processing/statuses.py` | Copied | Unchanged — load_status_map, resolve_status. |
| `data/actor_map.json` | Copied | 51 actors from OLD, no changes needed in V1. |
| `data/status_map.json` | Adapted | Verified codes vs GED "Réponse" values. Added "Annulé" → ANN. |

---

## C. Files Intentionally NOT Copied

| OLD file | Reason |
|----------|--------|
| `cockpit/` | UI product — out of scope |
| `cockpit_export.py` | Dashboard product — out of scope |
| `processing/grouper.py` | Old submittal grouping logic — different purpose |
| `processing/analyzer.py` | Old MOEX backlog analysis — different purpose |
| `processing/loader.py` | Old GED loader with SOCOTEC injection and exception lists — replaced by ged_ingest.py with cleaner architecture |
| `processing/normalizer.py` | Old orchestration wrapper — replaced by run_update_grandfichier.py pipeline |
| Old queue / backlog / arbitration orchestration | Different product |
| `data/exception_list.json` | Exception logic not in scope for V1 |
| `data/socotec_verdicts.json` | SOCOTEC injection logic not in scope for V1 |
| `data/socotec_registry.json` | Same |

---

## D. Canonical Data Model

### CanonicalResponse
One normalized response record from ANY source (GED / SAS / REPORT).

Fields:
- **Source traceability**: source_type, source_file, source_row_or_page
- **Document identification**: document_key, lot, type_doc, numero, indice, batiment, zone, niveau, emetteur
- **Response data**: mission, respondant, raw_status, normalized_status, response_date, deadline_date, days_delta, comment, attachments
- **Matching metadata**: confidence ("EXACT" | "FUZZY" | "PARTIAL" | "UNMATCHED"), match_strategy, parse_warnings

### SourceEvidence
Links an updated GrandFichier field to its source.

Fields: sheet_name, row_number, column_name, old_value, new_value, source_type, source_file, source_row_or_page, update_reason

### AnomalyRecord
Logs anything that couldn't be resolved.

Fields: anomaly_type (UNMATCHED_GED | UNMATCHED_GF | KEY_CONFLICT | STATUS_CONFLICT | PARSE_FAILURE | MISSING_FIELD), severity (ERROR | WARNING | INFO), source_type, source_file, source_row_or_page, document_key, description, raw_data

### DeliverableRecord
Consolidates all responses for one document across all sources.

Fields: document_key, lot, type_doc, numero, indice, titre, responses (list[CanonicalResponse]), conflicts (list[AnomalyRecord]), consolidated_status

---

## E. Key Matching Strategy — 4 Cascade Levels

**Level 1 — Full composite key**
Build: `AFFAIRE + PROJET + BATIMENT + PHASE + EMETTEUR + SPECIALITE + LOT + TYPE_DOC + ZONE + NIVEAU + NUMERO`
Compare against GrandFichier column A (DOCUMENT).
Confidence: EXACT

**Level 2 — LOT + TYPE_DOC + NUMERO**
Normalize LOT prefix (I003 ↔ G003, A031 ↔ 031, strip leading letters).
Confidence: FUZZY

**Level 3 — TYPE_DOC + NUMERO within same sheet**
Cross-lot fallback. Use only within the LOT sheet being processed.
Confidence: PARTIAL

**Level 4 — NUMERO-only with trailing-alpha stripping**
Strip trailing alpha chars, zero-pad to 6 digits, compare.
Confidence: PARTIAL

**Unmatched** → log AnomalyRecord(UNMATCHED_GED, WARNING).

Each CanonicalResponse records which level succeeded in `match_strategy`.

---

## F. Merge Strategy Summary

1. Group all CanonicalResponse records by (document_key, mission).
2. For each group, if multiple records exist from same source → take most recent (by response_date).
3. If records exist from multiple sources for same field → apply source_priority rules.
4. Conflicts (same field, different values, same priority) → log STATUS_CONFLICT anomaly, use first by priority order.
5. Build DeliverableRecord per document_key, compute consolidated_status (worst-case TAG_PRIORITY).

---

## G. GrandFichier Update Strategy Summary

For each GrandFichier row (document), for each approbateur 3-column group (DATE / N° / STATUT):

| Field | Rule |
|-------|------|
| STATUT | Overwrite with normalized tag (VSO/VAO/REF/DEF/HM/SUS) from highest-priority source |
| DATE | Overwrite if source has newer/non-empty value |
| N° | Update from GED col 32 (attachments) if available |
| VISA GLOBAL | Recompute from worst-case TAG_PRIORITY across all approbateurs for this row |
| OBSERVATIONS | Append new comments; do NOT overwrite existing |

ANCIEN rows: update normally but flag in evidence export.
New documents (GED but not in GF): do NOT auto-insert, log as UNMATCHED_GED WARNING.

---

## H. Approbateur Mapping Strategy

GED mission names (e.g. `"0-BET Structure"`) must be mapped to GrandFichier Row 8 display names
(e.g. `"BET STR-TERRELL"`).

Mapping stored in `data/mission_map.json`.
Populated by:
1. Starting from OLD repo's actor_map.json + MISSION_GROUPING
2. Extending with known GF approbateur names per sheet inspection
3. Any unmapped mission → AnomalyRecord(UNMATCHED_MISSION, WARNING)

The mapping is bidirectional: GED mission → list of possible GF display names (one per sheet variant).

---

## I. Explicit Assumptions Requiring Future Confirmation

1. **LOT prefix normalization**: The mapping `I003 → G003`, `A031 → 031` is inferred from the
   DOCUMENT key example `P17T2GEEXELGDGOEG003NDCTZTN028000`. Confirm with actual GrandFichier data.

2. **GrandFichier sheet layout variants**: Two variants (A: with Zone, B: without Zone) are
   documented. There may be additional edge-case variants requiring manual inspection.

3. **SAS file format**: SAS source not yet available. `sas_ingest.py` is a placeholder with
   documented expected schema. Format must be confirmed when SAS extract is provided.

4. **PDF report format**: `pdf_ingest.py` uses pattern matching. Actual PDF structure must be
   validated against real report files to tune the regex patterns.

5. **Approbateur name variants**: GrandFichier Row 8 names vary per sheet and may have typos or
   abbreviation variants. `mission_map.json` must be validated against actual GrandFichier file.

6. **VISA GLOBAL computation**: TAG_PRIORITY order `DEF > REF > SUS > VAO > VSO > HM > ANN` is
   from OLD repo. Confirm this ordering is correct for GrandFichier use case.

7. **N° BDX column update**: The rule for updating N°BDX from GED col 32 (attachments) needs
   confirmation. It may be a manual entry field not intended for automated update.
