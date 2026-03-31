"""
JANSA GrandFichier Updater — Canonical Data Models (V1)

All core dataclasses. Replaces OLD WorkflowRow with purpose-built
CanonicalResponse / SourceEvidence / AnomalyRecord / DeliverableRecord.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# CanonicalResponse
# One normalized response record from ANY source (GED / SAS / REPORT).
# ---------------------------------------------------------------------------
@dataclass
class CanonicalResponse:
    # --- Source traceability ---
    source_type: str              # "GED" | "SAS" | "REPORT"
    source_file: str              # filename (basename only)
    source_row_or_page: str       # e.g. "row 1542" or "page 3"

    # --- Document identification ---
    document_key: str             # normalized composite key for matching (built by canonical.py)
    lot: str                      # LOT code (normalized, e.g. "G003")
    type_doc: str                 # TYPE_DOC 3-letter code (e.g. "NDC", "PLA")
    numero: str                   # NUMERO string (zero-padded 6 digits e.g. "028000")
    indice: str                   # revision letter (A, B, C…)
    batiment: str                 # building code (e.g. "GE", "B1")
    zone: str                     # zone code (e.g. "TZ", "B1")
    niveau: str                   # level code (e.g. "TX", "R0", "SS")
    emetteur: str                 # submitter code (e.g. "LGD", "AXI")

    # --- Response data ---
    mission: str                  # reviewer mission name (from GED col 25 or SAS/REPORT)
    respondant: str               # person name who responded
    raw_status: str               # original status text as found in source
    normalized_status: str        # normalized code: VSO / VAO / REF / DEF / HM / SUS / ANN / NONE
    response_date: str            # date of response (ISO format "YYYY-MM-DD" or "" if absent)
    deadline_date: str            # deadline date if available
    days_delta: Optional[int]     # days late (positive) or early (negative)
    comment: str                  # free-text observation
    attachments: str              # attachment filenames (semicolon-separated)
    libelle: str = ""             # document title/label (from GED col 14 — Libellé du fichier)
    date_depot: str = ""          # date document was deposited in GED (col 19 — date de réception)

    # --- Matching metadata (filled in by matcher.py) ---
    confidence: str = "UNMATCHED"  # "EXACT" | "FUZZY" | "PARTIAL" | "UNMATCHED"
    match_strategy: str = ""        # which matching level succeeded
    gf_sheet: str = ""              # GrandFichier sheet name matched to
    gf_row: int = 0                 # GrandFichier row number matched to (1-indexed)
    parse_warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SourceEvidence
# Links an updated GrandFichier field to its source (for audit trail).
# ---------------------------------------------------------------------------
@dataclass
class SourceEvidence:
    sheet_name: str           # GrandFichier sheet name
    row_number: int           # GrandFichier row number (1-indexed)
    column_name: str          # field that was updated (e.g. "STATUT", "DATE", "VISA GLOBAL")
    old_value: str            # previous value (empty string if was blank)
    new_value: str            # new value written
    source_type: str          # "GED" | "SAS" | "REPORT" | "COMPUTED"
    source_file: str          # source filename
    source_row_or_page: str   # source row/page reference
    update_reason: str        # human-readable reason (e.g. "GED response date newer than existing")

    def to_dict(self) -> dict:
        return {
            "sheet_name":        self.sheet_name,
            "gf_row":            self.row_number,
            "column_name":       self.column_name,
            "old_value":         self.old_value,
            "new_value":         self.new_value,
            "source_type":       self.source_type,
            "source_file":       self.source_file,
            "source_row":        self.source_row_or_page,
            "update_reason":     self.update_reason,
        }


# ---------------------------------------------------------------------------
# AnomalyRecord
# Logs anything that couldn't be resolved deterministically.
# ---------------------------------------------------------------------------
@dataclass
class AnomalyRecord:
    anomaly_type: str    # UNMATCHED_GED | UNMATCHED_GF | UNMATCHED_MISSION |
                         # KEY_CONFLICT | STATUS_CONFLICT | PARSE_FAILURE | MISSING_FIELD
    severity: str        # "ERROR" | "WARNING" | "INFO"
    source_type: str     # "GED" | "SAS" | "REPORT" | "GRANDFICHIER"
    source_file: str
    source_row_or_page: str
    document_key: str
    description: str
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "anomaly_type":      self.anomaly_type,
            "severity":          self.severity,
            "source_type":       self.source_type,
            "source_file":       self.source_file,
            "source_row_or_page":self.source_row_or_page,
            "document_key":      self.document_key,
            "description":       self.description,
            "raw_data":          self.raw_data,
        }


# ---------------------------------------------------------------------------
# DeliverableRecord
# Consolidates all responses for one document across all sources.
# ---------------------------------------------------------------------------
@dataclass
class DeliverableRecord:
    document_key: str
    lot: str
    type_doc: str
    numero: str
    indice: str
    titre: str
    gf_sheet: str
    gf_row: int
    responses: list[CanonicalResponse] = field(default_factory=list)
    conflicts: list[AnomalyRecord] = field(default_factory=list)
    consolidated_status: str = ""    # worst-case tag from TAG_PRIORITY


# ---------------------------------------------------------------------------
# GFRow
# Internal representation of one GrandFichier data row.
# Used by grandfichier_reader.py to pass structured data to the writer.
# ---------------------------------------------------------------------------
@dataclass
class GFRow:
    sheet_name: str
    row_number: int           # 1-indexed Excel row
    document_key: str         # col A DOCUMENT value
    titre: str
    lot: str
    type_doc: str
    numero: str
    indice: str
    niveau: str
    zone: str
    ancien: bool              # True if ANCIEN flag = 1
    visa_global: str          # current VISA GLOBAL value
    observations: str         # current OBSERVATIONS text
    approbateurs: list[GFApprobateur] = field(default_factory=list)


@dataclass
class GFApprobateur:
    name: str                 # display name from row 8 (e.g. "MOEX GEMO")
    col_date: int             # 0-indexed column of DATE field
    col_num: int              # 0-indexed column of N° field
    col_statut: int           # 0-indexed column of STATUT field
    current_date: str = ""
    current_num: str = ""
    current_statut: str = ""
