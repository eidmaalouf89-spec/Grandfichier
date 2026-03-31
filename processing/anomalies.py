"""
JANSA GrandFichier Updater — Anomaly logging utilities (V1)
"""
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from processing.models import AnomalyRecord

logger = logging.getLogger(__name__)


class AnomalyLogger:
    """Collects AnomalyRecords during pipeline execution and exports them."""

    def __init__(self):
        self._records: list[AnomalyRecord] = []

    def add(self, record: AnomalyRecord) -> None:
        self._records.append(record)
        lvl = logging.WARNING if record.severity == "WARNING" else (
              logging.ERROR   if record.severity == "ERROR"   else logging.INFO)
        logger.log(lvl, "[%s/%s] %s — %s",
                   record.anomaly_type, record.severity,
                   record.document_key, record.description)

    def log_not_moex_responsibility(
        self, source_file: str, source_row: str,
        document_key: str, numero: str, indice: str, mission: str
    ) -> None:
        """
        Document has no MOEX mission — not processed by this pipeline.
        Logged as NOT_MOEX_RESPONSIBILITY / DEBUG (normal and expected for BET-only docs).
        """
        self.add(AnomalyRecord(
            anomaly_type="NOT_MOEX_RESPONSIBILITY",
            severity="DEBUG",
            source_type="GED",
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=(
                f"Document ({numero}/{indice}) has no MOEX mission — "
                f"skipped (mission: '{mission}')"
            ),
            raw_data={"numero": numero, "indice": indice, "mission": mission},
        ))

    def log_unmatched_ged(
        self, source_file: str, source_row: str,
        document_key: str, raw_data: dict
    ) -> None:
        """
        Classify unmatched GED records:
        - Empty key → EMPTY_KEY / DEBUG (data quality issue in GED)
        - Valid key, no GF match → NEW_DOCUMENT / INFO (expected: new submittal not yet in GF)
        """
        if not document_key or not document_key.strip():
            self.add(AnomalyRecord(
                anomaly_type="EMPTY_KEY",
                severity="DEBUG",
                source_type="GED",
                source_file=source_file,
                source_row_or_page=source_row,
                document_key="",
                description="GED row has no document key fields populated",
                raw_data=raw_data,
            ))
        else:
            self.add(AnomalyRecord(
                anomaly_type="NEW_DOCUMENT",
                severity="INFO",
                source_type="GED",
                source_file=source_file,
                source_row_or_page=source_row,
                document_key=document_key,
                description="New submittal — no corresponding row in GrandFichier",
                raw_data=raw_data,
            ))

    def log_unmatched_mission(
        self, source_file: str, source_row: str,
        document_key: str, mission_name: str
    ) -> None:
        self.add(AnomalyRecord(
            anomaly_type="UNMATCHED_MISSION",
            severity="WARNING",
            source_type="GED",
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=f"GED mission '{mission_name}' not mapped to any GrandFichier approbateur",
            raw_data={"mission": mission_name},
        ))

    def log_no_gf_column(
        self, source_file: str, source_row: str,
        document_key: str, mission_name: str, group_name: str,
        status: str, response_date: str, comment: str
    ) -> None:
        """
        GED mission group has no approbateur column in the GrandFichier.
        Logged as INFO — not an error, just informational for groups like
        MOEX SAS, BET VRD, BIM MANAGER, CSPS, H51-ASC, etc.
        """
        self.add(AnomalyRecord(
            anomaly_type="NO_GF_COLUMN",
            severity="INFO",
            source_type="GED",
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=(
                f"Mission '{mission_name}' (group '{group_name}') has no "
                f"approbateur column in GrandFichier — response not written"
            ),
            raw_data={
                "mission": mission_name,
                "group": group_name,
                "status": status,
                "response_date": response_date,
                "comment": comment,
            },
        ))

    def log_sas_ref_disregard(
        self, source_file: str, source_row: str,
        document_key: str, numero: str, indice: str, sheet_name: str
    ) -> None:
        """
        GED record matches a SAS REF'd GF row — confirmed same document by
        indice / name / date proximity → disregarded, no update written.
        """
        self.add(AnomalyRecord(
            anomaly_type="SAS_REF_DISREGARD",
            severity="DEBUG",
            source_type="GED",
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=(
                f"Document ({numero}/{indice}) matches SAS REF'd GF row on sheet '{sheet_name}' "
                f"— same document confirmed, disregarded"
            ),
            raw_data={"numero": numero, "indice": indice, "sheet": sheet_name},
        ))

    def log_sas_ref_new_submittal(
        self, source_file: str, source_row: str,
        document_key: str, numero: str, indice: str, sheet_name: str
    ) -> None:
        """
        GED record shares NUMERO with a SAS REF'd GF row but is a genuinely
        different document → new row created.
        """
        self.add(AnomalyRecord(
            anomaly_type="SAS_REF_NEW_SUBMITTAL",
            severity="INFO",
            source_type="GED",
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=(
                f"Document ({numero}/{indice}) shares NUMERO with SAS REF'd row on "
                f"sheet '{sheet_name}' but is a different submittal → new row created"
            ),
            raw_data={"numero": numero, "indice": indice, "sheet": sheet_name},
        ))

    def log_status_conflict(
        self, source_type: str, source_file: str, source_row: str,
        document_key: str, field: str, value_a: str, source_a: str,
        value_b: str, source_b: str
    ) -> None:
        self.add(AnomalyRecord(
            anomaly_type="STATUS_CONFLICT",
            severity="WARNING",
            source_type=source_type,
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=(
                f"Conflicting '{field}' values: '{value_a}' (from {source_a}) vs "
                f"'{value_b}' (from {source_b}). Using {source_a} per priority config."
            ),
            raw_data={"field": field, "value_a": value_a, "source_a": source_a,
                      "value_b": value_b, "source_b": source_b},
        ))

    def log_parse_failure(
        self, source_type: str, source_file: str, source_row: str,
        document_key: str, description: str, raw_data: dict = None
    ) -> None:
        self.add(AnomalyRecord(
            anomaly_type="PARSE_FAILURE",
            severity="WARNING",
            source_type=source_type,
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=description,
            raw_data=raw_data or {},
        ))

    def log_missing_field(
        self, source_type: str, source_file: str, source_row: str,
        document_key: str, field: str
    ) -> None:
        self.add(AnomalyRecord(
            anomaly_type="MISSING_FIELD",
            severity="INFO",
            source_type=source_type,
            source_file=source_file,
            source_row_or_page=source_row,
            document_key=document_key,
            description=f"Field '{field}' is absent or empty",
            raw_data={"field": field},
        ))

    @property
    def records(self) -> list[AnomalyRecord]:
        return list(self._records)

    def counts_by_type(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in self._records:
            counts[r.anomaly_type] = counts.get(r.anomaly_type, 0) + 1
        return counts

    def export_json(self, path: Path) -> None:
        """Write anomaly log JSON via temp file + shutil.copy2."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = [r.to_dict() for r in self._records]
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", encoding="utf-8", delete=False
        ) as tmp:
            tmp_path = tmp.name
            json.dump(data, tmp, ensure_ascii=False, indent=2)
        shutil.copy2(tmp_path, str(path))
        os.remove(tmp_path)
        logger.info("Anomaly log written: %s (%d records)", path, len(self._records))
