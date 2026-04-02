"""
JANSA GrandFichier Updater — Configuration (V1)

Column mappings, constants, TAG_PRIORITY, path definitions.
Adapted from OLD JANSA VISASIST V1 config.py v1.3.
Cockpit/backlog/MOEX-dashboard logic removed.
GrandFichier-specific constants added.
"""
from pathlib import Path
from datetime import date

# ---------------------------------------------------------------------------
# Pipeline version — bump this with every release
# ---------------------------------------------------------------------------
PIPELINE_VERSION = "4.2.0"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT         = Path(__file__).resolve().parent.parent
DATA_DIR             = PROJECT_ROOT / "data"
INPUT_DIR            = PROJECT_ROOT / "input"
OUTPUT_DIR           = PROJECT_ROOT / "output"

ACTOR_MAP_PATH       = DATA_DIR / "actor_map.json"
STATUS_MAP_PATH      = DATA_DIR / "status_map.json"
MISSION_MAP_PATH     = DATA_DIR / "mission_map.json"
SOURCE_PRIORITY_PATH = DATA_DIR / "source_priority.json"

# ---------------------------------------------------------------------------
# GED Source sheet configuration
# ---------------------------------------------------------------------------
# Primary detailed sheet — FLAT layout: every row fully populated.
# "Vue détaillée des documents 1" has one extra col (Type de document, col 35)
# but same core schema. Use this sheet: all 13,048 rows are self-contained.
GED_SHEET_PRIMARY    = "Vue détaillée des documents 1"
# Fallback sheet — parent/child layout: metadata cols null on child rows.
# Only used if PRIMARY is absent.
GED_SHEET_VARIANT    = "Vue détaillée des documents"
# Secondary summary sheet (one row per document, missions as columns)
GED_SHEET_GLOBAL     = "Vue globale des documents"

# Row offsets
GED_HEADER_ROW       = 2   # 1-indexed; headers on row 2
GED_DATA_START_ROW   = 3   # data starts row 3

# ---------------------------------------------------------------------------
# GED Column indices (0-indexed) — "Vue détaillée des documents"
# ---------------------------------------------------------------------------
GED_COL = {
    "chemin":           0,
    "identifiant":      1,
    "affaire":          2,
    "projet":           3,
    "batiment":         4,
    "phase":            5,
    "emetteur":         6,
    "specialite":       7,
    "lot":              8,
    "type_doc":         9,
    "zone":             10,
    "niveau":           11,
    "numero":           12,
    "indice":           13,
    "libelle":          14,
    "description":      15,
    "format":           16,
    "version_cree_par": 17,
    "date_prev":        18,
    "date_depot":       19,
    "ecart_depot":      20,
    "version":          21,
    "derniere_modif":   22,
    "taille_mo":        23,
    "statut_final":     24,
    "mission":          25,   # REVIEWER identity
    "respondant":       26,
    "date_limite":      27,
    "reponse_le":       28,
    "ecart_reponse":    29,
    "reponse":          30,   # Response tag (raw French string)
    "commentaire":      31,
    "pieces_jointes":   32,
    "type_reponse":     33,
    "mission_associee": 34,
    # NOTE: col 19 (date_depot) is always empty in current AxeoBIM exports.
    # Col 22 (derniere_modif) holds the actual document deposit timestamp.
    # If AxeoBIM ever populates col 19, update this constant only.
    "date_depot_effective": 22,  # override: use derniere_modif as deposit date
}

# ---------------------------------------------------------------------------
# GrandFichier sheet configuration
# ---------------------------------------------------------------------------
GF_TITLE_ROWS         = 6    # rows 1-6 are title/legend
GF_HEADER_ROW         = 7    # main header row (1-indexed)
GF_APPROBATEUR_ROW    = 8    # approbateur names row
GF_SUBHEADER_ROW      = 9    # DATE / N° / STATUT sub-headers
GF_DATA_START_ROW     = 10   # data starts row 10

# GrandFichier Variant A column indices (0-indexed, sheets WITH Zone column)
GF_COL_VARIANT_A = {
    "document":    0,   # composite key = DOCUMENT
    "titre":       1,
    "date_diff":   2,
    "lot":         3,
    "type_doc":    4,
    "niv":         5,
    "zone":        6,
    "numero":      7,
    "indice":      8,
    "type_format": 9,
    "ancien":      10,
    "num_bdx":     11,
    "date_recept": 12,
    "non_recu":    13,
    "date_contrat":14,
    "visa_global": 15,
    "appro_start": 16,  # approbateur columns start here
}

# GrandFichier Variant B column indices (0-indexed, sheets WITHOUT Zone column)
GF_COL_VARIANT_B = {
    "document":    0,
    "titre":       1,
    "date_diff":   2,
    "lot":         3,
    "type_doc":    4,
    "numero":      5,
    "indice":      6,
    "niv":         7,
    "type_format": 8,
    "ancien":      9,
    "num_bdx":     10,
    "date_recept": 11,
    "non_recu":    12,
    "date_contrat":13,
    "visa_global": 14,
    "appro_start": 15,  # approbateur columns start here
}

# Approbateur group structure: each approbateur occupies exactly 3 columns
GF_APPRO_COL_DATE    = 0   # offset: DATE (response date)
GF_APPRO_COL_NUM     = 1   # offset: N° (reference number)
GF_APPRO_COL_STATUT  = 2   # offset: STATUT (response tag)
GF_APPRO_GROUP_SIZE  = 3   # columns per approbateur group

# Special column after all approbateur groups
GF_OBSERVATIONS_HEADER = "OBSERVATIONS"

# ---------------------------------------------------------------------------
# Date handling
# ---------------------------------------------------------------------------
DATE_FORMAT = "%d/%m/%Y"
TODAY       = date.today()

# ---------------------------------------------------------------------------
# TAG PRIORITY — strict deterministic ordering
# Lower number = worse / higher priority.
# Adapted from OLD repo config.py v1.3.
# V1 GrandFichier ordering: DEF > REF > SUS > VAO > VSO > HM > ANN
# ---------------------------------------------------------------------------
TAG_PRIORITY: dict[str, int] = {
    "DEF":        1,
    "REF":        2,
    "SUS":        3,
    "VAO":        4,
    "VSO":        5,
    "FAV":        6,
    "VAO_SAS":    7,
    "SS":         8,
    "HM":         9,
    "ANN":        10,
    "NONE":       11,
    "EN_ATTENTE": 12,
    "GEMO_NJ":    13,
}
_TAG_PRIORITY_MAX = max(TAG_PRIORITY.values()) + 1


def resolve_worst_tag(tag_codes: list[str]) -> str | None:
    """Return the highest-priority (worst) tag from a list of tag codes."""
    if not tag_codes:
        return None
    valid = [t for t in tag_codes if t and t != "NONE"]
    if not valid:
        return None
    return min(valid, key=lambda t: TAG_PRIORITY.get(t, _TAG_PRIORITY_MAX))


def has_blocking_tag(tag_codes: list[str]) -> bool:
    """Return True if any tag is REF or DEF (blocking)."""
    return any(t in ("REF", "DEF") for t in tag_codes)


# ---------------------------------------------------------------------------
# OLD sheet prefix (shared constant — used by matcher.py and bet_backfill.py)
# ---------------------------------------------------------------------------
OLD_SHEET_PREFIX = "OLD "

# ---------------------------------------------------------------------------
# BET Backfill — Consultant mapping
# ---------------------------------------------------------------------------

# Maps internal consultant key → list of possible Row 8 approbateur names in GF sheets
# These names are used to find the right approbateur column in each LOT sheet.
BET_CONSULTANT_APPRO_NAMES: dict[str, list[str]] = {
    'lesommer': ['AMO HQE LE SOMMER'],
    'avls':     ['ACOUSTICIEN AVLS', 'ACOUSTICIEN : AVLS'],
    'terrell':  ['BET STR-TERRELL', 'STR-TERRELL'],
    'socotec':  ['BC SOCOTEC', 'SOCOTEC'],
}

# Terrell is OBS-ONLY: GED already fills his STATUT/DATE/N° columns.
# The BET pass only appends OBSERVATIONS for Terrell — never writes columns.
BET_OBS_ONLY_CONSULTANTS: set[str] = {'terrell'}

# Maps consultant key → unified group name (used for _GROUP_DISPLAY_NAMES lookup)
BET_CONSULTANT_GROUP: dict[str, str] = {
    'lesommer': 'AMO HQE',
    'avls':     'BET ACOUSTIQUE',
    'terrell':  'BET Structure',
    'socotec':  'Bureau de contrôle',
}

# Maps consultant key → RAPPORT_* sheet name in the GrandFichier workbook
BET_CONSULTANT_SHEET: dict[str, str] = {
    'lesommer': 'RAPPORT_LE_SOMMER',
    'avls':     'RAPPORT_AVLS',
    'terrell':  'RAPPORT_TERRELL',
    'socotec':  'RAPPORT_SOCOTEC',
}

# Field names in RAPPORT_* sheets for each data type
# (column headers as written by bet_gf_writer.py)
BET_FIELD_STATUT      = 'STATUT_NORM'
BET_FIELD_DATE        = 'DATE_FICHE'    # avls/lesommer/socotec use DATE_FICHE; terrell uses DATE_RECEPT
BET_FIELD_DATE_ALT    = 'DATE_RECEPT'   # fallback for terrell
BET_FIELD_RAPPORT_ID  = 'RAPPORT_ID'
BET_FIELD_COMMENTAIRE = 'COMMENTAIRE'   # avls / lesommer / socotec
BET_FIELD_OBSERVATIONS = 'OBSERVATIONS' # terrell
BET_FIELD_NUMERO      = 'NUMERO'
BET_FIELD_INDICE      = 'INDICE'

# ---------------------------------------------------------------------------
# Defaults for actor resolution fallback
# ---------------------------------------------------------------------------
DEFAULT_ACTOR_FAMILY   = "unknown"
DEFAULT_ACTOR_RELEVANT = False

# ---------------------------------------------------------------------------
# Fuzzy indice-mismatch fallback — scoring thresholds
# ---------------------------------------------------------------------------

# Minimum total score for a fuzzy fallback match to be accepted.
# Breakdown: TYPE_DOC(3) + DATE(4) + TITRE(5) + EMETTEUR(3) + LOT(2) + INDICE_ADJ(1) = 18 max.
# 7.0 requires at minimum DATE_PROXIMITY + EMETTEUR or DATE_PROXIMITY + TITRE(weak).
FUZZY_MATCH_THRESHOLD: float = 7.0

# Date window (days) for fuzzy date scoring — wider than strict (30d)
# because re-submissions after SAS REF can happen weeks later.
FUZZY_DATE_WINDOW_DAYS: int = 90

# Maximum alphabetic indice distance scored for adjacence bonus.
# A→B=1, A→C=2, A→E=4, A→G=6. Beyond FUZZY_MAX_INDICE_DISTANCE → no bonus.
FUZZY_MAX_INDICE_DISTANCE: int = 5

# ---------------------------------------------------------------------------
# Matching confidence levels
# -----