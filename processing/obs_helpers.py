"""
obs_helpers.py — Shared OBSERVATIONS cell helpers
JANSA GrandFichier Updater

Extracted from grandfichier_writer.py so that bet_backfill.py can import
these utilities without creating circular imports.

Both grandfichier_writer.py and bet_backfill.py should import from here.
"""

import re

# ---------------------------------------------------------------------------
# Empty-comment detection
# ---------------------------------------------------------------------------

# Phrases that indicate "no real comment — placeholder only".
# startswith matching: if the comment begins with one of these, it's a placeholder.
_EMPTY_COMMENT_PREFIX_PATTERNS = [
    "voir documents joints",
    "voir document joint",
    "voir doc joint",
    "voir pièce jointe",
    "voir pièces jointes",
    "voir pièces joints",
    "voir pj",
    "voir visa",
    "voir note",
    "voir annotation",
    "voir fichier joint",
    "voir fichier",
    "sans observation",
    "pas d'observation",
    "pas de remarque",
    "aucune observation",
    "aucune remarque",
    "non concerné",
    "hors mission",
    "document non visé",
    "non visé",
]

# Exact-match-only patterns: the ENTIRE comment (stripped, lowercased) must equal one of these.
_EMPTY_COMMENT_EXACT_PATTERNS = {
    "-", ".", "..", "...", "ok", "ok.", "n/a", "na", "/",
    "ras", "r.a.s", "r.a.s.", "rsa", "r a s",
    "néant", "neant",
    "none", "rien",
    "sans obs", "sans obs.",
    "x",
}


def _is_empty_comment(comment: str) -> bool:
    """
    Return True if the comment has no real content (placeholder or empty).

    Strategy:
    1. Empty or whitespace-only → empty
    2. Very short (< 3 chars after stripping) → empty
    3. Exact match against short placeholder tokens → empty
    4. Starts with a known placeholder phrase → empty
    5. Everything else → real comment
    """
    if not comment:
        return True
    clean = comment.strip().lower()
    if len(clean) < 3:
        return True
    if clean in _EMPTY_COMMENT_EXACT_PATTERNS:
        return True
    if any(clean.startswith(p) for p in _EMPTY_COMMENT_PREFIX_PATTERNS):
        return True
    return False


# ---------------------------------------------------------------------------
# Group normalization map
# ---------------------------------------------------------------------------

_OBS_GROUP_NORMALIZE_MAP: dict[str, str] = {
    # ── MOEX ──
    'GEMO': 'MOEX',
    'MOEX': 'MOEX',
    'MOEX GEMO': 'MOEX',
    'VISA GEMO': 'MOEX',
    'GEMO / MOA': 'MOEX',
    'GEMO/MOA': 'MOEX',

    # ── MOEX SAS ──
    'SAS': 'MOEX SAS',
    'GEMO SAS': 'MOEX SAS',
    'GEMO:SAS': 'MOEX SAS',
    'GEMO: SAS': 'MOEX SAS',
    'GEMO-SAS': 'MOEX SAS',
    'GEMO  SAS': 'MOEX SAS',
    'GEMO :  SAS': 'MOEX SAS',
    'GEMO : SAS': 'MOEX SAS',
    'GEMO: SASA': 'MOEX SAS',
    'GEMO SAS REF': 'MOEX SAS',
    'GEMO SA': 'MOEX SAS',
    'GEMO: MOX': 'MOEX SAS',

    # ── ARCHITECTE ──
    'MOX': 'ARCHITECTE',
    'ARCHI': 'ARCHITECTE',
    'ARCHI MOX': 'ARCHITECTE',
    'ARCHITECTE': 'ARCHITECTE',
    'ARCHITECTE MOX': 'ARCHITECTE',
    'ARCHITECTES': 'ARCHITECTE',
    'MOX ARCHI': 'ARCHITECTE',
    'ARCHIMOX': 'ARCHITECTE',
    'ARCHBI MOX': 'ARCHITECTE',
    'ARCHII': 'ARCHITECTE',
    'ARCHII MOX': 'ARCHITECTE',
    'ARCHIO': 'ARCHITECTE',
    'ARCHIA': 'ARCHITECTE',
    'ARCI': 'ARCHITECTE',
    'ARCI MOX': 'ARCHITECTE',
    'ACHI': 'ARCHITECTE',
    'ARCH MOX': 'ARCHITECTE',
    'ARECHI MOX': 'ARCHITECTE',
    'ARHITECTE': 'ARCHITECTE',
    'ARCHITEECTE': 'ARCHITECTE',
    'ARCHITCTES': 'ARCHITECTE',
    'B-ARCHITECTE': 'ARCHITECTE',
    'B- MOX': 'ARCHITECTE',
    'ARCHIO MOX': 'ARCHITECTE',
    'ARCHBI': 'ARCHITECTE',
    'ARCHI /': 'ARCHITECTE',
    'ARCH /': 'ARCHITECTE',

    # ── BET Structure ──
    'BET STR': 'BET Structure',
    'BET STR-TERRELL': 'BET Structure',
    'BET STR TERRELL': 'BET Structure',
    'STR-TERRELL': 'BET Structure',
    'TERRELL': 'BET Structure',
    'TERREL': 'BET Structure',
    'TERELLE': 'BET Structure',
    'TERELL': 'BET Structure',
    'BET TERRELL': 'BET Structure',
    'BET STRUCTURE': 'BET Structure',
    'BET STRUCTURE TERRELL': 'BET Structure',
    'BET TRL': 'BET Structure',
    'BET TER': 'BET Structure',
    'BET SRT': 'BET Structure',
    'BET STRUCRURE': 'BET Structure',
    'BET STRE TERRELL': 'BET Structure',
    'BET TSR TERRELL': 'BET Structure',
    'BET STR TERRRELLL': 'BET Structure',
    'BETSTR': 'BET Structure',
    'BUT STRUCTURE': 'BET Structure',
    'TERRELL STR': 'BET Structure',
    'TERREL STR': 'BET Structure',
    'TERRELL:STR': 'BET Structure',
    'B-BET STRUCTURE TERRELL': 'BET Structure',
    'BET STR TERRELL /': 'BET Structure',

    # ── Bureau de contrôle ──
    'SOCOTEC': 'Bureau de control',
    'BC SOCOTEC': 'Bureau de control',
    'BC': 'Bureau de control',
    'CT SOCOTEC': 'Bureau de control',
    'BC SOSCOTEC': 'Bureau de control',
    'BUREAU DE CONTRÔLE': 'Bureau de control',
    'BUREAU DE CONTROLE': 'Bureau de control',
    'BET CONTROLE': 'Bureau de control',

    # ── AMO HQE ──
    'AMO HQE': 'AMO HQE',
    'AMO HQE LE SOMMER': 'AMO HQE',
    'LE SOMMER': 'AMO HQE',
    'HQE': 'AMO HQE',
    'AMO': 'AMO HQE',
    'AMO ENV LE SOMMER': 'AMO HQE',
    'AMO ENV LESOMMER': 'AMO HQE',
    'B-AMO HQE': 'AMO HQE',
    '-AMO HQE': 'AMO HQE',
    'AMO: HQE': 'AMO HQE',

    # ── BET Géotech ──
    'GEOLIA': 'BET Géotech',
    'BET GEOLIA': 'BET Géotech',
    'BET GEOLIA - G4': 'BET Géotech',
    'BET GEOTECH GEOLIA': 'BET Géotech',
    'G4': 'BET Géotech',

    # ── BET ACOUSTIQUE ──
    'ACOUSTICIEN': 'BET ACOUSTIQUE',
    'ACOUSTICIEN AVLS': 'BET ACOUSTIQUE',
    'AVLS': 'BET ACOUSTIQUE',
    'BET AVLS': 'BET ACOUSTIQUE',
    'BET ACOUSTIQUE': 'BET ACOUSTIQUE',
    'BET ACOUS AVLS': 'BET ACOUSTIQUE',
}


def _normalize_obs_group(raw_name: str) -> str:
    """
    Normalize a group name found in OBSERVATIONS to match the unified
    group names from mission_map.json.

    Strategy:
    1. Exact match against comprehensive lookup table (case-insensitive)
    2. Partial / contains fallback for remaining variants
    """
    name = raw_name.strip().upper()

    if name in _OBS_GROUP_NORMALIZE_MAP:
        return _OBS_GROUP_NORMALIZE_MAP[name]

    for key in sorted(_OBS_GROUP_NORMALIZE_MAP.keys(), key=len, reverse=True):
        if key in name or name in key:
            return _OBS_GROUP_NORMALIZE_MAP[key]

    return name


def _detect_existing_obs_groups(observations_text: str) -> set:
    """
    Parse existing OBSERVATIONS text to find which consultant groups
    already have responses recorded.

    Detects patterns like:
    - "GEMO : VAO"
    - "BET STR:VSO"
    - "ARCHI : REF"
    - "ACOUSTICIEN : HM"

    Returns a set of normalized group names already present.
    """
    if not observations_text:
        return set()

    found = set()
    pattern = re.compile(
        r'([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s\-\.]{2,30}?)\s*:\s*'
        r'(VAO|VSO|REF|DEF|HM|FAV|SUS|SUSPENDU|EN.ATTENTE|FAVORABLE|DEFAVORABLE)',
        re.IGNORECASE
    )

    for match in pattern.finditer(observations_text.upper()):
        group_name = match.group(1).strip()
        normalized = _normalize_obs_group(group_name)
        if normalized:
            found.add(normalized)

    return found


def _build_obs_entry(group_display_name: str, status: str, comment: str) -> str:
    """
    Build a single OBSERVATIONS entry in the standard format.

    Format:
        GROUP_NAME : STATUS
        comment text (if not empty/placeholder)
    """
    header = f"{group_display_name} : {status}"

    if _is_empty_comment(comment):
        return header

    return f"{header}\n{comment}"


# Display name mapping: unified group → short display name for OBSERVATIONS
_GROUP_DISPLAY_NAMES = {
    'MOEX': 'GEMO',
    'ARCHITECTE': 'ARCHI MOX',
    'BET Structure': 'BET STR',
    'Bureau de control': 'SOCOTEC',
    'AMO HQE': 'AMO HQE',
    'BET Géotech': 'GEOLIA',
    'BET ACOUSTIQUE': 'ACOUSTICIEN',
    'BET POL': 'BET POLLUTION',
    'BET CVC': 'BET CVC',
    'BET Plomberie': 'BET PLOMB',
    'BET ELEC': 'BET EGIS',
    'BET Façade': 'BET FACADE',
    'BET Ascenseur': 'BET ASCENSEUR',
    'BET SPK': 'BET SPK',
    'BET EV': 'PAYSAGISTE MUGO',
}
