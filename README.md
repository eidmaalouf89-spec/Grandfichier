# JANSA GrandFichier Updater

**Projet :** P17&CO Tranche 2
**Rôle :** MOEX (Maître d'Œuvre d'Exécution) — Eid Maalouf
**Statut :** 🟢 Opérationnel — Pipeline GED→GF complet + BET backfill (Step 8) implémenté

---

## Qu'est-ce que c'est ?

Un outil batch déterministe qui met à jour automatiquement le **GrandFichier** (tableau Excel de suivi des VISAs consultants) à partir de deux sources de données :

1. **Le dump AxeoBIM GED** — export Excel de la plateforme GED contenant tous les avis des consultants sur les documents soumis
2. **Les rapports PDF des BET** — rapports individuels des 4 bureaux d'études techniques (Le Sommer, AVLS, Terrell, SOCOTEC)

Le GrandFichier est un classeur Excel multi-feuilles (une par LOT) avec une ligne par document soumis et une colonne groupe (DATE / N° / STATUT) par consultant approbateur.

**Ce n'est pas un dashboard. Ce n'est pas une interface de gestion.** C'est un moteur de consolidation propre, traçable et extensible.

---

## Architecture globale

### Pipeline en 2 temps séquentiels

```
╔══════════════════════════════════════════════════════════════════╗
║  TEMPS 1 — run_update_grandfichier.py                           ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  input/ged/dump.xlsx ──► ged_ingest.py                          ║
║       (AxeoBIM export)        │                                  ║
║                               ▼                                  ║
║  input/grandfichier/GF.xlsx  canonical.py / matcher.py          ║
║       (GrandFichier master) ──► merge_engine.py                  ║
║                               │                                  ║
║                               ▼                                  ║
║                        grandfichier_writer.py                    ║
║                               │                                  ║
║                               ▼                                  ║
║  output/run_TIMESTAMP/updated_grandfichier.xlsx  ◄── GED pass   ║
║                        + evidence_export.csv                     ║
║                        + anomaly_log.json                        ║
║                        + match_summary.csv                       ║
║                        + orphan_ged_documents.xlsx               ║
╚══════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════╗
║  TEMPS 2 — Step 8 dans run_update_grandfichier (--bet-reports)  ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  input/reports/AMO HQE/*.pdf          ──► lesommer_ingest.py    ║
║  input/reports/BET Acoustique AVLS/*.pdf ► avls_ingest.py       ║
║  input/reports/BET Structure TERRELL/*.pdf ► terrell_ingest.py  ║
║  input/reports/socotec/*.pdf          ──► socotec_ingest.py     ║
║                     │                                            ║
║                     ▼                                            ║
║              bet_gf_writer.py                                    ║
║        (écrit RAPPORT_LE_SOMMER, RAPPORT_AVLS,                  ║
║         RAPPORT_TERRELL, RAPPORT_SOCOTEC dans le GF)            ║
║                     │                                            ║
║                     ▼                                            ║
║              bet_backfill.py  ✅ Implémenté                      ║
║        (reporte les avis BET dans les colonnes                   ║
║         consultant des feuilles LOT — cellules vides only)      ║
║                     │                                            ║
║                     ▼                                            ║
║  output/run_TIMESTAMP/updated_grandfichier.xlsx  ◄── FINAL      ║
║        (avec feuilles RAPPORT_* comme historique interne)        ║
╚══════════════════════════════════════════════════════════════════╝
```

### Principe fondamental : GF-master

Le GrandFichier est toujours la **source de vérité pour les documents**. Le pipeline n'insère jamais de nouvelles lignes dans les feuilles LOT. Il met à jour uniquement les cellules existantes pour les documents déjà indexés dans le GF. Les documents GED sans correspondance dans le GF sont loggués comme orphelins.

---

## Structure du projet

```
GRANDFICHIER_UPDATER/
│
├── data/
│   ├── actor_map.json          # 51 acteurs GED → nom normalisé
│   ├── mission_map.json        # Mission GED → groupe → nom colonne GF + display OBS
│   ├── source_priority.json    # Priorité par champ (GED > SAS > REPORT)
│   └── status_map.json         # Statut brut GED → code normalisé (VSO/VAO/REF/DEF/HM…)
│
├── input/                      # Dossier des inputs (non versionné)
│   ├── ged/                    # Dump AxeoBIM Excel
│   ├── grandfichier/           # GrandFichier courant
│   ├── reports/
│   │   ├── AMO HQE/            # Rapports PDF Le Sommer
│   │   ├── BET Acoustique AVLS/# Rapports PDF AVLS
│   │   ├── BET Structure TERRELL/ # Fiches d'examen Terrell (FExx*.pdf)
│   │   └── socotec/            # Rapports PDF SOCOTEC
│   └── sas/                    # (désactivé — process manuel)
│
├── output/                     # Sorties timestampées (non versionné)
│   └── run_YYYYMMDD_HHMMSS/
│       ├── updated_grandfichier.xlsx    # GF mis à jour (contient les RAPPORT_* sheets)
│       ├── evidence_export.csv          # Traçabilité : 1 ligne par champ écrit
│       ├── anomaly_log.json             # Toutes les anomalies (UNMATCHED, CONFLICT…)
│       ├── match_summary.csv            # Statistiques de matching
│       ├── orphan_ged_documents.xlsx    # Docs GED sans correspondance GF
│       └── orphan_summary.xlsx          # Résumé des orphelins
│
├── processing/                 # Modules Python du pipeline
│   ├── actors.py               # Résolution noms acteurs
│   ├── anomalies.py            # Logger d'anomalies
│   ├── avls_ingest.py          # Parser PDF AVLS (BET Acoustique)
│   ├── bet_backfill.py         # Step 8 : backfill avis BET → colonnes LOT
│   ├── bet_gf_writer.py        # Écrit les feuilles RAPPORT_* dans le GF
│   ├── canonical.py            # Normalisation clés et numéros
│   ├── config.py               # Constantes, mappings de colonnes, TAG_PRIORITY
│   ├── dates.py                # Parsing et calcul de délais
│   ├── ged_ingest.py           # Lecture dump GED → CanonicalResponse
│   ├── grandfichier_reader.py  # Lecture GF → GFRow (master)
│   ├── grandfichier_writer.py  # Écriture GF depuis DeliverableRecord
│   ├── lesommer_ingest.py      # Parser PDF Le Sommer (AMO HQE)
│   ├── matcher.py              # Moteur de matching GF-master par NUMERO+INDICE
│   ├── merge_engine.py         # Consolidation CanonicalResponse → DeliverableRecord
│   ├── models.py               # Dataclasses : CanonicalResponse, GFRow, SourceEvidence…
│   ├── obs_helpers.py          # Helpers OBSERVATIONS partagés (dédup, format, display)
│   ├── pdf_ingest.py           # Parser PDF générique (placeholder V1)
│   ├── sas_ingest.py           # Parser SAS (désactivé — process manuel)
│   ├── socotec_ingest.py       # Parser PDF SOCOTEC (Bureau de Contrôle)
│   ├── statuses.py             # Normalisation des statuts
│   └── terrell_ingest.py       # Parser PDF Terrell (BET Structure)
│
├── tests/                      # Tests pytest
│   ├── test_avls_ingest.py
│   ├── test_ged_ingest.py
│   ├── test_grandfichier_reader.py
│   ├── test_key_matching.py
│   ├── test_lesommer_ingest.py
│   ├── test_merge_engine.py
│   ├── test_socotec_ingest.py
│   ├── test_status_normalization.py
│   └── test_terrell_ingest.py
│
├── ui/                         # Interface React (Vite, V1.0.3 — basique)
│   ├── src/
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
│
├── api_server.py               # FastAPI wrapping run_update_grandfichier.py pour l'UI
├── run_bet_ingest.py           # Entrypoint CLI pour ingestion BET standalone
├── run_update_grandfichier.py  # Entrypoint principal du pipeline GED→GF
├── requirements.txt
├── DESIGN.md                   # Spécifications techniques détaillées (V1)
└── README.md                   # Ce fichier
```

---

## Modules de traitement — rôle précis

| Module | Rôle |
|---|---|
| `ged_ingest.py` | Lit le dump AxeoBIM (feuille "Vue détaillée des documents 1"). Produit une `CanonicalResponse` par ligne (document × mission répondant). Filtre les lignes sans mission. |
| `grandfichier_reader.py` | Lit toutes les feuilles LOT du GrandFichier. Détecte la variante de layout (A: avec Zone, B: sans Zone). Lit les approbateurs depuis la Row 8. Produit une `GFRow` par ligne de données. |
| `matcher.py` | **GF-master** : pour chaque GFRow, cherche les CanonicalResponse GED correspondantes par NUMERO normalisé + INDICE. Gère les sheets `OLD *` (skip d'écriture, consume les doc IDs pour éviter les faux orphelins). |
| `merge_engine.py` | Consolide les CanonicalResponse matchées en `DeliverableRecord`. Applique la priorité source (GED > SAS > REPORT) par champ. Détecte les conflits. |
| `grandfichier_writer.py` | Applique les mises à jour sur le classeur GF : DATE/N°/STATUT par colonne approbateur, OBSERVATIONS (append-only avec dédup par groupe), VISA GLOBAL (copié depuis la colonne MOEX GEMO). |
| `bet_gf_writer.py` | Crée/upserte les 4 feuilles RAPPORT_* dans le GF. Logique upsert par UPSERT_KEY — les rapports déjà traités ne sont pas réinsérés lors des runs suivants. |
| `bet_backfill.py` | **Step 8** : lit les feuilles RAPPORT_*, construit un `BETReportIndex` par (NUMERO, INDICE), et reporte les avis dans les colonnes consultants des feuilles LOT. |
| `obs_helpers.py` | Helpers OBSERVATIONS partagés entre `grandfichier_writer.py` et `bet_backfill.py` : dédup par groupe, format d'entrée, normalisation des noms de groupes. |
| `lesommer_ingest.py` | Parser PDF Le Sommer (AMO HQE) : extraction Y-band spatiale pour LUMINAIRES/CVC/PLB. Statuts : 0→skip, 1→REF, 2→VAO, 3→VSO. |
| `avls_ingest.py` | Parser PDF AVLS (BET Acoustique) : parser streaming de blocs avis (VSO/VAO/REF/HM). Gère les layouts à 6 et 7 colonnes de header. |
| `terrell_ingest.py` | Parser PDF Terrell (BET Structure) : tables 19-colonnes, reconstruction de ref P17 décomposée. Statuts : cases X/✓ en colonnes 14–17. |
| `socotec_ingest.py` | Parser PDF SOCOTEC (Bureau de Contrôle) : mapping F→VSO, S→VAO, D→REF. Skip page 1. |
| `canonical.py` | Normalisation des clés de documents et des numéros (zero-padding, strip alpha). |
| `anomalies.py` | Logger centralisé. Types : UNMATCHED_GED, UNMATCHED_GF, STATUS_CONFLICT, PARSE_FAILURE, NO_GF_COLUMN, NOT_MOEX_RESPONSIBILITY. |

---

## Modèle de données

### CanonicalResponse
Représentation normalisée d'un avis depuis n'importe quelle source (GED, SAS, RAPPORT).
Champs clés : `numero`, `indice`, `lot`, `type_doc`, `mission`, `normalized_status`, `response_date`, `comment`, `source_type`.

### GFRow
Représentation d'une ligne du GrandFichier (document).
Champs clés : `sheet_name`, `row_number`, `document_key`, `numero`, `indice`, `ancien` (bool), `approbateurs` (list[GFApprobateur]).

### GFApprobateur
Un groupe de 3 colonnes (DATE / N° / STATUT) pour un consultant dans une feuille LOT.
Champs : `name` (texte Row 8), `col_date`, `col_num`, `col_statut` (0-indexed), `current_statut`.

### SourceEvidence
Trace chaque champ écrit : sheet, row, colonne, ancienne valeur, nouvelle valeur, source fichier+ligne.

### DeliverableRecord
Regroupe toutes les CanonicalResponse pour un document GF. Utilisé par le writer.

---

## Règles métier clés

| Règle | Description |
|---|---|
| **GF-master** | Le pipeline itère les GFRows, jamais les records GED en premier |
| **No new rows** | Aucune ligne n'est insérée dans les feuilles LOT |
| **OLD sheets** | Sheets `OLD *` : skip d'écriture, les doc IDs sont quand même consommés |
| **No overwrite** | Une cellule STATUT déjà remplie n'est jamais écrasée |
| **MOEX filter** | Seuls les documents avec au moins une réponse de mission MOEX sont traités |
| **VISA GLOBAL** | Copié depuis la colonne MOEX GEMO uniquement — jamais calculé par le pipeline |
| **OBS append-only** | OBSERVATIONS : ajout uniquement, jamais d'écrasement, dédup par groupe |
| **GED > BET** | La passe GED prime : si elle a rempli une colonne, la passe BET ne la touche pas |
| **Terrell OBS-only** | Terrell donne son avis dans la GED. Sa passe BET ne complète que les OBSERVATIONS |
| **RAPPORT_* history** | Les feuilles RAPPORT_* dans l'output servent d'historique : les rapports déjà traités ne sont pas réinsérés lors des runs suivants (détection par UPSERT_KEY) |
| **Déterminisme** | Mêmes inputs → mêmes outputs, toujours |

---

## Mapping consultants BET

| Consultant | Feuille RAPPORT_* | Colonne GF (Row 8) | Display OBS | Spécificité |
|---|---|---|---|---|
| Le Sommer | `RAPPORT_LE_SOMMER` | `AMO HQE LE SOMMER` | `AMO HQE` | Standard |
| AVLS | `RAPPORT_AVLS` | `ACOUSTICIEN AVLS` / `ACOUSTICIEN : AVLS` | `ACOUSTICIEN` | Standard |
| Terrell | `RAPPORT_TERRELL` | `BET STR-TERRELL` / `STR-TERRELL` | `BET STR` | **OBS-ONLY** — avis via GED, rapport = observations uniquement |
| SOCOTEC | `RAPPORT_SOCOTEC` | `BC SOCOTEC` / `SOCOTEC` | `SOCOTEC` | Standard |

---

## Installation et lancement

### Prérequis
- Python 3.11+
- Les fichiers input placés dans `input/` (voir structure ci-dessus)

### Installation
```bash
pip install -r requirements.txt
```

### Run complet (GED + BET backfill)
```powershell
python run_update_grandfichier.py --ged "input/ged/17CO_Tranche_2_du_23_mars_2026_07_45.xlsx" --grandfichier "input/grandfichier/P17-T2-VISA-Tableau de suivi (1).xlsx" --output output/ --bet-reports input/reports/ --loglevel INFO
```

### Run GED uniquement
```powershell
python run_update_grandfichier.py --ged "input/ged/17CO_Tranche_2_du_23_mars_2026_07_45.xlsx" --grandfichier "input/grandfichier/P17-T2-VISA-Tableau de suivi (1).xlsx" --output output/ --loglevel INFO
```

### Run BET standalone (parse PDFs → feuilles RAPPORT_* uniquement)
```powershell
python run_bet_ingest.py --lesommer "input/reports/AMO HQE" --avls "input/reports/BET Acoustique AVLS" --terrell "input/reports/BET Structure TERRELL" --socotec "input/reports/socotec" --gf "input/grandfichier/P17-T2-VISA-Tableau de suivi (1).xlsx"
```

### Réutilisation de l'output comme input (runs suivants)
```powershell
python run_update_grandfichier.py --ged "input/ged/NOUVEAU_DUMP.xlsx" --grandfichier "output/run_20260401_143000/updated_grandfichier.xlsx" --output output/ --bet-reports input/reports/ --loglevel INFO
```
> Le `updated_grandfichier.xlsx` d'un run précédent contient les feuilles `RAPPORT_*`.
> Les rapports PDF déjà traités ne seront pas réinsérés (détection par UPSERT_KEY).
> Seuls les nouveaux PDFs dans `input/reports/` produiront de nouveaux records.

### Tests
```bash
python -m pytest tests/ -v
```

### UI (optionnel — basique)
```bash
# Terminal 1
python api_server.py

# Terminal 2
cd ui && npm install && npm run dev
# Ouvrir http://localhost:5173
```

---

## Sorties

| Fichier | Description |
|---|---|
| `updated_grandfichier.xlsx` | GrandFichier mis à jour. Contient aussi les feuilles `RAPPORT_*` comme historique interne. |
| `evidence_export.csv` | Une ligne par champ écrit : sheet, row, colonne, ancienne valeur, nouvelle valeur, source. |
| `anomaly_log.json` | Toutes les anomalies : documents non matchés, conflits de statut, erreurs de parsing. |
| `match_summary.csv` | Statistiques de matching : GF_MATCHED / GF_NO_GED / GF_INDICE_MISMATCH / GF_OLD_SHEET_SKIP. |
| `orphan_ged_documents.xlsx` | Documents présents dans la GED mais absents du GrandFichier (non insérés). |
| `orphan_summary.xlsx` | Résumé agrégé des orphelins par LOT et TYPE_DOC. |

---

## État actuel

### ✅ Terminé et opérationnel

| Composant | Statut |
|---|---|
| Pipeline GED → GF complet (Steps 1–7) | ✅ Opérationnel |
| 4 parsers PDF BET (Le Sommer, AVLS, Terrell, SOCOTEC) | ✅ Stable — 171+474+214+508 records |
| Écriture feuilles RAPPORT_* dans le GF (`bet_gf_writer.py`) | ✅ Avec upsert historique |
| Moteur de matching GF-master (NUMERO + INDICE) | ✅ |
| Logique OBSERVATIONS (append-only + dédup par groupe) | ✅ |
| **BET Backfill — Step 8** (`bet_backfill.py` + `obs_helpers.py`) | ✅ Implémenté et opérationnel |
| Tests unitaires pour tous les parsers et modules core | ✅ 9 fichiers de tests |
| UI React basique (V1.0.3) + API FastAPI | ✅ Fonctionnel |

### 🟡 Améliorations futures

- **UI améliorée** : la UI actuelle (V1.0.3) affiche les logs mais pas les résultats structurés
- **Support `--bet-reports` dans l'UI** : `api_server.py` n'expose pas encore ce flag
- **Tests BET backfill** : `tests/test_bet_backfill.py` à créer (13 cas documentés dans le plan)
- **SAS** : désactivé volontairement — process manuel côté MOEX, non intégré en V1

---

## Configuration

### data/mission_map.json
Mappe les missions GED → groupe unifié → noms de colonnes GF (variants) → rôle.

```json
{
  "ged_to_group":     { "0-BET Structure": "BET Structure", ... },
  "group_to_gf_appro":{ "BET Structure": ["BET STR-TERRELL", "STR-TERRELL"], ... },
  "special_groups":   { "MOEX SAS": "SKIP" },
  "no_gf_column":     ["MOEX SAS", "BET VRD", "BIM MANAGER", ...],
  "group_role":       { "BET Structure": "primary", "AMO HQE": "secondary", ... }
}
```

### data/source_priority.json
Priorité source par champ : GED > SAS > REPORT. Modifiable sans toucher au code.

### data/status_map.json
Mapping statuts bruts GED → codes normalisés. Ex : `"Visa avec observations"` → `"VAO"`.

### TAG_PRIORITY (dans config.py)
Ordre de criticité des statuts : `DEF > REF > SUS > VAO > VSO > FAV > HM > ANN`

---

## Principes de développement

- **Déterministe** : mêmes inputs → mêmes outputs, toujours
- **Traçable** : chaque valeur écrite est liée à sa source (SourceEvidence)
- **Anomalie-first** : tout ce qui est ambigu est loggué, jamais silencieusement ignoré
- **Config-driven** : les règles métier sont dans les JSON, pas hardcodées
- **Pas d'IA dans le core** : le parsing PDF utilise du pattern matching déterministe
- **GF-master** : le GrandFichier est la source de vérité structurelle — le pipeline ne crée jamais de nouvelles lignes
- **Patch discipline** : les correctifs sont ciblés et scopés — pas de refactoring global sans raison explicite
