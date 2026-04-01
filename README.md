# JANSA GrandFichier Updater

**Projet :** P17&CO Tranche 2
**Rôle :** MOEX (Maître d'Œuvre d'Exécution) — Eid Maalouf
**Statut :** 🟢 Opérationnel — Pipeline GED→GF complet + BET backfill (Step 8) + UI React V2

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
├── ui/                         # Interface React (Vite, V2.0.0)
│   ├── src/
│   │   ├── main.jsx                # Point d'entrée React
│   │   ├── App.jsx                 # Composant racine — gestion état global
│   │   ├── index.css               # CSS global (variables CSS natives, pas de framework)
│   │   └── components/
│   │       ├── Sidebar.jsx         # Historique des runs de session
│   │       ├── UploadPanel.jsx     # Upload GED + GF + toggle BET (4 zones PDF)
│   │       ├── ProgressPanel.jsx   # SSE stream + 8 steps avec statuts
│   │       └── ResultsPanel.jsx    # Métriques + boutons de téléchargement
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

### UI React V2
```bash
# Terminal 1 — backend API (FastAPI sur port 8000)
python api_server.py

# Terminal 2 — frontend Vite (sur port 5173)
cd ui
npm install   # seulement la première fois, ou après changement de machine
npm run dev

# Ouvrir http://localhost:5173
```

> **Important :** `npm install` doit être relancé si le dossier `node_modules` a été créé sur
> une autre plateforme (ex : Linux → Windows). Les binaires natifs de Rollup/Vite ne sont
> pas portables entre OS.

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

## Interface React V2

### Architecture

L'UI est une SPA React (Vite) qui communique avec un backend FastAPI via proxy Vite (`/api` → `http://127.0.0.1:8000`). Aucune dépendance CSS externe — tout est en variables CSS natives.

```
Browser (localhost:5173)
    │
    ├─► GET /api/runs            → liste des runs en session (sidebar)
    ├─► POST /api/run            → démarrage pipeline (upload GED + GF + BET PDFs)
    ├─► GET /api/stream/{run_id} → SSE stream des logs (progression temps réel)
    ├─► GET /api/run/{id}/status → statut final + métriques
    └─► GET /api/run/{id}/download/{grandfichier|debug_zip} → téléchargement
```

### Composants

**`App.jsx`** — gestion de l'état global : liste des runs (`GET /api/runs`), run actif, panel actif (`upload` / `progress` / `results`). Topbar avec le timestamp du run et le badge mode (GED only / GED + BET). Navigation 3 étapes.

**`Sidebar.jsx`** — liste tous les runs de la session avec statut coloré (en cours / succès / erreur). Bouton "+ Nouveau run" pour réinitialiser vers l'écran d'upload.

**`UploadPanel.jsx`** — deux zones de dépôt (GED xlsx + GF xlsx, requis) + toggle "Inclure la passe BET" qui déploie 4 zones PDF (Le Sommer, AVLS, Terrell, SOCOTEC). Bannière d'avertissement SAS/indices. Le bouton "Lancer le pipeline" est désactivé tant que les 2 fichiers requis ne sont pas sélectionnés.

**`ProgressPanel.jsx`** — se connecte au SSE `/api/stream/{run_id}`. Parse chaque ligne de log pour détecter la step active (détection par mots-clés). Affiche une barre de progression + les 8 steps avec indicateurs visuels (en attente / actif / fait / erreur). Compteur de temps réel.

**`ResultsPanel.jsx`** — après complétion, charge le statut via `/api/run/{id}/status`. Affiche 3 métriques (statut, nombre de logs, mode). Propose le téléchargement du GrandFichier mis à jour et du ZIP de debug. En cas d'erreur, affiche les deux boutons quand même (le GF est souvent produit même si step 8 crash).

### Fonctionnement du backend (api_server.py)

Le serveur FastAPI maintient un registre en mémoire `_runs` (perdu au redémarrage). Chaque run est identifié par un UUID court de 8 caractères.

Au lancement (`POST /api/run`) :
1. Les fichiers uploadés sont écrits dans un dossier temporaire `tmp/`
2. Si des PDFs BET sont fournis, les sous-dossiers `AMO HQE/`, `BET Acoustique AVLS/`, etc. sont créés dans `tmp/reports/`
3. La commande `run_update_grandfichier.py` est lancée en subprocess
4. Le dossier `tmp/` est conservé (pas nettoyé) pour pouvoir être inclus dans le ZIP de debug

Le pipeline crée son propre sous-dossier `run_{ts}/` à l'intérieur du `--output` passé. Les endpoints de téléchargement recherchent les fichiers récursivement pour gérer cette double imbrication.

---

## Problèmes rencontrés et solutions

### 1. `run_bet_ingest.py` tronqué après un merge conflict

**Problème :** La fonction `run_bet_ingest_to_workbook()` était tronquée à la ligne 322 — les 6 dernières lignes manquaient (reste d'un merge conflict mal résolu). Python levait une `SyntaxError` à l'import, causant le crash silencieux de tous les runs GED+BET. Les fichiers `evidence_export.csv`, `anomaly_log.json` et `match_summary.csv` n'étaient pas générés car le pipeline mourait avant la section "Write outputs".

**Symptôme visible :** Run marqué "Erreur", seuls 3 fichiers produits (`updated_grandfichier.xlsx`, `orphan_ged_documents.xlsx`, `orphan_summary.xlsx`), pas de traceback visible dans l'UI.

**Solution :** Restauration des 6 lignes manquantes depuis `git show cf759a1:run_bet_ingest.py`. Ajout d'un `try/except` autour de l'appel `backfill_bet_reports()` dans `run_update_grandfichier.py` pour que le pipeline continue d'écrire ses outputs même si step 8 crash, et affiche le traceback complet dans le terminal.

---

### 2. Répertoire de sortie doublement imbriqué

**Problème :** `api_server.py` crée `output/run_{ts}/` et le passe comme `--output` au pipeline. Mais `run_update_grandfichier.py` crée lui-même un sous-répertoire `run_{ts}/` à l'intérieur. Les fichiers finaux se retrouvaient donc dans `output/run_20260401_175911/run_20260401_175941/` au lieu de `output/run_20260401_175911/`.

**Symptôme visible :** Bouton "Télécharger GF" retournait 404. Le ZIP ne contenait que les inputs, pas les outputs.

**Solution :** L'endpoint `download_file` utilise maintenant `rglob()` pour trouver `updated_grandfichier.xlsx` récursivement. La construction du ZIP utilise aussi `rglob("*")` au lieu de `iterdir()` pour inclure tous les fichiers imbriqués.

---

### 3. Download bloqué même sur runs en erreur

**Problème :** L'endpoint `GET /api/run/{id}/download/{filename}` vérifiait `run["success"] == True` avant d'autoriser n'importe quel téléchargement. Mais le ZIP de debug est particulièrement utile justement quand le run a échoué.

**Solution :** La guard condition ne vérifie plus que `run["done"]`. Le GF est servi s'il existe sur disque (sans vérification de success). Le `ResultsPanel` affiche maintenant les deux boutons de téléchargement même en cas d'erreur.

---

### 4. `node_modules` Windows/Linux incompatibles

**Problème :** Les binaires natifs de Rollup (utilisé par Vite) ne sont pas portables entre plateformes. Un `node_modules` installé sous Linux ne fonctionne pas sous Windows et vice versa, causant l'erreur `Cannot find module @rollup/rollup-linux-x64-gnu`.

**Solution :** Toujours relancer `npm install` après avoir cloné ou changé de machine. Le `node_modules` ne doit pas être versionné ni transféré entre OS.

---

### 5. Vite inaccessible depuis Chrome sur Windows

**Contexte (environnement de dev uniquement) :** Lors du développement, le serveur Vite tournait dans un container Linux tandis que Chrome était sur Windows. `localhost:5173` sur Windows ne résolvait pas vers le container.

**Solution (dev uniquement) :** Vite relancé avec `--host 0.0.0.0` pour exposer sur le réseau local + `allowedHosts: "all"` dans `vite.config.js` + tunnel localtunnel pour passer la vérification de host. En utilisation normale (Cursor sur Windows), `localhost:5173` fonctionne directement.

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
| UI React V2 + API FastAPI (BET uploads, download endpoints, /api/runs) | ✅ Fonctionnel |

### 🟡 Améliorations futures

- **Tests BET backfill** : `tests/test_bet_backfill.py` à créer (13 cas documentés dans le plan)
- **Métriques réelles dans ResultsPanel** : afficher les vraies stats du run (champs mis à jour, matchs, orphelins) en parsant `match_summary.csv` côté API plutôt que les métriques approximatives actuelles
- **Nettoyage des dossiers tmp** : les inputs BET ne sont jamais supprimés actuellement — prévoir un nettoyage après 24h ou après téléchargement du ZIP
- **SAS** : désactivé volontairement — process manuel côté MOEX, non intégré en V1
- **Persistance des runs** : le registre `_runs` est en mémoire — perdu au redémarrage de l'API. Une persistance fichier (JSON) permettrait de retrouver l'historique après restart

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
