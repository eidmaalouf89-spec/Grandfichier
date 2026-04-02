# -*- mode: python ; coding: utf-8 -*-
#
# PyInstaller spec — JANSA GrandFichier Updater
#
# Build steps (run from project root):
#
#   1. Build the React frontend first:
#      cd ui
#      npm install
#      npm run build
#      cd ..
#
#   2. Then build the EXE:
#      pip install pyinstaller
#      pyinstaller JANSA_GF.spec
#
# Output: dist1/JANSA_GrandFichier/JANSA_GrandFichier.exe
#
# The EXE is a FOLDER distribution (onedir), not a single file (onefile).
# onefile mode causes slow cold starts (>30s) because it extracts to a temp
# dir on every launch — incompatible with a server that must start fast.

import sys
from pathlib import Path
import PyInstaller.config  # noqa: E402

ROOT = Path(SPECPATH)  # noqa: F821  (PyInstaller magic variable)

# Output to dist1/ so we don't conflict with any locked dist/ folder
PyInstaller.config.CONF['distpath'] = str(ROOT / 'dist1')

block_cipher = None

a = Analysis(
    # Entry point — the API server launches the pipeline as a subprocess
    [str(ROOT / 'api_server.py')],

    pathex=[str(ROOT)],
    binaries=[],

    datas=[
        # JSON config maps
        (str(ROOT / 'data'),              'data'),

        # Processing modules (imported by run_update_grandfichier.py subprocess)
        (str(ROOT / 'processing'),        'processing'),

        # Pipeline entrypoints (launched as subprocesses by api_server.py)
        (str(ROOT / 'run_update_grandfichier.py'), '.'),
        (str(ROOT / 'run_bet_ingest.py'),          '.'),

        # React compiled frontend — MUST run `npm run build` in ui/ first
        (str(ROOT / 'ui' / 'dist'),       'ui/dist'),
    ],

    hiddenimports=[
        # FastAPI / Starlette internals
        'fastapi',
        'fastapi.staticfiles',
        'fastapi.responses',
        'starlette.routing',
        'starlette.middleware',
        'starlette.middleware.cors',
        'starlette.staticfiles',
        # Uvicorn
        'uvicorn',
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
        # Multipart (file uploads)
        'multipart',
        'python_multipart',
        # Excel
        'openpyxl',
        'openpyxl.styles',
        'openpyxl.utils',
        # PDF
        'pdfplumber',
        'pdfminer',
        'pdfminer.high_level',
        'pdfminer.layout',
        # Processing modules
        'processing.actors',
        'processing.anomalies',
        'processing.avls_ingest',
        'processing.bet_backfill',
        'processing.bet_gf_writer',
        'processing.canonical',
        'processing.config',
        'processing.dates',
        'processing.ged_ingest',
        'processing.grandfichier_reader',
        'processing.grandfichier_writer',
        'processing.lesommer_ingest',
        'processing.matcher',
        'processing.merge_engine',
        'processing.models',
        'processing.obs_helpers',
        'processing.pdf_ingest',
        'processing.sas_ingest',
        'processing.socotec_ingest',
        'processing.statuses',
        'processing.terrell_ingest',
        # Stdlib extras
        'asyncio',
        'email.mime.multipart',
        'email.mime.text',
        'logging.handlers',
    ],

    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'pytest',
        'pip',
        'setuptools',
        'wheel',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)  # noqa: F821

exe = EXE(  # noqa: F821
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='JANSA_GrandFichier',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,   # Change to False once stable to hide the terminal window
    icon=None,
)

coll = COLLECT(  # noqa: F821
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='JANSA_GrandFichier',
)
