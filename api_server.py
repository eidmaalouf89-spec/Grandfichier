#!/usr/bin/env python3
"""
JANSA GrandFichier Updater — UI API Server

Wraps run_update_grandfichier.py for the React UI.

Usage:
    pip install fastapi uvicorn python-multipart
    python api_server.py
    # Then open http://localhost:5173 (after: cd ui && npm run dev)
"""
import asyncio
import json
import os
import shutil
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ── PyInstaller self-dispatch ──────────────────────────────────────────────────
# When frozen, subprocesses cannot run .py scripts directly (sys.executable is
# the EXE, not python.exe).  Instead, api_server.py re-invokes ITSELF with a
# special hidden flag so the same EXE can act as the pipeline runner.
_PIPELINE_FLAG = "--_internal-pipeline"

if getattr(sys, "frozen", False) and len(sys.argv) > 1 and sys.argv[1] == _PIPELINE_FLAG:
    # ── Pipeline subprocess mode ──
    # Strip our flag so the pipeline's argparse sees the real args.
    sys.argv = [sys.argv[0]] + sys.argv[2:]
    # _MEIPASS is on sys.path in frozen builds, so this import works.
    import run_update_grandfichier as _pipeline_mod  # noqa: E402
    _pipeline_mod.main()
    sys.exit(0)

# ── PyInstaller compatibility ──────────────────────────────────────────────────
# When frozen by PyInstaller, __file__ is inside a temp _MEIPASS bundle.
# BASE_DIR must point to the folder containing the EXE (where output/ lives),
# not to the bundle temp dir.
if getattr(sys, "frozen", False):
    # Running as PyInstaller EXE
    BASE_DIR = Path(sys.executable).parent
    _BUNDLE_DIR = Path(sys._MEIPASS)
    # The pipeline script is extracted to the bundle temp dir
    _PIPELINE_SCRIPT = _BUNDLE_DIR / "run_update_grandfichier.py"
    # Python interpreter bundled by PyInstaller
    _PYTHON_EXE = sys.executable
else:
    # Running in normal dev mode
    BASE_DIR = Path(__file__).parent
    _BUNDLE_DIR = BASE_DIR
    _PIPELINE_SCRIPT = BASE_DIR / "run_update_grandfichier.py"
    _PYTHON_EXE = sys.executable

OUTPUT_DIR = BASE_DIR / "output"
RUNS_DB_PATH = BASE_DIR / "runs.json"


def _load_runs_db() -> dict:
    """Load run registry from disk. Returns empty dict if file missing/corrupt."""
    try:
        if RUNS_DB_PATH.exists():
            with open(RUNS_DB_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_runs_db(runs: dict) -> None:
    """Persist run registry to disk. Silently ignore write errors."""
    try:
        with open(RUNS_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(runs, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def _cleanup_old_tmp_dirs(max_age_hours: int = 48) -> None:
    """
    Delete tmp directories created by previous runs that are older than max_age_hours.
    Called once at server startup. Silently ignores errors.
    """
    import time
    cutoff = time.time() - (max_age_hours * 3600)
    tmp_root = Path(tempfile.gettempdir())
    deleted = 0
    try:
        for d in tmp_root.glob("jansa_ui_*"):
            if d.is_dir() and d.stat().st_mtime < cutoff:
                shutil.rmtree(d, ignore_errors=True)
                deleted += 1
        if deleted:
            print(f"[startup] Cleaned up {deleted} old tmp dir(s) older than {max_age_hours}h")
    except Exception:
        pass


def _read_match_summary(output_run_dir: Path) -> dict:
    """
    Read match_summary.csv from the run output directory (recursive search).
    Returns a dict with keys: gf_matched, gf_no_ged, gf_indice_mismatch, gf_old_skip, total
    Returns empty dict if file not found or unreadable.
    """
    import csv
    matches = list(output_run_dir.rglob("match_summary.csv"))
    if not matches:
        return {}
    try:
        counts = {}
        with open(matches[0], newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                level = row.get("match_level", "").upper()
                try:
                    counts[level] = int(row.get("count", 0))
                except ValueError:
                    counts[level] = 0
        total = sum(counts.values())
        return {
            "gf_matched": counts.get("GF_MATCHED", 0),
            "gf_no_ged": counts.get("GF_NO_GED", 0),
            "gf_indice_mismatch": counts.get("GF_INDICE_MISMATCH", 0),
            "gf_old_skip": counts.get("GF_OLD_SHEET_SKIP", 0),
            "total": total,
        }
    except Exception:
        return {}


app = FastAPI(title="JANSA GrandFichier Updater API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Run registry: loaded from disk at startup, persisted after each update
_runs: dict = _load_runs_db()


@app.post("/api/run")
async def start_run(
    ged: UploadFile = File(...),
    gf: UploadFile = File(...),
    bet_lesommer: Optional[List[UploadFile]] = File(default=None),
    bet_avls: Optional[List[UploadFile]] = File(default=None),
    bet_terrell: Optional[List[UploadFile]] = File(default=None),
    bet_socotec: Optional[List[UploadFile]] = File(default=None),
):
    """
    Accept GED + GrandFichier uploads (+ optional BET PDFs), start the pipeline in the background.
    Returns {run_id} immediately.
    """
    run_id = str(uuid.uuid4())[:8]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Write uploads to a temp directory
    tmp = Path(tempfile.mkdtemp(prefix="jansa_ui_"))
    ged_path = tmp / ged.filename
    gf_path = tmp / gf.filename

    with open(ged_path, "wb") as f:
        f.write(await ged.read())
    with open(gf_path, "wb") as f:
        f.write(await gf.read())

    # Préparer les dossiers BET si des PDFs sont fournis
    bet_reports_dir = None
    bet_map = {
        "AMO HQE": bet_lesommer,
        "BET Acoustique AVLS": bet_avls,
        "BET Structure TERRELL": bet_terrell,
        "socotec": bet_socotec,
    }
    has_bet = any(v for v in bet_map.values() if v)
    if has_bet:
        bet_reports_dir = tmp / "reports"
        for folder_name, files in bet_map.items():
            if files:
                folder = bet_reports_dir / folder_name
                folder.mkdir(parents=True, exist_ok=True)
                for f in files:
                    dest = folder / f.filename
                    with open(dest, "wb") as out:
                        out.write(await f.read())

    # Output goes to output/run_YYYYMMDD_HHMMSS/
    output_run_dir = OUTPUT_DIR / f"run_{ts}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    _runs[run_id] = {
        "logs": [],
        "done": False,
        "success": False,
        "output_dir": f"output/run_{ts}",
        "output_run_dir": str(output_run_dir),
        "input_dir": str(tmp),
        "tmp": str(tmp),
        "returncode": None,
        "has_bet": has_bet,
        "mode": "GED + BET" if has_bet else "GED only",
    }

    if getattr(sys, "frozen", False):
        # Frozen EXE: re-invoke self with the internal pipeline flag.
        cmd = [
            _PYTHON_EXE,
            _PIPELINE_FLAG,
            "--ged", str(ged_path),
            "--grandfichier", str(gf_path),
            "--output", str(output_run_dir),
        ]
    else:
        # Dev mode: call the script via python interpreter.
        cmd = [
            _PYTHON_EXE,
            str(_PIPELINE_SCRIPT),
            "--ged", str(ged_path),
            "--grandfichier", str(gf_path),
            "--output", str(output_run_dir),
        ]
    if bet_reports_dir:
        cmd += ["--bet-reports", str(bet_reports_dir)]

    _save_runs_db(_runs)
    asyncio.create_task(_run_pipeline(run_id, cmd))
    return {"run_id": run_id}


async def _run_pipeline(run_id: str, cmd: list):
    """Background task: run subprocess, collect stdout line by line."""
    run = _runs[run_id]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(_BUNDLE_DIR),
        )
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            if line:
                run["logs"].append(line)
        await proc.wait()
        run["returncode"] = proc.returncode
        run["success"] = proc.returncode == 0
    except Exception as exc:
        run["logs"].append(f"ERROR: {exc}")
        run["success"] = False
    finally:
        run["done"] = True
        _save_runs_db(_runs)  # persist to disk
        # Note: tmp dir is kept for ZIP download — not cleaned up here


@app.get("/api/stream/{run_id}")
async def stream_run(run_id: str):
    """
    SSE stream of log lines for a running pipeline.
    Each event is JSON: {line, done, output_dir?, success?}
    """
    if run_id not in _runs:
        raise HTTPException(status_code=404, detail="Run not found")

    async def _generate():
        idx = 0
        while True:
            run = _runs[run_id]
            # Flush any new lines
            while idx < len(run["logs"]):
                payload = json.dumps({"line": run["logs"][idx], "done": False})
                yield f"data: {payload}\n\n"
                idx += 1
            # Check if process finished
            if run["done"] and idx >= len(run["logs"]):
                payload = json.dumps({
                    "line": "",
                    "done": True,
                    "output_dir": run["output_dir"],
                    "success": run["success"],
                })
                yield f"data: {payload}\n\n"
                break
            await asyncio.sleep(0.15)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/run/{run_id}/status")
async def run_status(run_id: str):
    if run_id not in _runs:
        raise HTTPException(status_code=404, detail="Run not found")
    r = _runs[run_id]
    stats = {}
    if r["done"]:
        stats = _read_match_summary(Path(r["output_run_dir"]))
    return {
        "done": r["done"],
        "success": r["success"],
        "output_dir": r["output_dir"],
        "log_lines": len(r.get("logs", [])),
        "mode": r.get("mode", "GED only"),
        "has_bet": r.get("has_bet", False),
        "stats": stats,  # {gf_matched, gf_no_ged, gf_indice_mismatch, gf_old_skip, total}
    }


@app.get("/api/run/{run_id}/download/{filename}")
async def download_file(run_id: str, filename: str):
    """Télécharger un fichier output du run (GF xlsx ou ZIP debug)."""
    if run_id not in _runs:
        raise HTTPException(status_code=404, detail="Run not found")
    run = _runs[run_id]
    if not run["done"]:
        raise HTTPException(status_code=400, detail="Run not complete")

    output_run_dir = Path(run["output_run_dir"])

    # The pipeline creates its own run_{ts} subdirectory inside output_run_dir.
    # Search recursively so we handle both flat and nested structures.
    def _find_output_files(root: Path) -> list[Path]:
        """Return all output files under root, recursively, excluding ZIPs."""
        return [f for f in root.rglob("*") if f.is_file() and not f.name.endswith(".zip")]

    if filename == "grandfichier":
        matches = list(output_run_dir.rglob("updated_grandfichier.xlsx"))
        if not matches:
            raise HTTPException(status_code=404, detail="GrandFichier not found — pipeline may have failed before writing it")
        gf_file = matches[0]
        return FileResponse(
            path=str(gf_file),
            filename="updated_grandfichier.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    elif filename == "debug_zip":
        ts = output_run_dir.name.replace("run_", "")
        zip_name = f"Pour_EID_RUN_{ts}.zip"
        zip_path = output_run_dir / zip_name

        # Always rebuild the ZIP so it picks up all outputs
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # Outputs — recurse into nested run_* subdirectory if present
            for f in _find_output_files(output_run_dir):
                rel = f.relative_to(output_run_dir)
                zf.write(f, f"outputs/{rel}")
            # Inputs (ged + gf + BET PDFs)
            input_dir = Path(run.get("input_dir", run.get("tmp", "")))
            if input_dir.exists():
                for f in input_dir.rglob("*"):
                    if f.is_file():
                        rel = f.relative_to(input_dir)
                        zf.write(f, f"inputs/{rel}")

        return FileResponse(
            path=str(zip_path),
            filename=zip_name,
            media_type="application/zip"
        )

    else:
        raise HTTPException(status_code=400, detail="Unknown filename. Use 'grandfichier' or 'debug_zip'")


@app.get("/api/runs")
async def list_runs():
    """Retourne la liste de tous les runs pour la sidebar historique."""
    result = []
    for run_id, r in _runs.items():
        # Only show runs whose output directory still exists on disk
        odir = Path(r.get("output_run_dir", ""))
        if not odir.exists():
            continue
        result.append({
            "run_id": run_id,
            "done": r["done"],
            "success": r["success"],
            "mode": r.get("mode", "GED only"),
            "output_dir": r["output_dir"],
            "timestamp": r["output_dir"].replace("output/run_", ""),
        })
    return {"runs": sorted(result, key=lambda x: x["timestamp"], reverse=True)}


if __name__ == "__main__":
    import webbrowser
    import threading
    import uvicorn

    # Serve the compiled React frontend (ui/dist/) as static files.
    # Works both in dev (relative path) and when frozen by PyInstaller (_BUNDLE_DIR).
    _UI_DIST = _BUNDLE_DIR / "ui" / "dist"
    if _UI_DIST.exists():
        # Mount static assets (JS/CSS/icons)
        app.mount("/assets", StaticFiles(directory=str(_UI_DIST / "assets")), name="assets")

        # SPA catch-all: serve index.html for any non-/api route
        from fastapi.responses import HTMLResponse

        @app.get("/{full_path:path}", include_in_schema=False)
        async def spa_fallback(full_path: str):
            index = _UI_DIST / "index.html"
            return HTMLResponse(content=index.read_text(encoding="utf-8"))

    # Open browser after a short delay (give uvicorn time to start)
    def _open_browser():
        import time
        time.sleep(1.2)
        webbrowser.open("http://127.0.0.1:8000")

    threading.Thread(target=_open_browser, daemon=True).start()

    _cleanup_old_tmp_dirs(max_age_hours=48)
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False)
