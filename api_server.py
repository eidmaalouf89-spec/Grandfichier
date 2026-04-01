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

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

app = FastAPI(title="JANSA GrandFichier Updater API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory run registry: run_id -> run state dict
_runs: dict = {}


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

    cmd = [
        sys.executable,
        str(BASE_DIR / "run_update_grandfichier.py"),
        "--ged", str(ged_path),
        "--grandfichier", str(gf_path),
        "--output", str(output_run_dir),
    ]
    if bet_reports_dir:
        cmd += ["--bet-reports", str(bet_reports_dir)]

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
            cwd=str(BASE_DIR),
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
    return {
        "done": r["done"],
        "success": r["success"],
        "output_dir": r["output_dir"],
        "log_lines": len(r["logs"]),
        "mode": r.get("mode", "GED only"),
        "has_bet": r.get("has_bet", False),
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
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=False)
