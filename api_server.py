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
import shutil
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

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
):
    """
    Accept GED + GrandFichier uploads, start the pipeline in the background.
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

    # Output goes to output/run_YYYYMMDD_HHMMSS/
    output_run_dir = OUTPUT_DIR / f"run_{ts}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    _runs[run_id] = {
        "logs": [],
        "done": False,
        "success": False,
        "output_dir": f"output/run_{ts}",
        "tmp": str(tmp),
        "returncode": None,
    }

    cmd = [
        sys.executable,
        str(BASE_DIR / "run_update_grandfichier.py"),
        "--ged", str(ged_path),
        "--grandfichier", str(gf_path),
        "--output", str(output_run_dir),
    ]

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
        shutil.rmtree(run.get("tmp", ""), ignore_errors=True)


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
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=False)
