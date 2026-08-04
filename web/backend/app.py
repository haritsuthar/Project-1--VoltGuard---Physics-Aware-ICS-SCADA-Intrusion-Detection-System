"""
VoltGuard Web Backend — FastAPI
Endpoints:
  GET  /api/decisions  — all records from output/decisions.jsonl
  GET  /api/stats      — summary counts
  POST /api/run        — run full pipeline (sync, returns when done)
  GET  /api/stream     — SSE live stream of pipeline output
  GET  /               — frontend dashboard
"""

import asyncio
import json
import queue
import subprocess
import sys
import threading
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[2]
INTERCEPTOR = ROOT / "packet_interceptor" / "interceptor.py"
GENERATOR   = ROOT / "packet_interceptor" / "generator.py"
DECISIONS   = ROOT / "output" / "decisions.jsonl"
FRONTEND    = Path(__file__).resolve().parent.parent / "frontend"

app = FastAPI(title="VoltGuard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(FRONTEND / "static")), name="static")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _read_decisions():
    if not DECISIONS.exists() or DECISIONS.stat().st_size == 0:
        return []
    records = []
    with DECISIONS.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


# ── Frontend ──────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(content=(FRONTEND / "index.html").read_text(encoding="utf-8"))


# ── API: decisions ────────────────────────────────────────────────────────────
@app.get("/api/decisions")
async def get_decisions():
    records = _read_decisions()
    if not records:
        return JSONResponse({"records": [], "message": "No decisions yet. Run the pipeline first."})
    return {"records": records}


# ── API: stats ────────────────────────────────────────────────────────────────
@app.get("/api/stats")
async def get_stats():
    records = _read_decisions()
    total = len(records)
    allow = sum(1 for r in records if r.get("action") == "ALLOW")
    drop  = total - allow
    return {
        "total":     total,
        "allow":     allow,
        "drop":      drop,
        "allow_pct": round(allow / total * 100, 1) if total else 0,
        "drop_pct":  round(drop  / total * 100, 1) if total else 0,
    }


# ── API: run (synchronous, returns when complete) ─────────────────────────────
@app.post("/api/run")
async def run_pipeline():
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: subprocess.check_call(
            [sys.executable, str(GENERATOR)], timeout=30))
        await loop.run_in_executor(None, lambda: subprocess.check_call(
            [sys.executable, str(INTERCEPTOR)], timeout=60))
        return {"status": "success", "message": "Pipeline completed successfully."}
    except subprocess.CalledProcessError as exc:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(exc)})
    except subprocess.TimeoutExpired:
        return JSONResponse(status_code=500, content={"status": "error", "message": "Pipeline timed out."})


# ── API: stream (SSE) ─────────────────────────────────────────────────────────
@app.get("/api/stream")
async def stream_pipeline(request: Request):
    """
    SSE stream of the full pipeline run.
    Uses a thread + queue to safely stream subprocess stdout on Windows,
    where asyncio.create_subprocess_exec is unreliable with SelectorEventLoop.
    Sends retry:0 so the browser does NOT auto-reconnect.
    """

    # Queue carries either a dict payload or the sentinel None (stream done)
    q: queue.Queue = queue.Queue()

    def _run_pipeline():
        """Runs entirely in a background thread — no async code here."""

        def push(msg: str, typ: str = "info"):
            q.put({"msg": msg, "type": typ})

        # ── Step 1: generator ────────────────────────────────────────────
        push("[1/2] Generating Modbus commands...", "info")
        try:
            subprocess.run(
                [sys.executable, str(GENERATOR)],
                timeout=30,
                check=True,
                capture_output=True,
            )
            push("[1/2] sample_log.jsonl created.", "info")
        except Exception as exc:
            push(f"Generator error: {exc}", "error")
            push("Pipeline failed.", "done")
            q.put(None)  # sentinel
            return

        # ── Step 2: interceptor — stream stdout line-by-line ─────────────
        push("[2/2] Running detection pipeline...", "info")
        try:
            proc = subprocess.Popen(
                [sys.executable, str(INTERCEPTOR)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,          # line-buffered
                env={**__import__("os").environ, "PYTHONIOENCODING": "utf-8"},
            )
            for line in proc.stdout:
                line = line.rstrip()
                if not line:
                    continue
                if "DROP" in line:
                    typ = "drop"
                elif "ALLOW" in line:
                    typ = "allow"
                else:
                    typ = "info"
                push(line, typ)
            proc.wait()
        except Exception as exc:
            push(f"Interceptor error: {exc}", "error")

        push("Pipeline complete!", "done")
        q.put(None)  # sentinel — tells generator() to stop

    async def generate():
        # Disable browser auto-reconnect
        yield "retry: 0\n"
        yield _sse({"msg": "▶ Starting pipeline...", "type": "info"})

        # Kick off the blocking pipeline in a thread-pool thread
        loop = asyncio.get_event_loop()
        thread_future = loop.run_in_executor(None, _run_pipeline)

        while True:
            # Check client disconnect
            if await request.is_disconnected():
                break

            # Drain everything currently in the queue without blocking the loop
            try:
                while True:
                    item = q.get_nowait()
                    if item is None:
                        # Sentinel — pipeline finished
                        await thread_future   # ensure thread is done
                        return
                    yield _sse(item)
            except queue.Empty:
                # Nothing ready yet — yield control briefly so other coroutines run
                await asyncio.sleep(0.05)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"
