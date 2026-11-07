"""FastAPI service: start, signal, describe, history, terminate, list, health, metrics, UI."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel

from durableflow import metrics
from durableflow.clock import iso
from durableflow.engine import Engine
from durableflow.errors import NotFound, NotRunning
from durableflow.logs import log
from durableflow.worker import Registry, Worker

logger = logging.getLogger("durableflow.api")
STATIC = Path(__file__).parent / "static"


class StartRequest(BaseModel):
    workflow_type: str
    input: Any = None
    workflow_id: str | None = None


class SignalRequest(BaseModel):
    name: str
    payload: Any = None


class TerminateRequest(BaseModel):
    reason: str = "terminated by operator"


def create_app(engine: Engine, registry: Registry, embedded_workers: int = 0) -> FastAPI:
    app = FastAPI(title="durableflow", version="1.0.0")
    stop = threading.Event()
    app.state.engine = engine
    app.state.stop = stop

    if embedded_workers:
        for i in range(embedded_workers):
            w = Worker(engine, registry, f"embedded-{i + 1}")
            threading.Thread(target=w.run_forever, args=(stop,), daemon=True).start()

    @app.middleware("http")
    async def access_log(request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        t = time.perf_counter()
        response = await call_next(request)
        if request.url.path not in ("/healthz", "/metrics"):
            log(logger, "request", method=request.method, path=request.url.path,
                status=response.status_code, ms=round((time.perf_counter() - t) * 1000, 2),
                request_id=rid)
        response.headers["x-request-id"] = rid
        return response

    def _err(e: Exception) -> HTTPException:
        if isinstance(e, NotFound):
            return HTTPException(404, str(e))
        if isinstance(e, NotRunning):
            return HTTPException(409, str(e))
        raise e

    @app.get("/healthz")
    def healthz() -> dict:
        engine._q("SELECT 1")
        return {"ok": True, "now": iso(engine.clock.now())}

    @app.get("/metrics", response_class=PlainTextResponse)
    def prom() -> str:
        return metrics.render(engine)

    @app.get("/api/types")
    def types() -> dict:
        return {"workflows": sorted(registry.workflows), "activities": sorted(registry.activities)}

    @app.post("/api/workflows", status_code=201)
    def start(req: StartRequest) -> dict:
        if req.workflow_type not in registry.workflows:
            raise HTTPException(400, f"unknown workflow type {req.workflow_type!r}")
        return engine.start_workflow(req.workflow_type, req.input, req.workflow_id)

    @app.get("/api/workflows")
    def list_(status: str | None = None, type: str | None = None, q: str | None = None,
              limit: int = 50, offset: int = 0, top_level: bool = False) -> dict:
        limit = max(1, min(limit, 500))
        out = engine.list_workflows(status, type, q, limit, offset, top_level)
        out["counts"] = engine.status_counts()
        out["now"] = engine.clock.now()
        return out

    @app.get("/api/workflows/{workflow_id:path}/history")
    def history(workflow_id: str, run_id: str | None = None) -> dict:
        try:
            run = engine.run(run_id) if run_id else engine.current_run(workflow_id)
        except Exception as e:
            raise _err(e) from e
        return {"run_id": run["run_id"], "events": engine.history(run["run_id"])}

    @app.post("/api/workflows/{workflow_id:path}/signal")
    def signal(workflow_id: str, req: SignalRequest) -> dict:
        try:
            seq = engine.signal(workflow_id, req.name, req.payload)
        except Exception as e:
            raise _err(e) from e
        return {"ok": True, "seq": seq}

    @app.post("/api/workflows/{workflow_id:path}/terminate")
    def terminate(workflow_id: str, req: TerminateRequest | None = None) -> dict:
        try:
            engine.terminate(workflow_id, (req or TerminateRequest()).reason)
        except Exception as e:
            raise _err(e) from e
        return {"ok": True}

    @app.get("/api/workflows/{workflow_id:path}")
    def describe(workflow_id: str) -> dict:
        try:
            d = engine.describe(workflow_id)
        except Exception as e:
            raise _err(e) from e
        d["effects"] = engine.side_effects(d["workflow_id"])
        d["now"] = engine.clock.now()
        return d

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    return app
