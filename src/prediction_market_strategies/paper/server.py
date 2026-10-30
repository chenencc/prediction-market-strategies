"""Local read-only Paper terminal; no execution or account endpoints."""

import argparse
import asyncio
import json
from pathlib import Path
import re
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles


def create_app(root, static=None):
    root = Path(root).resolve()
    app = FastAPI(title="Paper Observatory", docs_url=None, redoc_url=None)

    def directory(run_id):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", run_id):
            raise HTTPException(400, "Invalid run ID")
        p = (root / run_id).resolve()
        if p.parent != root or not p.is_dir():
            raise HTTPException(404, "Run not found")
        return p

    def state(p):
        try:
            return json.loads((p / "monitor-state.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            raise HTTPException(404, "No complete observation yet")

    @app.middleware("http")
    async def local_only(request: Request, call_next):
        # Reject cross-origin browser access and DNS rebinding, even on loopback.
        host = request.headers.get("host", "").split(":")[0]
        origin = request.headers.get("origin")
        if host not in {"127.0.0.1", "localhost", "testserver"} or (
            origin
            and origin
            not in {
                f"http://{request.headers.get('host')}",
                f"https://{request.headers.get('host')}",
            }
        ):
            from fastapi.responses import JSONResponse

            return JSONResponse(
                {"error": "local same-origin access only"}, status_code=403
            )
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.get("/api/runs")
    def runs():
        result = []
        for p in root.iterdir():
            if (
                not p.is_dir()
                or p.is_symlink()
                or not (p / "monitor-state.json").is_file()
            ):
                continue
            try:
                s = state(p)
                result.append(
                    {
                        k: s.get(k)
                        for k in (
                            "run_id",
                            "status",
                            "source",
                            "slug",
                            "title",
                            "strategy",
                            "started_ns",
                            "published_ns",
                            "phase",
                            "account",
                        )
                    }
                )
            except (HTTPException, OSError):
                continue
        return sorted(result, key=lambda s: int(s.get("started_ns") or 0), reverse=True)

    @app.get("/api/runs/{run_id}/state")
    def current(run_id: str):
        return state(directory(run_id))

    def frames(p):
        result = []
        path = p / "monitor-history.jsonl"
        if not path.is_file():
            return result
        # Finite runs only. Incomplete trailing lines are never a snapshot.
        with path.open(encoding="utf-8") as f:
            for line in f:
                if not line.endswith("\n"):
                    break
                result.append(json.loads(line))
                if len(result) > 10_000:
                    raise HTTPException(413, "Run history exceeds bounded reader")
        return result

    @app.get("/api/runs/{run_id}/history")
    def history(run_id: str):
        values = frames(directory(run_id))
        return [
            dict(
                index=i,
                observed_ns=s.get("observed_ns"),
                equity=s.get("account", {}).get("equity"),
                books=[
                    dict(
                        outcome=b["outcome"],
                        fresh=b["fresh"],
                        bid=b["bids"][0][0] if b["bids"] else None,
                        ask=b["asks"][0][0] if b["asks"] else None,
                    )
                    for b in s.get("books", [])
                ],
            )
            for i, s in enumerate(values)
        ]

    @app.get("/api/runs/{run_id}/frames/{index}")
    def frame(run_id: str, index: int):
        values = frames(directory(run_id))
        if not 0 <= index < len(values):
            raise HTTPException(404, "Frame not found")
        return values[index]

    @app.get("/api/runs/{run_id}/stream")
    async def stream(run_id: str, request: Request):
        p = directory(run_id)

        async def updates():
            last = None
            while not await request.is_disconnected():
                try:
                    s = await asyncio.to_thread(state, p)
                    # Full replacement snapshots make reconnects idempotent.
                    key = (s.get("published_ns"), s.get("persisted_event_seq"))
                    if key != last:
                        yield "event: state\ndata: " + json.dumps(s) + "\n\n"
                        last = key
                    else:
                        yield ": heartbeat\n\n"
                except HTTPException:
                    yield "event: unavailable\ndata: {}\n\n"
                await asyncio.sleep(0.5)

        return StreamingResponse(
            updates(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )

    if static:
        static = Path(static).resolve()
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

        @app.get("/")
        def index():
            return FileResponse(static / "index.html")

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="results")
    parser.add_argument("--static", default="web/dist")
    parser.add_argument("--port", type=int, default=5190)
    args = parser.parse_args()
    import uvicorn

    uvicorn.run(
        create_app(args.root, args.static),
        host="127.0.0.1",
        port=args.port,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
