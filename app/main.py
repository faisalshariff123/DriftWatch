import os
import time
import csv
import io
from collections import defaultdict

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import List

from app.db import get_conn
from app.scoring import score_all_statements, get_representative_statement

app = FastAPI()



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DASHBOARD_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard.html")



_rate_limit_log = defaultdict(list)


def rate_limit(limit: int, window_seconds: int):
    """Returns a FastAPI dependency enforcing `limit` requests per
    `window_seconds` per client IP for whichever route it's attached to."""

    def dependency(request: Request):
        ip = request.client.host if request.client else "unknown"
        key = (ip, request.url.path)
        now = time.time()
        timestamps = _rate_limit_log[key]
        _rate_limit_log[key] = [t for t in timestamps if now - t < window_seconds]
        if len(_rate_limit_log[key]) >= limit:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded — max {limit} requests per {window_seconds}s. Try again shortly.",
            )
        _rate_limit_log[key].append(now)

    return dependency


@app.get("/")
def serve_dashboard():
    """Serves the dashboard at the same origin as the API, so the frontend
    never needs a hardcoded backend URL — it just calls relative paths."""
    return FileResponse(DASHBOARD_PATH)


@app.get("/health")
def health():
    """Reports database and Redis status separately, rather than a single
    blanket OK/fai l useful for distinguishing a partial outage (e.g.
    ingestion broken, but existing dashboard data still fully viewable)
    from a total one."""
    db_ok = True
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1")
    except Exception:
        db_ok = False

    redis_ok = True
    try:
        import redis
        r = redis.Redis.from_url(os.environ["REDIS_URL"])
        r.ping()
    except Exception:
        redis_ok = False

    overall = "ok" if (db_ok and redis_ok) else "degraded"
    return {
        "status": overall,
        "database": "ok" if db_ok else "unreachable",
        "redis": "ok" if redis_ok else "unreachable",
    }


@app.post("/reset", dependencies=[Depends(rate_limit(limit=10, window_seconds=60))])
def reset_all():
    """
    Wipes every statement and entity. Called on dashboard page load so
    the app always starts from a clean slate combined with the wipe
    inside create_entity, this means data only ever exists for
    whatever was most recently searched, and never persists across a
    refresh. this is so that noone else can see what u searched.
    """
    with get_conn() as conn:
        conn.execute("DELETE FROM statements")
        conn.execute("DELETE FROM entities")
        conn.commit()
    return {"status": "cleared"}


@app.get("/entities")
def get_entities():
    with get_conn() as conn:
        cur = conn.execute("SELECT id, name FROM entities")
        rows = cur.fetchall()
    return {"entities": [{"id": r[0], "name": r[1]} for r in rows]}


class NewEntity(BaseModel):
    name: str
    search_terms: List[str]


@app.post("/entities", dependencies=[Depends(rate_limit(limit=5, window_seconds=60))])
def create_entity(payload: NewEntity):
    """
    True wipe-and-replace: only ONE entity ever exists, period. No
    protected/permanent entity, no entity_type distinction. Adding a
    new one deletes EVERYTHING that existed before it.
    """
    with get_conn() as conn:
        conn.execute("DELETE FROM statements")
        conn.execute("DELETE FROM entities")

        cur = conn.execute(
            "INSERT INTO entities (name, entity_type, search_terms) VALUES (%s, %s, %s) RETURNING id",
            (payload.name, "custom", payload.search_terms),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
    return {"id": new_id, "name": payload.name}


@app.get("/drift/{entity_id}")
def get_drift(entity_id: int):
    results = score_all_statements(entity_id)
    return {
        "entity_id": entity_id,
        "timeline": [
            {
                "id": r[0],
                "text": r[1],
                "published_at": r[2],
                "url": r[3],
                "drift_score": r[4],
            }
            for r in results
        ],
    }


@app.get("/baseline/{entity_id}")
def get_baseline(entity_id: int):
    row = get_representative_statement(entity_id)
    if row is None:
        return {"entity_id": entity_id, "baseline_text": None}
    return {"entity_id": entity_id, "baseline_text": row[0], "published_at": row[1]}


@app.get("/export/{entity_id}")
def export_csv(entity_id: int):
    """Downloads the full scored timeline for an entity as a CSV — lets
    anyone (an analyst, a judge, a curious recruiter) grab the raw data
    behind the chart instead of only viewing it in the dashboard."""
    results = score_all_statements(entity_id)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "text", "published_at", "url", "drift_score"])
    for r in results:
        writer.writerow([r[0], r[1], r[2], r[3], r[4]])
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=entity_{entity_id}_drift.csv"},
    )


@app.post("/trigger-ingest/{entity_id}", dependencies=[Depends(rate_limit(limit=3, window_seconds=600))])
def trigger_ingest(entity_id: int):
    """
    Fetches + queues real articles for an existing entity's search terms.
    Calls SerpApi for real (spends quota) — rate limited more strictly
    than other endpoints (3 per 10 minutes per IP) specifically because
    SerpApi's free tier is a scarce, shared resource across every visitor
    to this deployed demo.

    run_ingestion is imported here, not at module level, so that if
    Redis is temporarily unreachable, only this endpoint fails — the
    rest of the API (dashboard, existing drift data) stays up.
    """
    from app.ingest import run_ingestion

    with get_conn() as conn:
        cur = conn.execute("SELECT search_terms FROM entities WHERE id = %s", (entity_id,))
        row = cur.fetchone()
    if not row:
        return {"error": "entity not found"}

    results = []
    for term in row[0]:
        success = run_ingestion(search_query=term, entity_id=entity_id)
        results.append({"query": term, "queued": success})
    return {"entity_id": entity_id, "results": results}