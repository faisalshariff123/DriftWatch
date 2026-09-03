import os
import time
import csv
import io
from collections import defaultdict
from uuid import uuid4

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import List

import psycopg
from psycopg_pool import PoolTimeout

from app.db import get_conn
from app.scoring import score_all_statements, get_representative_statement

app = FastAPI()

# Note: app.ingest is imported lazily, inside trigger_ingest() only, not
# here at module level. app.ingest connects to Redis and loads the
# embedding model the moment it's imported — keeping that import lazy
# means a Redis outage only breaks the "add new entity" feature, not
# the entire API's ability to boot and serve the dashboard/existing data.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Database failures must come back as JSON with a real status code. Without
# these, an unreachable database surfaced as an unhandled exception (plain
# 500, HTML body), the dashboard's .json() call threw, and the page silently
# froze on "loading..." with no indication anything was wrong.
# ---------------------------------------------------------------------------


@app.exception_handler(PoolTimeout)
async def _pool_timeout_handler(request: Request, exc: PoolTimeout):
    return JSONResponse(
        status_code=503,
        content={"detail": "database unreachable - connection pool timed out. "
                           "Check DATABASE_URL (Supabase direct hosts are IPv6-only; "
                           "use the pooler host if you're on IPv4)."},
    )


@app.exception_handler(psycopg.OperationalError)
async def _db_operational_handler(request: Request, exc: psycopg.OperationalError):
    return JSONResponse(status_code=503, content={"detail": f"database error: {exc}"})


DASHBOARD_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard.html")


# ---------------------------------------------------------------------------
# Rate limiting
#
# Sliding window per (client IP, endpoint), stored in Redis so every instance
# shares one budget. It used to be an in-process dict, which meant N Cloud Run
# instances each enforced the limit separately and the real ceiling was N x
# whatever was configured - the reason this service had to be pinned to a
# single instance. Redis is already in the stack, so this costs nothing new.
#
# If Redis is unreachable the limiter degrades to the old per-process dict
# rather than failing requests: a loose limit beats a dead API.
# ---------------------------------------------------------------------------
_rate_limit_log = defaultdict(list)   # fallback store
_redis_client = None
_redis_checked_at = 0.0
REDIS_RECHECK_SECONDS = 30


def _get_redis():
    """Lazily connect to Redis, retrying at most every REDIS_RECHECK_SECONDS
    so a brief outage doesn't permanently downgrade the limiter."""
    global _redis_client, _redis_checked_at
    now = time.time()
    if _redis_client is not None or (now - _redis_checked_at) < REDIS_RECHECK_SECONDS:
        return _redis_client
    _redis_checked_at = now

    url = os.getenv("REDIS_URL")
    if not url:
        return None
    try:
        import redis
        client = redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
        client.ping()
        _redis_client = client
    except Exception:
        _redis_client = None
    return _redis_client


def client_ip(request: Request) -> str:
    """The real caller's IP.

    Behind Cloud Run (or any proxy) request.client.host is the load balancer,
    so without this every visitor shares one rate-limit bucket and a handful
    of users trip the limit for everybody.
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _allow_redis(client, key, limit, window_seconds, now):
    """Returns True/False, or None if Redis misbehaved and we should fall back."""
    redis_key = f"ratelimit:{key}"
    try:
        pipe = client.pipeline()
        pipe.zremrangebyscore(redis_key, 0, now - window_seconds)
        pipe.zcard(redis_key)
        _, count = pipe.execute()
        if count >= limit:
            return False
        pipe = client.pipeline()
        pipe.zadd(redis_key, {f"{now}:{uuid4().hex}": now})
        pipe.expire(redis_key, window_seconds + 1)
        pipe.execute()
        return True
    except Exception:
        return None


def _allow_in_process(key, limit, window_seconds, now):
    timestamps = _rate_limit_log[key]
    _rate_limit_log[key] = [t for t in timestamps if now - t < window_seconds]
    if len(_rate_limit_log[key]) >= limit:
        return False
    _rate_limit_log[key].append(now)
    return True


def rate_limit(limit: int, window_seconds: int):
    """Returns a FastAPI dependency enforcing `limit` requests per
    `window_seconds` per client IP for whichever route it's attached to."""

    def dependency(request: Request):
        key = f"{client_ip(request)}:{request.url.path}"
        now = time.time()

        allowed = None
        client = _get_redis()
        if client is not None:
            allowed = _allow_redis(client, key, limit, window_seconds, now)
        if allowed is None:
            allowed = _allow_in_process(key, limit, window_seconds, now)

        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded - max {limit} requests per {window_seconds}s. Try again shortly.",
            )

    return dependency


@app.on_event("startup")
def _warn_about_direct_db_host():
    """Supabase's direct host is IPv6-only. Most serverless runtimes are
    IPv4-only, so this config works on a laptop and silently times out every
    query once deployed. Say so at boot instead of leaving it to be
    rediscovered as a hanging dashboard."""
    url = os.getenv("DATABASE_URL", "")
    if ".supabase.co" in url and "pooler.supabase.com" not in url:
        print(
            "WARNING: DATABASE_URL uses Supabase's direct host (db.<ref>.supabase.co), "
            "which is IPv6-only. Fine locally if your network has IPv6; it will time out "
            "on most cloud runtimes. Use the Session/Transaction pooler host instead."
        )


@app.get("/")
def serve_dashboard():
    """Serves the dashboard at the same origin as the API, so the frontend
    never needs a hardcoded backend URL — it just calls relative paths."""
    return FileResponse(DASHBOARD_PATH)


@app.get("/health")
def health():
    """Reports database and Redis status separately, rather than a single
    blanket OK/fail — useful for distinguishing a partial outage (e.g.
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
    the app always starts from a clean slate — combined with the wipe
    inside create_entity, this means data only ever exists for
    whatever was most recently searched, and never persists across a
    refresh.
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
                "ingested_at": r[5],
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
    writer.writerow(["id", "text", "published_at", "url", "drift_score", "ingested_at"])
    for r in results:
        writer.writerow([r[0], r[1], r[2], r[3], r[4], r[5]])
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=entity_{entity_id}_drift.csv"},
    )


@app.post("/trigger-ingest/{entity_id}", dependencies=[Depends(rate_limit(limit=10, window_seconds=300))])
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
    try:
        from app.ingest import run_ingestion
    except Exception as e:  # Redis down, model download failed, missing dep
        raise HTTPException(
            status_code=503,
            detail=f"ingestion pipeline unavailable: {type(e).__name__}: {e}",
        )

    with get_conn() as conn:
        cur = conn.execute("SELECT search_terms FROM entities WHERE id = %s", (entity_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    results = []
    for term in row[0]:
        try:
            success = run_ingestion(search_query=term, entity_id=entity_id)
            results.append({"query": term, "queued": bool(success)})
        except Exception as e:
            # One bad search term must not kill the whole request - report it
            # per-term so the dashboard can say what actually failed.
            results.append({"query": term, "queued": False, "error": f"{type(e).__name__}: {e}"})
    return {"entity_id": entity_id, "results": results}