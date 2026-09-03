import time
from app.db import get_conn

_centroid_cache = {}  # entity_id -> (cached_at_timestamp, centroid)
CENTROID_CACHE_TTL_SECONDS = 5


def compute_centroid(entity_id: int):
    """Average all stored embeddings for this entity into one baseline vector.

    Cached briefly per entity — the dashboard's polling loop (waitForData)
    hits /drift every 2 seconds while waiting for ingestion to finish, and
    each call recomputes this AVG() aggregate over every stored row unless
    cached. A "no data yet" result is deliberately never cached, so a
    real centroid becomes visible the moment it exists, not up to
    CENTROID_CACHE_TTL_SECONDS late.
    """
    now = time.time()
    cached = _centroid_cache.get(entity_id)
    if cached and now - cached[0] < CENTROID_CACHE_TTL_SECONDS:
        return cached[1]

    with get_conn() as conn:
        cur = conn.execute(
            "SELECT AVG(embedding) FROM statements WHERE entity_id = %s",
            (entity_id,),
        )
        row = cur.fetchone()
        centroid = row[0] if row else None

    if centroid is not None:
        _centroid_cache[entity_id] = (now, centroid)
    return centroid


def score_all_statements(entity_id: int):
    """Score every stored statement for this entity against its own centroid.
    Returns id, text, published_at, source_url, drift_score, ingested_at per row.

    ingested_at is returned so the frontend has a fallback timestamp: SerpApi
    hands back plenty of dates the worker can't parse, and rows with a NULL
    published_at used to be discarded client-side, making a populated database
    render as "no data"."""
    centroid = compute_centroid(entity_id)
    if centroid is None:
        return []

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, raw_text, published_at, source_url,
                   embedding <=> %s::vector AS drift_score,
                   ingested_at
            FROM statements
            WHERE entity_id = %s
            ORDER BY COALESCE(published_at, ingested_at)
            """,
            (centroid, entity_id),
        )
        return cur.fetchall()


def get_representative_statement(entity_id: int):
    """Returns the single stored statement closest to the centroid —
    i.e. the most 'typical' example of what this entity usually says."""
    centroid = compute_centroid(entity_id)
    if centroid is None:
        return None

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT raw_text, published_at, embedding <=> %s::vector AS distance
            FROM statements
            WHERE entity_id = %s
            ORDER BY distance ASC
            LIMIT 1
            """,
            (centroid, entity_id),
        )
        return cur.fetchone()