from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

from app.db import get_conn
from app.scoring import score_all_statements, get_representative_statement
from app.ingest import run_ingestion

app = FastAPI()

# Note: importing app.ingest above also loads the sentence-transformer model
# and connects to Redis at startup, so uvicorn will take a few extra seconds
# to become ready. That's expected, not a bug.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    with get_conn() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/entities")
def get_entities():
    with get_conn() as conn:
        cur = conn.execute("SELECT id, name FROM entities")
        rows = cur.fetchall()
    return {"entities": [{"id": r[0], "name": r[1]} for r in rows]}


class NewEntity(BaseModel):
    name: str
    search_terms: List[str]


@app.post("/entities")
def create_entity(payload: NewEntity):
    with get_conn() as conn:
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


@app.post("/trigger-ingest/{entity_id}")
def trigger_ingest(entity_id: int):
    """
    Fetches + queues real articles for an existing entity's search terms.
    Calls SerpApi for real (spends quota). The worker process must already
    be running separately to actually process the queued jobs.
    """
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