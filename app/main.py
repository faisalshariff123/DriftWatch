from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.db import get_conn
from app.scoring import score_all_statements

app = FastAPI()

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

@app.get("/drift/{entity_id}")
def get_drift(entity_id: int):
    results = score_all_statements(entity_id)
    return {
        "entity_id": entity_id,
        "timeline": [
            {"id": r[0], "text": r[1], "published_at": r[2], "drift_score": r[3]}
            for r in results
        ],
    }