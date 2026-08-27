from app.db import get_conn


def compute_centroid(entity_id: int):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT AVG(embedding) FROM statements WHERE entity_id = %s",
            (entity_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def score_all_statements(entity_id: int):
    centroid = compute_centroid(entity_id)
    if centroid is None:
        return []

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, raw_text, published_at, embedding <=> %s::vector AS drift_score
            FROM statements
            WHERE entity_id = %s
            ORDER BY published_at
            """,
            (centroid, entity_id),
        )
        return cur.fetchall()