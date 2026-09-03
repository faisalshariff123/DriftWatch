CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS entities (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    search_terms TEXT[],
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS statements (
    id SERIAL PRIMARY KEY,
    entity_id INT REFERENCES entities(id),
    source_url TEXT,
    raw_text TEXT,
    embedding VECTOR(384),
    published_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS drift_scores (
    id SERIAL PRIMARY KEY,
    entity_id INT REFERENCES entities(id),
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    drift_score FLOAT,
    baseline_centroid VECTOR(384)
);