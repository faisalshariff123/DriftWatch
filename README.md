# DriftWatch

Tracks how a company or public figure's public messaging drifts over time. Pulls real news coverage, embeds each statement, and scores every article by how far it's strayed from that subject's own baseline — so "Tesla FSD is basically here" (2023) and "FSD is still a couple years out" (2025) show up as a measurable break in the line, not just a vibe.

## Demo

![DriftWatch demo](docs/demo.gif)

*(Recording: search a subject, watch articles get pulled, embedded, and scored live on the chart.)*

## How it works

1. You type in a subject and some search terms ("Nvidia", "nvidia earnings, nvidia AI chips").
2. The backend calls SerpApi's Google News engine for real articles with real publish dates.
3. Each article gets embedded with `sentence-transformers` (`all-MiniLM-L6-v2`, 384-dim) and stored in Postgres via `pgvector`.
4. A background worker computes the subject's centroid — the "average" of everything they've said — and scores every article by cosine distance from it. Far from centroid = high drift.
5. The dashboard (Chart.js) plots the timeline, flags the 8 most recent entries, and puts the oldest and newest statements side by side so you can read the actual shift in language, not just look at a number.

## Stack

FastAPI + Postgres/pgvector (Supabase) + Redis/RQ (Upstash) for the ingest queue + `sentence-transformers` for embeddings + a single-file HTML/Chart.js dashboard, no frontend build step.

## Running it locally

**1. Install dependencies** (Python 3.10+):

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

**2. Set up `.env`** — copy `.env.example` to `.env` and fill in:

- `DATABASE_URL` — a Postgres connection string with the `vector` extension available (Supabase's pooler connection works; see `.env.example` for the gotcha around their direct host being IPv6-only)
- `REDIS_URL` — any Redis instance (Upstash's free tier works)
- `SERPAPI_KEY` — from [serpapi.com](https://serpapi.com), used for the Google News search

**3. Apply the schema** — run `migrations/001.init.sql` against your database once (Supabase: paste it into the SQL Editor; anywhere else: `psql "$DATABASE_URL" -f migrations/001.init.sql`).

**4. Run the API and the worker** — two separate processes, both need to be running:

```bash
# terminal 1
.venv/bin/python -m uvicorn app.main:app --port 8000

# terminal 2
.venv/bin/python -m app.worker
```

**5. Open** `http://127.0.0.1:8000` and log a subject.

Note: the dashboard wipes its data on every page load (`/reset`) by design — it's meant to show one subject's drift at a time, not accumulate a database of tracked entities.

## Project structure

```
app/
  main.py      FastAPI routes, rate limiting, error handling
  ingest.py    SerpApi fetch + embedding
  worker.py    background job: stores embeddings, parses dates
  scoring.py   centroid + drift score computation
  db.py        Postgres connection pool
dashboard.html the entire frontend, single file
migrations/    schema
```
