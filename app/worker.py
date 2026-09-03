import os
import psycopg
from dotenv import load_dotenv
from rq import SimpleWorker
from pgvector.psycopg import register_vector
import re
from datetime import datetime, timedelta, timezone
import redis

load_dotenv()


_RELATIVE_RE = re.compile(
    r"^(?:about\s+)?(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago$", re.I
)

_UNIT_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
    "month": 2592000,
    "year": 31536000,
}

_DATE_FORMATS = (
    "%m/%d/%Y, %I:%M %p, %z",
    "%m/%d/%Y, %I:%M %p",
    "%m/%d/%Y",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%b %d, %Y",
    "%d %b %Y",
)


def parse_serpapi_date(date_str):
    """Parse whatever SerpApi's google_news engine hands back.

    It is NOT one format. Depending on the result you get an absolute stamp
    ('07/22/2026, 07:00 AM, +0000 UTC'), a relative one ('3 hours ago'), a
    bare date, or an ISO string. The old version only handled the first, so
    most rows landed with published_at = NULL - and the dashboard then threw
    every one of those rows away, showing "no data" over a full database.
    Returns None only when nothing at all matches.
    """
    if not date_str:
        return None

    s = str(date_str).strip()
    now = datetime.now(timezone.utc)

    m = _RELATIVE_RE.match(s)
    if m:
        return now - timedelta(seconds=int(m.group(1)) * _UNIT_SECONDS[m.group(2).lower()])

    low = s.lower()
    if low in ("just now", "today", "now"):
        return now
    if low == "yesterday":
        return now - timedelta(days=1)

    cleaned = re.sub(r"\s+(UTC|GMT)$", "", s).strip()
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        pass

    return None


def store_embeddings(articles, embeddings, entity_id=1):
    """
    This function runs in the background worker.
    Takes articles + embeddings and stores them in Supabase.
    """
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL not in .env file")

    # prepare_threshold=None: Supabase's pooler (port 6543) is PgBouncer in
    # transaction mode. It hands out backend sessions round-robin, and psycopg3
    # auto-prepares repeated statements (like this INSERT, run once per article)
    # after a few executions, naming them deterministically ("_pg3_0", ...). A
    # later connection can get handed a backend session that already has a
    # statement by that same name from a DIFFERENT client - collision, then
    # "prepared statement already exists". Disabling prepare on this connection
    # avoids it entirely; the cost (re-parsing each INSERT) is negligible here.
    conn = psycopg.connect(db_url, prepare_threshold=None)
    register_vector(conn)  # required so psycopg knows how to send the VECTOR type
    cur = conn.cursor()

    try:
        for i, article in enumerate(articles):
            raw_text = f"{article.get('title', '')} {article.get('snippet', '')}"
            source_url = article.get('link', '')
            published_at = parse_serpapi_date(article.get('date'))
            embedding = embeddings[i].tolist()  # numpy array -> list

            cur.execute(
                """INSERT INTO statements (entity_id, source_url, raw_text, embedding, published_at)
                   VALUES (%s, %s, %s, %s, %s)""",
                (entity_id, source_url, raw_text, embedding, published_at)
            )

        conn.commit()
        print(f"Saved {len(articles)} articles.")

    except Exception as e:
        print(f"Could not save articles: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()


def _serve_health_port():
    """Cloud Run rejects any service that doesn't listen on $PORT, and an RQ
    worker has no HTTP surface of its own. Bind a trivial health endpoint on a
    background thread so the worker deploys as a normal service; the queue loop
    still runs in the foreground. No-op locally, where PORT isn't set.
    """
    port = os.getenv("PORT")
    if not port:
        return

    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Health(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", int(port)), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def start_worker():
    """
    Start the background worker process.
    Uses SimpleWorker (no fork) to avoid the macOS segfault that happens
    when RQ's default Worker forks a child process after numpy/Accelerate
    has already initialized native state.
    """
    _serve_health_port()

    redis_conn = redis.Redis.from_url(os.getenv('REDIS_URL'))
    worker = SimpleWorker(['default'], connection=redis_conn)

    print("Driftwatch worker running. Ctrl+C to stop.")

    worker.work()


if __name__ == "__main__":
    start_worker()