import os
import psycopg
from dotenv import load_dotenv
from rq import SimpleWorker
from pgvector.psycopg import register_vector
from datetime import datetime
import redis

load_dotenv()


def parse_serpapi_date(date_str):
    """SerpApi google_news dates look like: '07/22/2026, 07:00 AM, +0000 UTC'
    Strip the trailing timezone name and parse the rest. Falls back to None
    on anything unparseable so one bad date never kills the whole insert."""
    if not date_str:
        return None
    try:
        cleaned = date_str.replace(" UTC", "").strip()
        return datetime.strptime(cleaned, "%m/%d/%Y, %I:%M %p, %z")
    except (ValueError, TypeError):
        print(f"  [Warning] Could not parse date: {date_str}")
        return None


def store_embeddings(articles, embeddings, entity_id=1):
    """
    This function runs in the background worker.
    Takes articles + embeddings and stores them in Supabase.
    """
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL not in .env file")

    print(f"[Connecting] Connecting to Supabase...")
    conn = psycopg.connect(db_url)
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
            print(f"  [Inserted] Article {i+1}/{len(articles)}")

        conn.commit()
        print(f"[Success] Stored {len(articles)} articles in Supabase")

    except Exception as e:
        print(f"[Error] Failed to insert: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()


def start_worker():
    """
    Start the background worker process.
    Uses SimpleWorker (no fork) to avoid the macOS segfault that happens
    when RQ's default Worker forks a child process after numpy/Accelerate
    has already initialized native state.
    """
    redis_conn = redis.Redis.from_url(os.getenv('REDIS_URL'))
    worker = SimpleWorker(['default'], connection=redis_conn)

    print("\n" + "="*60)
    print("DRIFTWATCH BACKGROUND WORKER")
    print("="*60)
    print("Listening to Redis queue...")
    print("(Press Ctrl+C to stop)")
    print("="*60 + "\n")

    worker.work()


if __name__ == "__main__":
    start_worker()