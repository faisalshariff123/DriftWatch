import os
import psycopg
from dotenv import load_dotenv
from rq import Worker
from pgvector.psycopg import register_vector
import redis

load_dotenv()


def store_embeddings(articles, embeddings, entity_id=1):
    """
    This function runs in the background worker.

    Takes articles + embeddings and stores them in Supabase.
    """
    db_url = os.getenv('DATABASE_URL')

    if not db_url:
        raise ValueError("DATABASE_URL not in .env file")

    # Connect to Supabase
    print(f"[Connecting] Connecting to Supabase...")
    conn = psycopg.connect(db_url)
    register_vector(conn)  # required so psycopg knows how to send the VECTOR type
    cur = conn.cursor()

    try:
        # Insert each article + embedding into Supabase
        for i, article in enumerate(articles):
            raw_text = f"{article.get('title', '')} {article.get('snippet', '')}"
            source_url = article.get('link', '')
            published_at = article.get('date', None)
            embedding = embeddings[i].tolist()  # Convert numpy array to list

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

    This runs forever, listening to the Redis queue.
    When a job arrives, it runs the function.
    """
    redis_conn = redis.Redis.from_url(os.getenv('REDIS_URL'))
    worker = Worker(['default'], connection=redis_conn)

    print("\n" + "="*60)
    print("DRIFTWATCH BACKGROUND WORKER")
    print("="*60)
    print("Listening to Redis queue...")
    print("(Press Ctrl+C to stop)")
    print("="*60 + "\n")

    worker.work()


if __name__ == "__main__":
    start_worker()