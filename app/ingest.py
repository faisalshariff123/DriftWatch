import os
import time
import requests
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from rq import Queue
import redis
from app.worker import store_embeddings

load_dotenv()

# Initialize once (expensive operation, don't repeat)
model = SentenceTransformer('all-MiniLM-L6-v2')
redis_conn = redis.Redis.from_url(os.getenv('REDIS_URL'))
queue = Queue(connection=redis_conn)


def fetch_articles_from_serpapi(search_query, num_articles=10):
    """
    Step 1: Call SerpApi to get real articles.
    Uses google_news engine specifically because it returns real publish
    dates, unlike the default organic search results.
    """
    api_key = os.getenv('SERPAPI_KEY')
    if not api_key:
        raise ValueError("SERPAPI_KEY not in .env file")

    url = "https://serpapi.com/search"
    params = {
        'q': search_query,
        'engine': 'google_news',
        'api_key': api_key,
        'num': num_articles
    }

    print(f"[Fetching] Calling SerpApi with query: {search_query}")
    response = requests.get(url, params=params, timeout=10)

    if response.status_code != 200:
        print(f"[Error] SerpApi returned status {response.status_code}")
        return []

    try:
        data = response.json()
        articles = data.get('news_results', [])
        print(f"[Success] Got {len(articles)} articles from SerpApi")
        return articles
    except Exception as e:
        print(f"[Error] Failed to parse response: {e}")
        return []


def extract_and_embed_articles(articles):
    """
    Step 2: Extract text from articles
    Step 3: Convert text to embeddings (384 numbers)

    valid_articles and texts are built together in lockstep so that an
    article with an empty title/snippet never causes the embedding array
    to drift out of alignment with the article list.
    """
    if not articles:
        print("[Warning] No articles to embed")
        return [], []

    valid_articles = []
    texts = []
    for article in articles:
        title = article.get('title', '').strip()
        snippet = article.get('snippet', '').strip()
        text = f"{title} {snippet}".strip()
        if text:
            valid_articles.append(article)
            texts.append(text)

    if not texts:
        print("[Warning] No valid text extracted from articles")
        return [], []

    print(f"[Embedding] Converting {len(texts)} texts to vectors...")
    embeddings = model.encode(texts, show_progress_bar=False)
    print(f"[Success] Created {len(embeddings)} embeddings of dimension {embeddings[0].shape}")

    return valid_articles, embeddings


def queue_ingestion_job(articles, embeddings, entity_id):
    """
    Step 4: Queue the job in Redis.
    Instead of immediately inserting to Supabase (slow, blocks the API),
    a job is placed in Redis. A background worker (worker.py) picks it
    up and does the actual insert.
    """
    if not articles or len(embeddings) == 0:
        print("[Warning] No data to queue")
        return None

    print(f"[Queueing] Queuing job to store {len(articles)} articles in Supabase...")
    job = queue.enqueue(store_embeddings, articles, embeddings, entity_id)
    print(f"[Queued] Job ID: {job.id} - Status: {job.get_status()}")
    return job


def run_ingestion(search_query, entity_id, num_articles=10):
    """
    Main function: fetch -> embed -> queue for one search query.

    search_query and entity_id are REQUIRED, no defaults. This is
    intentional: if any caller forgets to pass one, this now throws a
    loud TypeError immediately instead of silently falling back to a
    stale hardcoded value and mis-attributing articles to the wrong
    entity.
    """
    print("\n" + "="*60)
    print(f"DRIFTWATCH INGESTION PIPELINE — entity {entity_id} — query: {search_query}")
    print("="*60 + "\n")

    articles = fetch_articles_from_serpapi(search_query, num_articles=num_articles)
    if not articles:
        print("[Error] No articles fetched. Aborting.")
        return False

    articles, embeddings = extract_and_embed_articles(articles)
    if len(embeddings) == 0:
        print("[Error] Failed to embed articles. Aborting.")
        return False

    job = queue_ingestion_job(articles, embeddings, entity_id)
    if not job:
        print("[Error] Failed to queue job. Aborting.")
        return False

    print("\n" + "="*60)
    print("INGESTION QUEUED SUCCESSFULLY")
    print("="*60 + "\n")
    return True


def run_backfill(entity_id, queries=None):
    """
    Runs several searches spanning different time periods/framings of a
    topic, so a fresh entity gets real spread instead of everything
    clustered on today's date.

    entity_id is required. queries defaults to the original Tesla FSD
    set only if you don't pass your own — pass your own list for any
    other entity.
    """
    if queries is None:
        queries = [
            "Tesla full self-driving 2023",
            "Tesla FSD delay",
            "Tesla full self-driving 2024",
            "Tesla FSD robotaxi 2025",
            "Tesla full self-driving next year",
        ]

    for q in queries:
        run_ingestion(search_query=q, entity_id=entity_id, num_articles=6)
        time.sleep(2)  # be polite to SerpApi rate limits

    print("\n[Backfill complete] Now start the worker to process all queued jobs:")
    print("python3 -m app.worker")


if __name__ == "__main__":
    # Explicit entity_id required now — 1 was Tesla in this project's
    # seeded data, adjust if you're backfilling a different entity.
    run_backfill(entity_id=1)