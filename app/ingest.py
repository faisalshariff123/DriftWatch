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
    FIXED: builds valid_articles and texts together so they never drift
    out of sync when an article gets skipped for having no title/snippet.
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


def queue_ingestion_job(articles, embeddings, entity_id=1):
    if not articles or len(embeddings) == 0:
        print("[Warning] No data to queue")
        return None

    print(f"[Queueing] Queuing job to store {len(articles)} articles in Supabase...")
    job = queue.enqueue(store_embeddings, articles, embeddings, entity_id)
    print(f"[Queued] Job ID: {job.id} - Status: {job.get_status()}")
    return job


def run_ingestion(search_query="Tesla full self-driving", entity_id=1, num_articles=10):
    print("\n" + "="*60)
    print(f"DRIFTWATCH INGESTION PIPELINE — query: {search_query}")
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


def run_backfill(entity_id=1):
    """
    NEW: runs multiple searches spanning different time periods/framings
    of Tesla FSD news, so the demo has real spread instead of everything
    clustered on today's date. Run this once, then let worker.py process
    the queued jobs.
    """
    queries = [
        "Tesla full self-driving 2023",
        "Tesla FSD delay",
        "Tesla full self-driving 2024",
        "Tesla FSD robotaxi 2025",
        "Tesla full self-driving next year",
    ]

    for q in queries:
        run_ingestion(search_query=q, entity_id=entity_id, num_articles=6)
        time.sleep(2)  # small pause between SerpApi calls, be polite to rate limits

    print("\n[Backfill complete] Now start the worker to process all queued jobs:")
    print("python app/worker.py")


if __name__ == "__main__":
    run_backfill()