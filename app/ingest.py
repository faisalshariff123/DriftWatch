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

    print(f"Searching: {search_query}")
    response = requests.get(url, params=params, timeout=10)

    if response.status_code != 200:
        print(f"Search failed (HTTP {response.status_code})")
        return []

    try:
        data = response.json()
        if data.get('error'):
            print(f"Search failed: {data['error']}")
            return []
        raw = data.get('news_results', [])
        articles = flatten_news_results(raw)
        print(f"Found {len(articles)} articles.")
        return articles
    except Exception as e:
        print(f"Could not read search results: {e}")
        return []


def flatten_news_results(raw_results):
    """google_news does not always return flat articles.

    Many queries come back as story CLUSTERS: an item with no title of its
    own, just a `highlight` object and a `stories` list. The old code read
    item['title'] directly, got '', and dropped the entire cluster - so a
    query whose results were mostly clusters ingested ZERO articles while
    still reporting success. Flatten clusters out, then dedupe by link.
    """
    flattened = []
    for item in raw_results or []:
        if not isinstance(item, dict):
            continue
        if item.get('title'):
            flattened.append(item)
        highlight = item.get('highlight')
        if isinstance(highlight, dict) and highlight.get('title'):
            flattened.append(highlight)
        for story in item.get('stories') or []:
            if isinstance(story, dict) and story.get('title'):
                flattened.append(story)

    seen = set()
    deduped = []
    for article in flattened:
        key = article.get('link') or article.get('title')
        if key and key not in seen:
            seen.add(key)
            deduped.append(article)
    return deduped


def extract_and_embed_articles(articles):
    """
    Step 2: Extract text from articles
    Step 3: Convert text to embeddings (384 numbers)

    valid_articles and texts are built together in lockstep so that an
    article with an empty title/snippet never causes the embedding array
    to drift out of alignment with the article list.
    """
    if not articles:
        return [], []

    valid_articles = []
    texts = []
    for article in articles:
        title = (article.get('title') or '').strip()
        snippet = (article.get('snippet') or '').strip()
        source = article.get('source') or {}
        source_name = (source.get('name') if isinstance(source, dict) else str(source)) or ''
        text = ' '.join(p for p in (title, snippet, source_name.strip()) if p).strip()
        if text:
            valid_articles.append(article)
            texts.append(text)

    if not texts:
        return [], []

    print(f"Analyzing {len(texts)} articles...")
    embeddings = model.encode(texts, show_progress_bar=False)

    return valid_articles, embeddings


def queue_ingestion_job(articles, embeddings, entity_id):
    """
    Step 4: Queue the job in Redis.
    Instead of immediately inserting to Supabase (slow, blocks the API),
    a job is placed in Redis. A background worker (worker.py) picks it
    up and does the actual insert.
    """
    if not articles or len(embeddings) == 0:
        return None

    return queue.enqueue(store_embeddings, articles, embeddings, entity_id)


def run_ingestion(search_query, entity_id, num_articles=10):
    """
    Main function: fetch -> embed -> queue for one search query.

    search_query and entity_id are REQUIRED, no defaults. This is
    intentional: if any caller forgets to pass one, this now throws a
    loud TypeError immediately instead of silently falling back to a
    stale hardcoded value and mis-attributing articles to the wrong
    entity.
    """
    articles = fetch_articles_from_serpapi(search_query, num_articles=num_articles)
    if not articles:
        print("No articles found.")
        return False

    articles, embeddings = extract_and_embed_articles(articles)
    if len(embeddings) == 0:
        print("Nothing usable in those articles.")
        return False

    job = queue_ingestion_job(articles, embeddings, entity_id)
    if not job:
        return False

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

    print("Backfill done.")


if __name__ == "__main__":
    # Explicit entity_id required now — 1 was Tesla in this project's
    # seeded data, adjust if you're backfilling a different entity.
    run_backfill(entity_id=1)