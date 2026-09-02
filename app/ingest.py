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
    Step 1: Call SerpApi to get real articles
    
    Input: search_query = "Tesla full self-driving"
    Output: list of articles with title, snippet, date, link
    """
    api_key = os.getenv('SERPAPI_KEY')
    
    if not api_key:
        raise ValueError("SERPAPI_KEY not in .env file")
    
    url = "https://serpapi.com/search"
    params = {
        'q': search_query,
        'engine': 'google_news',  # ← Use Google News (has publish dates)
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
    
    Input: articles = [{title: "...", snippet: "...", ...}, ...]
    Output: (articles, embeddings) where embeddings are 384-dim vectors
    """
    if not articles:
        print("[Warning] No articles to embed")
        return [], []
    
    # Extract text from each article
    texts = []
    for article in articles:
        title = article.get('title', '').strip()
        snippet = article.get('snippet', '').strip()
        text = f"{title} {snippet}".strip()
        if text:
            texts.append(text)
    
    if not texts:
        print("[Warning] No valid text extracted from articles")
        return articles[:len(texts)], []
    
    print(f"[Embedding] Converting {len(texts)} texts to vectors...")
    
    # Batch encode (all at once, not one-by-one)
    embeddings = model.encode(texts, show_progress_bar=False)
    
    print(f"[Success] Created {len(embeddings)} embeddings of dimension {embeddings[0].shape}")
    
    return articles[:len(texts)], embeddings


def queue_ingestion_job(articles, embeddings, entity_id=1):
    """
    Step 4: Queue the job in Redis
    
    Instead of immediately inserting to Supabase (slow, blocks API),
    we put a job in Redis. A background worker will pick it up and process it.
    """
    if not articles or len(embeddings) == 0:
        print("[Warning] No data to queue")
        return None
    
    print(f"[Queueing] Queuing job to store {len(articles)} articles in Supabase...")
    
    # Enqueue the job (doesn't run yet, just adds to Redis queue)
    job = queue.enqueue(
        store_embeddings,  # Function to run
        articles,          # Argument 1
        embeddings,        # Argument 2
        entity_id          # Argument 3
    )
    
    print(f"[Queued] Job ID: {job.id} - Status: {job.get_status()}")
    return job


def run_ingestion(search_query="Tesla full self-driving", entity_id=1):
    """
    Main function: Orchestrate the entire ingestion flow
    
    This is what you call to ingest data:
    1. Fetch articles from SerpApi
    2. Extract and embed them
    3. Queue job to store them
    """
    print("\n" + "="*60)
    print("DRIFTWATCH INGESTION PIPELINE")
    print("="*60 + "\n")
    
    # Step 1: Fetch
    articles = fetch_articles_from_serpapi(search_query, num_articles=5)
    if not articles:
        print("[Error] No articles fetched. Aborting.")
        return False
    
    # Step 2 & 3: Extract and embed
    articles, embeddings = extract_and_embed_articles(articles)
    if len(embeddings) == 0:
        print("[Error] Failed to embed articles. Aborting.")
        return False
    
    # Step 4: Queue job
    job = queue_ingestion_job(articles, embeddings, entity_id)
    if not job:
        print("[Error] Failed to queue job. Aborting.")
        return False
    
    print("\n" + "="*60)
    print("INGESTION QUEUED SUCCESSFULLY")
    print(f"Job will be processed by background worker")
    print("="*60 + "\n")
    
    return True


if __name__ == "__main__":
    # Test ingestion manually
    success = run_ingestion("Tesla full self-driving")
    if success:
        print("\n[Next] Start the worker: python app/worker.py")