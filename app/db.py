import os
from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

# Bounded wait on checkout. psycopg_pool's default is 30s, which means one
# unreachable database host (classic case: Supabase's direct
# db.<ref>.supabase.co endpoint is IPv6-only, so an IPv4-only network never
# connects) makes EVERY endpoint hang for half a minute before failing. In
# the browser that reads as "the frontend doesn't respond to the backend".
# Fail fast instead, and let main.py turn it into a real 503 + JSON error.
POOL_TIMEOUT_SECONDS = float(os.getenv("DB_POOL_TIMEOUT", "8"))


def configure(conn):
    register_vector(conn)
    # See the matching comment in app/worker.py: Supabase's pooler is PgBouncer
    # in transaction mode, which is incompatible with psycopg3's server-side
    # prepared statements (statement-name collisions across pooled backend
    # sessions). These connections are long-lived and reused across many
    # requests, so this is worth disabling here too, not just in the worker.
    conn.prepare_threshold = None


pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=5,
    timeout=POOL_TIMEOUT_SECONDS,
    max_idle=300,
    configure=configure,
    check=ConnectionPool.check_connection,  # drop connections Supabase closed on us
    open=True,
)


def get_conn():
    return pool.connection()
