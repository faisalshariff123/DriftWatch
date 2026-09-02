import os
from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

def configure(conn):
    register_vector(conn)

pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, configure=configure)

def get_conn():
    return pool.connection()