# etl/utils/database.py
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
from etl.config import ETLConfig

class PostgresManager:
    def __init__(self, config: ETLConfig):
        self.config = config
        self._pool = None
    
    @contextmanager
    def connection(self):
        conn = psycopg2.connect(self.config.db.connection_string)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    @contextmanager
    def cursor(self, cursor_factory=None):
        with self.connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()
    
    def execute_batch(self, query: str, values: List[tuple], page_size: int = 1000):
        """Execute batch insert with automatic commit"""
        with self.cursor() as cur:
            execute_values(cur, query, values, page_size=page_size)
    
    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        with self.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()
    
    def fetch_many(self, query: str, params: tuple = None) -> List[Dict]:
        with self.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def check_idempotency(self, entity_type: str, entity_key: str) -> bool:
        """Check if data was already processed"""
        import hashlib
        key_hash = hashlib.sha256(f"{entity_type}:{entity_key}".encode()).hexdigest()
        
        query = """
            INSERT INTO etl_idempotency_keys (key_hash, entity_type, entity_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (key_hash) DO NOTHING
            RETURNING key_hash;
        """
        with self.cursor() as cur:
            cur.execute(query, (key_hash, entity_type, entity_key))
            result = cur.fetchone()
            return result is not None  # True if new, False if duplicate