import psycopg2
import logging
from typing import Dict, Optional
from src.config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.schema = config.DB_SCHEMA
        self.table = config.DB_TABLE
        self.conn = None
        
    def get_connection(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(config.DATABASE_URL)
            self.conn.autocommit = False
        return self.conn
    
    def init_database(self):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cur.execute(f"SET search_path TO {self.schema}")
            
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    asin VARCHAR(10) PRIMARY KEY,
                    category_id TEXT,
                    title TEXT,
                    brand VARCHAR(255),
                    current_price FLOAT,
                    sales_rank INTEGER,
                    monthly_sold INTEGER,
                    rating FLOAT,
                    review_count INTEGER,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_category ON {self.table}(category_id)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_asin ON {self.table}(asin)")
            
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.download_progress (
                    category_id TEXT PRIMARY KEY,
                    last_page INTEGER DEFAULT 0,
                    total_processed INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            logger.info(f"✅ Database ready: {self.schema}.{self.table}")
            
            cur.execute(f"SELECT COUNT(*) FROM {self.table}")
            count = cur.fetchone()[0]
            logger.info(f"📊 Existing ASINs in database: {count:,}")
            return count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Database init error: {e}")
            raise
        finally:
            cur.close()
    
    def insert_asin(self, product: Dict):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"SET search_path TO {self.schema}")
            cur.execute(f"""
                INSERT INTO {self.table} 
                (asin, category_id, title, brand, current_price, sales_rank, monthly_sold, rating, review_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (asin) DO UPDATE SET
                    category_id = EXCLUDED.category_id,
                    last_update = CURRENT_TIMESTAMP
            """, (
                product['asin'], product['category_id'], product['title'], product['brand'],
                product['current_price'], product['sales_rank'], product['monthly_sold'],
                product['rating'], product['review_count']
            ))
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Insert error for {product['asin']}: {e}")
            return False
        finally:
            cur.close()
    
    def get_progress(self, category_id: str) -> Dict:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"SET search_path TO {self.schema}")
            cur.execute("SELECT last_page, total_processed FROM download_progress WHERE category_id = %s", (category_id,))
            row = cur.fetchone()
            return {'last_page': row[0], 'total_processed': row[1]} if row else {'last_page': 0, 'total_processed': 0}
        finally:
            cur.close()
    
    def update_progress(self, category_id: str, page: int, total_processed: int):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"SET search_path TO {self.schema}")
            cur.execute("""
                INSERT INTO download_progress (category_id, last_page, total_processed, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (category_id) DO UPDATE SET
                    last_page = EXCLUDED.last_page,
                    total_processed = EXCLUDED.total_processed,
                    updated_at = CURRENT_TIMESTAMP
            """, (category_id, page, total_processed))
            conn.commit()
        finally:
            cur.close()
    
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()