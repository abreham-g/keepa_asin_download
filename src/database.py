import psycopg2
import logging
from typing import Dict, Optional
from src.config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.schema = config.DB_SCHEMA  # "keepa_scrape"
        self.table = config.DB_TABLE     # "downloaded_uk_asin_6m"
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
            # Create schema if not exists
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cur.execute(f"SET search_path TO {self.schema}")
            
            # Create main ASIN table in keepa_scrape schema
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
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
            
            # Create indexes in keepa_scrape schema
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table}_category ON {self.schema}.{self.table}(category_id)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table}_asin ON {self.schema}.{self.table}(asin)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table}_last_update ON {self.schema}.{self.table}(last_update)")
            
            # Create progress table in keepa_scrape schema
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.download_progress (
                    category_id TEXT PRIMARY KEY,
                    last_page INTEGER DEFAULT 0,
                    total_processed INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create batches table in keepa_scrape schema (optional, for tracking)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.download_batches (
                    id SERIAL PRIMARY KEY,
                    category_id TEXT,
                    page_number INTEGER,
                    asin_count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(category_id, page_number)
                )
            """)
            
            conn.commit()
            logger.info(f"✅ Database ready - Schema: {self.schema}")
            logger.info(f"   Main table: {self.schema}.{self.table}")
            logger.info(f"   Progress table: {self.schema}.download_progress")
            logger.info(f"   Batches table: {self.schema}.download_batches")
            
            # Get existing ASIN count
            cur.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.table}")
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
        """Insert single ASIN immediately to keepa_scrape.downloaded_uk_asin_6m"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"""
                INSERT INTO {self.schema}.{self.table} 
                (asin, category_id, title, brand, current_price, sales_rank, monthly_sold, rating, review_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (asin) DO UPDATE SET
                    category_id = EXCLUDED.category_id,
                    title = EXCLUDED.title,
                    brand = EXCLUDED.brand,
                    current_price = EXCLUDED.current_price,
                    sales_rank = EXCLUDED.sales_rank,
                    monthly_sold = EXCLUDED.monthly_sold,
                    rating = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count,
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
        """Get progress from keepa_scrape.download_progress"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"""
                SELECT last_page, total_processed 
                FROM {self.schema}.download_progress 
                WHERE category_id = %s
            """, (category_id,))
            row = cur.fetchone()
            return {'last_page': row[0], 'total_processed': row[1]} if row else {'last_page': 0, 'total_processed': 0}
        finally:
            cur.close()
    
    def update_progress(self, category_id: str, page: int, total_processed: int):
        """Update progress in keepa_scrape.download_progress"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"""
                INSERT INTO {self.schema}.download_progress (category_id, last_page, total_processed, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (category_id) DO UPDATE SET
                    last_page = EXCLUDED.last_page,
                    total_processed = EXCLUDED.total_processed,
                    updated_at = CURRENT_TIMESTAMP
            """, (category_id, page, total_processed))
            conn.commit()
        finally:
            cur.close()
    
    def record_batch(self, category_id: str, page: int, asin_count: int):
        """Record batch in keepa_scrape.download_batches"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"""
                INSERT INTO {self.schema}.download_batches (category_id, page_number, asin_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (category_id, page_number) DO NOTHING
            """, (category_id, page, asin_count))
            conn.commit()
        except Exception as e:
            logger.debug(f"Batch record error (non-critical): {e}")
        finally:
            cur.close()
    
    def get_total_count(self) -> int:
        """Get total ASINs from keepa_scrape.downloaded_uk_asin_6m"""
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.table}")
            return cur.fetchone()[0]
        finally:
            cur.close()
    
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()