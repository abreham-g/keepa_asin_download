import requests
import time
import logging
from datetime import datetime
from src.config import config
from src.token_manager import KeepaTokenManager
from src.database import DatabaseManager

logger = logging.getLogger(__name__)

class KeepaDownloader:
    def __init__(self):
        self.api_key = config.KEEPA_API_KEY
        self.domain = config.KEEPA_DOMAIN
        self.base_url = "https://api.keepa.com/product"
        self.token_manager = KeepaTokenManager(config.TOKENS_PER_REFILL, config.REFILL_INTERVAL_MINUTES)
        self.db = DatabaseManager()
        self.total_inserted = 0
        self.start_time = datetime.now()
        
    def fetch_and_insert_page(self, category_id: str, page: int) -> int:
        """Fetch one page and immediately insert each ASIN to database"""
        params = {
            'key': self.api_key,
            'domain': self.domain,
            'category': category_id,
            'page': page,
            'perPage': config.PAGE_SIZE,
            'stats': 30,
            'buybox': 1,
        }
        
        # Wait for token
        self.token_manager.wait_for_tokens(1)
        
        # Make request with retries
        for attempt in range(config.MAX_RETRIES):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data.get('error'):
                    logger.error(f"API Error: {data['error']}")
                    time.sleep(5)
                    continue
                
                products = data.get('products', [])
                inserted_count = 0
                
                # Process each product and insert immediately
                for product in products:
                    if product and 'asin' in product:
                        # Extract product data
                        product_data = {
                            'asin': product['asin'],
                            'category_id': category_id,
                            'title': product.get('title', '')[:500],
                            'brand': product.get('brand', '')[:255],
                            'current_price': product.get('price', [0])[0] / 100 if product.get('price') else None,
                            'sales_rank': product.get('salesRank', [0])[0] if product.get('salesRank') else None,
                            'monthly_sold': product.get('monthlySold'),
                            'rating': product.get('rating'),
                            'review_count': product.get('reviewCount'),
                        }
                        
                        # Insert immediately to database (no memory storage)
                        if self.db.insert_asin(product_data):
                            inserted_count += 1
                            self.total_inserted += 1
                
                # Record batch if we got any ASINs
                if inserted_count > 0:
                    self.db.record_batch(category_id, page, inserted_count)
                
                logger.info(f"Page {page}: Inserted {inserted_count} new ASINs")
                return inserted_count
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt+1}/{config.MAX_RETRIES}): {e}")
                time.sleep(5 * (attempt + 1))
            except Exception as e:
                logger.error(f"Unexpected error (attempt {attempt+1}): {e}")
                time.sleep(5)
        
        return 0
    
    def run(self):
        """Main download loop"""
        logger.info("🚀 Starting Keepa downloader...")
        logger.info(f"📁 All data will be stored in schema: {self.db.schema}")
        
        # Initialize database and get existing count
        existing_count = self.db.init_database()
        self.total_inserted = existing_count
        target_total = 6900000
        
        logger.info(f"🎯 Target: {target_total:,} ASINs")
        logger.info(f"📊 Already have: {self.total_inserted:,} ASINs")
        logger.info(f"📈 Remaining: {target_total - self.total_inserted:,} ASINs")
        
        # Process each category
        for category in config.CATEGORIES:
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 Processing category: {category}")
            logger.info(f"{'='*60}")
            
            # Get progress for this category
            progress = self.db.get_progress(category)
            page = progress['last_page']
            total_in_category = progress['total_processed']
            
            logger.info(f"📌 Resuming from page {page}")
            logger.info(f"📊 Already have {total_in_category:,} ASINs in this category")
            
            consecutive_empty = 0
            
            while consecutive_empty < 10:  # Stop after 10 empty pages
                # Fetch and insert page
                inserted = self.fetch_and_insert_page(category, page)
                
                if inserted == 0:
                    consecutive_empty += 1
                    logger.info(f"📭 Page {page}: 0 inserts. Empty count: {consecutive_empty}/10")
                    
                    if consecutive_empty >= 10:
                        logger.info(f"🏁 No more products for category {category}")
                        break
                else:
                    consecutive_empty = 0
                    total_in_category += inserted
                    
                    # Update progress after each page
                    self.db.update_progress(category, page, total_in_category)
                    
                    # Calculate statistics
                    elapsed_seconds = (datetime.now() - self.start_time).total_seconds()
                    elapsed_hours = elapsed_seconds / 3600
                    progress_pct = (self.total_inserted / target_total) * 100
                    speed = self.total_inserted / elapsed_seconds if elapsed_seconds > 0 else 0
                    
                    # Calculate ETA
                    remaining = target_total - self.total_inserted
                    eta_seconds = remaining / speed if speed > 0 else 0
                    eta_hours = eta_seconds / 3600
                    
                    logger.info(f"✅ Page {page} complete - Inserted {inserted} ASINs")
                    logger.info(f"📊 Total: {self.total_inserted:,} / {target_total:,} ({progress_pct:.2f}%)")
                    logger.info(f"⚡ Speed: {speed:.1f} ASINs/sec")
                    logger.info(f"⏱️  Time: {elapsed_hours:.1f}h | ETA: {eta_hours:.1f}h remaining")
                
                page += 1
                time.sleep(config.REQUEST_DELAY_SECONDS)
            
            logger.info(f"✅ Completed category {category}. Total ASINs in this category: {total_in_category:,}")
        
        # Final statistics
        elapsed_seconds = (datetime.now() - self.start_time).total_seconds()
        elapsed_hours = elapsed_seconds / 3600
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🎉 DOWNLOAD COMPLETE!")
        logger.info(f"{'='*60}")
        logger.info(f"📊 Total ASINs inserted: {self.total_inserted:,}")
        logger.info(f"⏱️  Total time: {elapsed_hours:.2f} hours")
        logger.info(f"⚡ Average speed: {self.total_inserted / elapsed_seconds:.1f} ASINs/sec")
        logger.info(f"💾 Database schema: {self.db.schema}")
        logger.info(f"📁 Main table: {self.db.schema}.{self.db.table}")
        logger.info(f"{'='*60}")
        
        # Close database connection
        self.db.close()