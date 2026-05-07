import time
import logging
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)

class KeepaTokenManager:
    def __init__(self, max_tokens: int = 3900, refill_minutes: int = 30):
        self.max_tokens = max_tokens
        self.refill_seconds = refill_minutes * 60
        self.refill_rate = max_tokens / self.refill_seconds
        self.current_tokens = max_tokens
        self.last_refill_time = datetime.now()
        self.lock = Lock()
        self.request_count = 0
        logger.info(f"🎫 Token Manager: {max_tokens} tokens refill every {refill_minutes}min")
    
    def wait_for_tokens(self, amount: int = 1):
        """Wait for tokens without storing state"""
        while True:
            with self.lock:
                now = datetime.now()
                time_passed = (now - self.last_refill_time).total_seconds()
                if time_passed > 0:
                    refilled = int(time_passed * self.refill_rate)
                    if refilled > 0:
                        self.current_tokens = min(self.max_tokens, self.current_tokens + refilled)
                        self.last_refill_time = now
                
                if self.current_tokens >= amount:
                    self.current_tokens -= amount
                    self.request_count += 1
                    return
            
            # Wait outside the lock to avoid blocking
            wait_time = 5
            logger.info(f"⏳ Waiting {wait_time}s for token refill...")
            time.sleep(wait_time)