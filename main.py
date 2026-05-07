#!/usr/bin/env python3
import sys
import logging
import signal
from datetime import datetime
from src.config import config
from src.keepa_downloader import KeepaDownloader

def signal_handler(sig, frame):
    print("\n\n⚠️ Download interrupted. Progress saved in database.")
    print("Run 'python main.py' again to resume.")
    sys.exit(0)

def setup_logging():
    log_filename = f"logs/keepa_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def main():
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     Keepa UK ASIN Downloader - Memory Efficient          ║
    ║             6.9 Million Products Target                  ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    logger = setup_logging()
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        config.validate()
        downloader = KeepaDownloader()
        downloader.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()