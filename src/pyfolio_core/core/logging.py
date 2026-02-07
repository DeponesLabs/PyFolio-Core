import os
import sys
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def setup_global_logging():

    root_logger = logging.getLogger() 
    root_logger.setLevel(logging.INFO)
        
    if not root_logger.handlers:
        """STDOUT to Screen"""
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
        c_handler.setFormatter(c_format)
        root_logger.addHandler(c_handler)

        """STDERR to File"""
        f_handler = logging.FileHandler('error.log', encoding='utf-8')
        f_handler.setLevel(logging.ERROR)
        f_format = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        f_handler.setFormatter(f_format)
        root_logger.addHandler(f_handler)
        
        print("Global Logging is running... (Root Logger)")
        
class ScanReporter:
    
    def __init__(self, output_dir="logs"):
        
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def log_failures(self, exchange: str, failed_symbols: list[str]) -> None:

        if not failed_symbols:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"failed_exchange_symbols_{exchange}_{timestamp}.log"
        file_path = os.path.join(self.output_dir, filename)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(f"Exchange: {exchange}\n")
                f.write(f"Date: {timestamp}\n")
                f.write(f"Total Failed: {len(failed_symbols)}\n")
                f.write("-" * 30 + "\n")
                for s in failed_symbols:
                    f.write(f"{s}\n")
            
            logger.error(f"SCAN COMPLETED WITH ERRORS. Report saved: {file_path}")

        except Exception as e:
            sys.stderr.write(f"[CRITICAL] Rapor dosyasi olusturulamadi: {e}\n")
            logger.critical(f"Failed to write failure report! Error: {e}")
            
    def log_fetch_success(self, exchange: str, data: dict) -> None:
        
        logger.info(f"Exchange: {exchange} | Symbol: {data.symbol} Close: {data.close} Volume: {data.volume}.")

    def log_fetch_error(self, exchange: str, symbol: str, error: Exception) -> None:
        
        logger.error(f"Error fetching -> Exchange: {exchange} | Symbol: {symbol}: {error}")