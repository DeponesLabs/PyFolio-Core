import logging
from datetime import datetime, timedelta
import pandas as pd
from tefas import Crawler
from typing import Dict

from pyfolio_core.core.database import MarketDatabase
from pyfolio_core.core.constants import SCALING_FACTOR

logger = logging.getLogger("FundService")
logger.setLevel(logging.INFO)

if not logger.handlers:
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    f_handler = logging.FileHandler('error.log')
    f_handler.setLevel(logging.ERROR)
    f_format = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

class FundDataService:

    def __init__(self, db_path: str):
        self.db_manager = MarketDatabase(db_path)
        self.crawler = Crawler()

    def _get_latest_fund_data(self) -> Dict[str, float]:

        today = datetime.now().date()

        logger.info(f"Fetching TEFAS data ({today})...")

        try:
            df = self.crawler.fetch(start=today)
        except Exception as e:
            logger.error(f"TEFAS fetch error: {e}")
            return {}
            
        if df is None or df.empty:
            yesterday = today - timedelta(days=1)
            logger.info(f"No data for today. Trying yesterday ({yesterday})...")
            try:
                df = self.crawler.fetch(start=yesterday)
            except Exception:
                pass
        
        if df is None or df.empty:
            day_before = today - timedelta(days=2)
            logger.info(f"Still no data. Trying 2 days ago ({day_before})...")
            try:
                df = self.crawler.fetch(start=day_before)
            except Exception:
                pass

        if df is not None and not df.empty:
            price_dict = pd.Series(df.price.values, index=df.code).to_dict()
            logger.info(f"TEFAS data retrieved. Total Funds: {len(price_dict)}")
            return price_dict
        else:
            logger.error("Cannot retrieve data from TEFAS (All attempts failed).")
            return {}

    def update_portfolio_prices(self):

        conn = self.db_manager.get_connection()
        
        try:
            result = conn.execute("SELECT symbol FROM portfolio_assets WHERE asset_type = 'FUND'").fetchall()
            db_symbols = {row[0] for row in result} # Set: {'AFT', 'TCD'}
        except Exception as e:
            logger.critical(f"Database read error: {e}")
            return

        if not db_symbols:
            logger.info("No funds found in portfolio to update.")
            return

        tefas_data = self._get_latest_fund_data()
        
        if not tefas_data:
            return

        update_count = 0
        logger.info(f"Updating {len(db_symbols)} funds in portfolio...")

        for symbol in db_symbols:
            if symbol in tefas_data:
                price_float = tefas_data[symbol]
                
                price_integer = int(round(price_float * SCALING_FACTOR))
                
                try:
                    conn.execute("""
                        UPDATE portfolio_assets 
                        SET current_price_integer = ?, 
                            last_updated = current_timestamp
                        WHERE symbol = ?
                    """, (price_integer, symbol))
                    
                    update_count += 1
                    logger.info(f"{symbol}: {price_float:.4f} TL -> updated.")
                    
                except Exception as e:
                    logger.error(f"DB Update Error ({symbol}): {e}")
            else:
                logger.warning(f"Fund {symbol} not found in TEFAS data.")

        logger.info(f"Fund update complete. Success: {update_count}/{len(db_symbols)}")

    def fetch_market_daily_close(self):
        """
        [CRON JOB] TEFAS'taki TÜM fonların verilerini 'daily_prices' tablosuna basar.
        """
        logger.info("Starting Daily TEFAS Sync...")
        
        tefas_data = self._get_latest_fund_data()
        if not tefas_data:
            return

        conn = self.db_manager.get_connection()
        today_str = datetime.now().date().strftime('%Y-%m-%d')
        
        success_count = 0
        
        for symbol, price in tefas_data.items():
            try:
                price_integer = int(round(price * SCALING_FACTOR))
                
                conn.execute("""
                    INSERT INTO daily_prices (symbol, event_date, open_integer, high_integer, low_integer, close_integer, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, event_date) DO UPDATE SET
                        close_integer = EXCLUDED.close_integer
                """, (
                    symbol, today_str, 
                    price_integer, price_integer, price_integer, price_integer, 
                    0
                ))
                success_count += 1
            except Exception as e:
                logger.debug(f"Sync skip {symbol}: {e}")
                
        logger.info(f"Daily Sync Complete. Processed: {success_count} funds.")

# --- TEST ---
# if __name__ == "__main__":
#     service = FundDataService(db_path="data/portfolio.duckdb")
    
    # 1. Portföy Güncelleme Testi
    # service.update_portfolio_prices()
    
    # 2. Tüm Pazar Verisi Testi
    # service.fetch_market_daily_close()