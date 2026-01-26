import time
import sqlite3
import logging
from tvDatafeed import TvDatafeed, Interval
from typing import Optional, Union

from Interfaces import StockService
from enums import Exchange

# Logging Settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MarketService")

class TradingViewService(StockService):

    def __init__(self, db_path: str, exchange: Union[Exchange, str] = Exchange.BIST):
        
        self.db_path = db_path
        
        self.exchange = exchange.value if isinstance(exchange, Exchange) else str(exchange).upper()
            
        self.tv = None  # Lazy-loading
        self._clean_map = str.maketrans('', '', '\u200b\t\n\r ')

    def _get_connection(self):

        if self.tv is None:
            logger.info("Connecting to TradingView servers...")
            try:
                self.tv = TvDatafeed()
            except Exception as e:
                logger.error(f"Connection Error: {e}")
                raise ConnectionError("TradingView connection could not be established.")
        return self.tv

    def _clean_symbol(self, symbol: str) -> str:

        if not symbol:
            return ""
        return str(symbol).translate(self._clean_map).strip().upper()

    def fetch_price(self, symbol: str) -> Optional[float]:
        
        clean_sym = self._clean_symbol(symbol)
        tv = self._get_connection()

        try:
            data = tv.get_hist(
                symbol=clean_sym,
                exchange=self.exchange,
                interval=Interval.in_daily,
                n_bars=1
            )

            if data is not None and not data.empty:
                price = data['close'].iloc[-1]
                return float(price)
            else:
                logger.warning(f"{clean_sym} returned empty data.")
                return None

        except Exception as e:
            logger.error(f"{clean_sym} data retrieval error: {e}")
            return None

    def update_single_stock(self, symbol: str, price_float: float) -> bool:

        clean_sym = self._clean_symbol(symbol)
        
        # Float -> Cents Dönüşümü
        price_cents = int(round(price_float * 100))

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE portfolio_assets 
                    SET current_price_cents = ?,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE symbol = ?
                """, (price_cents, clean_sym))
                conn.commit()
                
                # Etkilenen satır sayısını kontrol et
                if cursor.rowcount > 0:
                    logger.info(f"{clean_sym}: {price_float:.2f} updated to TL.")
                    return True
                else:
                    logger.warning(f"{clean_sym} could not be found in the database and could not be updated.")
                    return False

        except sqlite3.Error as e:
            logger.error(f"DB Error ({clean_sym}): {e}")
            return False

    def run_full_update(self):
       
        logger.info("*** Mass Update Begins ***")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT symbol FROM portfolio_assets WHERE asset_type = 'STOCK'")
                raw_symbols = [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.critical(f"Database could not be read.: {e}")
            return

        symbols = [self._clean_symbol(s) for s in raw_symbols]
        logger.info(f"Watchlist: {len(symbols)} shares.")

        success_count = 0
        
        for sym in symbols:
            # Optional: short wait to avoid hitting the Rate Limit
            time.sleep(0.5) 
            
            price = self.fetch_price(sym)
            if price:
                if self.update_single_stock(sym, price):
                    logger.info(f"{sym}: {price:.2f} TL")
                    success_count += 1
        
        logger.info(f"Update complete. Success: {success_count}/{len(symbols)}")
