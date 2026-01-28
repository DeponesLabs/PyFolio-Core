import time
import requests
import sqlite3
import logging
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from typing import Optional, Union

from Interfaces import StockService
from pyfolio_core.database import db_manager
from enums import Exchange

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MarketService")

class TradingViewService(StockService):

    def __init__(self, db_manager: str, exchange: Union[Exchange, str] = Exchange.BIST):
        
        self.db_manager = db_manager
        
        self.exchange = exchange.value if isinstance(exchange, Exchange) else str(exchange).upper()
            
        self.tv = None  # Lazy-loading
        self._clean_map = str.maketrans('', '', '\u200b\t\n\r ')

    def _get_server_connection(self):

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
        tv = self._get_server_connection()

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

    def get_all_bist_symbols():
        # TradingView Scanner Endpoint (Halka açık tarama sunucusu)
        url = "https://scanner.tradingview.com/turkey/scan"

        # Payload
        payload = {
            "filter": [
                {"left": "type", "operation": "equal", "right": "stock"}, # Only stocks
                {"left": "subtype", "operation": "in_range", "right": ["common", "preference"]}
            ],
            "options": {
                "lang": "tr"
            },
            "symbols": {
                "query": {
                    "types": []
                },
                "tickers": []
            },
            "columns": [
                "name",         # Symbol
                "description",  # Company name
                "sector",       # Sector (Ulaştırma)
                "close"         # Current price (Optional, for check)
            ],
            "sort": {
                "sortBy": "name",
                "sortOrder": "asc"
            },
            "range": [0, 1000] # Get first 1000. (There are already around 600 on BIST (Istanbul Stock Exchange))
        }

        print("Connecting to the TradingView server...")
        
        try:
            response = requests.post(url, json=payload)
            data = response.json()
            
            total_count = data['totalCount']
            print(f"The server found {total_count} shares.")

            asset_list = []
            for item in data['data']:
                d = item['d'] # Data array
                asset_list.append({
                    "symbol": d[0],
                    "company_name": d[1],
                    "sector": d[2],
                    "last_price": d[3]
                })

            df = pd.DataFrame(asset_list)
            df.to_csv("bist_full_list.csv", index=False)
            
            return df

        except Exception as e:
            print(f"Error occured: {e}")
            return None




