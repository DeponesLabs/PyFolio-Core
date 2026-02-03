import os
import datetime
import time
import requests
import logging
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Union
import pandas as pd

from tvDatafeed import TvDatafeed, Interval

from pyfolio_core.core.Interfaces import StockService
from pyfolio_core.core.database import MarketDatabase, PortfolioDatabase
from pyfolio_core.core.enums import Exchange
from pyfolio_core.core.constants import SCALING_FACTOR
from pyfolio_core.core.domain_objects import StockValue
from pyfolio_core.core.logging import ScanReporter

logger = logging.getLogger(__name__)

class TradingViewService(StockService):

    def __init__(self, market_db_path: str, pfolio_db_path: str, exchange: Union[Exchange, str] = Exchange.BIST):
        
        self.market_db = MarketDatabase(market_db_path)
        self.pfolio_db = PortfolioDatabase(pfolio_db_path)
        
        self.exchange = exchange.value if isinstance(exchange, Exchange) else str(exchange).upper()
            
        self._clean_map = str.maketrans('', '', '\u200b\t\n\r ')

    
    def _clean_symbol(self, symbol: str) -> str:

        if not symbol:
            return ""
        return str(symbol).translate(self._clean_map).strip().upper()

    def fetch_price(self, symbol: str) -> Optional[float]:
        
        clean_sym = self._clean_symbol(symbol)
        tv = self._get_server_connection()

        try:
            data = tv.get_hist(symbol=clean_sym, exchange=self.exchange, interval=Interval.in_daily, n_bars=1)
            if data is not None and not data.empty:
                price = data['close'].iloc[-1]
                return float(price)
            else:
                logger.warning(f"{clean_sym} returned empty data.")
                return None

        except Exception as e:
            logger.error(f"{clean_sym} data retrieval error: {e}")
            return None

    def update_single_price(self, symbol: str) -> bool:

        clean_sym = self._clean_symbol(symbol)
        
        price_float = self.fetch_price(clean_sym)
        if price_float is None:
            return False
        
        price = int(round(price_float * SCALING_FACTOR))

        try:
            conn = self.market_db.get_connection()
            conn.execute("""UPDATE portfolio_assets SET current_price = ?, last_updated = current_timestamp WHERE symbol = ?""", 
                         (price, clean_sym))
            
            logger.info(f"{clean_sym}: {price_float:.2f} updated.")
            return True
        except Exception as e:
            logger.error(f"DB Error ({clean_sym}): {e}")
            return False

    def update_portfolio_prices(self):
       
        logger.info("*** Mass Portfolio Update Begins ***")
        
        try:
            conn = self.market_db.get_connection()
            result = conn.execute("SELECT symbol FROM portfolio_assets WHERE asset_type = 'STOCK'").fetchall()
            raw_symbols = [row[0] for row in result]
        except Exception as e:
            logger.critical(f"Database read error: {e}")
            return

        symbols = [self._clean_symbol(s) for s in raw_symbols]
        logger.info(f"Watchlist: {len(symbols)} shares.")

        success_count = 0
        
        for sym in symbols:
            # Rate Limit Protection
            time.sleep(1) 
            
            if self.update_single_price(sym):
                success_count += 1
        
        logger.info(f"Update complete. Success: {success_count}/{len(symbols)}")

    def get_available_tickers(self) -> List[str]:
        
        SCANNER_MAP = {
            Exchange.BIST:   {'region': 'turkey',  'filter': 'BIST'},
            Exchange.NASDAQ: {'region': 'america', 'filter': 'NASDAQ'},
            Exchange.NYSE:   {'region': 'america', 'filter': 'NYSE'},
            Exchange.AMEX:   {'region': 'america', 'filter': 'AMEX'},
            Exchange.LSE:    {'region': 'uk',      'filter': 'LSE'},
            Exchange.XETRA:  {'region': 'germany', 'filter': 'XETRA'},
        }

        try:
            current_enum = Exchange(self.exchange)
        except ValueError:
            logger.error(f"Scanner Error: '{self.exchange}' geçerli bir Exchange Enum değeri değil.")
            return []

        if current_enum not in SCANNER_MAP:
            logger.warning(f"Scanner Warning: '{current_enum.name}' için hisse senedi taraması desteklenmiyor veya yapılandırılmadı.")
            return []

        config = SCANNER_MAP[current_enum]
        region = config['region']
        exchange_filter = config['filter']

        url = f"https://scanner.tradingview.com/{region}/scan"
        
        payload = {
            "filter": [
                {"left": "type", "operation": "equal", "right": "stock"},
                {"left": "subtype", "operation": "in_range", "right": ["common", "preference"]},
                {"left": "exchange", "operation": "equal", "right": exchange_filter}
            ],
            "options": {"lang": "tr"},
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": ["name"], 
            "sort": {"sortBy": "name", "sortOrder": "asc"},
            "range": [0, 2000]
        }

        logger.info(f"Scanning market: {current_enum.name} ({region})...")
        
        try:
            response = requests.post(url, json=payload, timeout=15)
            if response.status_code != 200:
                logger.error(f"Scanner API Error: {response.status_code} - {response.text}")
                return []

            data = response.json()
            tickers = [item['d'][0] for item in data['data']]
            
            logger.info(f"Market Scan ({current_enum.name}): Found {len(tickers)} symbols.")
            return tickers

        except Exception as e:
            logger.error(f"Scanner Exception: {e}")
            return []


    def fetch_market_daily_close(self) -> StockValue:
        
        logger.info(f"{self.exchange} Daily Market Data Sync Started...")
        
        tickers = self.get_available_tickers()
        if not tickers:
            logger.info(f"Ticker list read error.")
            return

        conn = self.market_db.get_connection()
        # tv = self._get_server_connection()
        
        print(f"Toplam {len(tickers)} hisse işlenecek.")
                
        scanner = MarketScanner(self.exchange, tickers)
        stockvalues = scanner.orchestrate()
        pipeline = MarketDataPipeline(self.market_db)
        pipeline.insert_batch(stockvalues)
        
        logger.info(f"Sync Complete. Processed: {len(stockvalues)}/{len(tickers)}")
        
class MarketDataPipeline:
    
    def __init__(self, market_db: MarketDatabase):
        
        self.market_db = market_db

    def insert_batch(self, stockvalues: list[StockValue]):
        
        if not stockvalues:
            logger.warning("No StockValue data provided to insert.")
            return
        
        try:
            df = pd.DataFrame(stockvalues)
        
            conn = self.market_db.get_connection()
            conn.execute("""INSERT OR REPLACE INTO daily_prices (symbol, event_date, open, high, low, close, volume) 
                         SELECT symbol, event_date, open, high, low, close, volume FROM df""")
            
            logger.info(f"Successfully loaded {len(df)} rows to GlobalMarket.duckdb")
            
        except Exception as e:
            logger.error(f"Bulk Loading Error: {e}")    
        
class MarketScanner:
    
    def __init__(self, exchange, symbols):
        
        self.exchange = exchange
        self.symbols = symbols
        self.failed_symbols = []
        self.results = []
        self.tv = None  # lazy-loading
        self.reporter = ScanReporter()

    def _connect_server(self):

        if self.tv is None:
            logger.info("Connecting to TradingView servers...")
            try:
                self.tv = TvDatafeed()
            except Exception as e:
                logger.error(f"Connection Error: {e}")
                raise ConnectionError("TradingView connection could not be established.")
        return self.tv


    def fetch_data(self, symbol)-> tuple[str, list[StockValue] | None]:
        
        try:
            time.sleep(random.uniform(0.5, 1.5))
            df = self.tv.get_hist(symbol=symbol, exchange=self.exchange, interval=Interval.in_daily, n_bars=1)
            
            if df is None or df.empty:
                return (symbol, None)
            
            data = [StockValue.from_tv_dataframe(symbol, row) 
                    for row in df.itertuples(index=True)]
            
            self.reporter.log_fetch_success(self.exchange, symbol, len(data))
            
            return (symbol, data)
            
        except Exception as error:
            self.reporter.log_fetch_error(self.exchange, symbol, error)
            return (symbol, None)

    def run_scan(self, symbol_list: list[str], max_workers:int = 1):
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            batch_results = list(executor.map(self.fetch_data, symbol_list))
        
        current_failed = [i for i, k in batch_results if k is None]
        success_data = [item for i, k in batch_results if k is not None for item in k]
        # for i, k in batch_results:
        #     if k is not None:
        #         for item in k:
        #             success_data.append(item)
        
        return success_data, current_failed

    def orchestrate(self, max_attempt = 5) -> list[StockValue]:
        
        to_process = self.symbols
        attempt = 1
        
        self.tv = self._connect_server()
        
        while to_process and attempt <= max_attempt:
            logger.info(f"Attempt {attempt}: Processing {len(to_process)} symbols...")
            success, to_process = self.run_scan(to_process)
            self.results.extend(success)
            
            if to_process and attempt < max_attempt:
                wait_time = attempt * 2     # Double wait time in each error.
                logger.warning(f"Failed: {len(to_process)}. Waiting {wait_time}s...")
                time.sleep(wait_time)
                attempt += 1
        
        if to_process:
            self.reporter.log_failures(self.exchange, to_process)
        else:
            logger.info("Scan completed successfully. No failures.")
            
        return self.results

