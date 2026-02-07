import os
import datetime
import time
import requests
import logging
import random
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Union
import pandas as pd

from tvDatafeed import TvDatafeed, Interval

from pyfolio_core.core.Interfaces import StockService
from pyfolio_core.core.database import MarketDatabase, PortfolioDatabase
from pyfolio_core.core.enums import Exchange
from pyfolio_core.core.constants import SCALING_FACTOR
from pyfolio_core.core.domainobjects import StockValue
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

    def get_market_status(self) -> dict:
        """
        Borsanın anlık durumunu (open, closed, holiday vb.) ve ticker listesini döner.
        """
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
            return {"status": "unknown", "tickers": []}

        if current_enum not in SCANNER_MAP:
            logger.warning(f"Scanner Warning: '{current_enum.name}' için hisse senedi taraması desteklenmiyor.")
            return {"status": "unknown", "tickers": []}

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
            "columns": ["name", "market_status"], # Status bilgisini de istiyoruz
            "sort": {"sortBy": "name", "sortOrder": "asc"},
            "range": [0, 2000] # Tek seferde tüm listeyi alıyoruz
        }

        try:
            logger.info(f"Checking market status and fetching tickers for {current_enum.name}...")
            response = requests.post(url, json=payload, timeout=15)
            
            if response.status_code != 200:
                logger.error(f"Scanner API Error: {response.status_code}")
                return {"status": "error", "tickers": []}

            data = response.json()
            if not data.get('data'):
                return {"status": "no_data", "tickers": []}

            # Market status bilgisini listedeki ilk sembolden alıyoruz
            # TradingView bu bilgiyi borsa genelinde tutarlı döner
            raw_status = data['data'][0]['d'][1]
            tickers = [item['d'][0] for item in data['data']]
            
            logger.info(f"Market: {current_enum.name} | Status: {raw_status.upper()} | Found {len(tickers)} symbols.")
            
            return {
                "status": raw_status,
                "tickers": tickers
            }

        except Exception as e:
            logger.error(f"Scanner Exception during status check: {e}")
            return {"status": "error", "tickers": []}

    def sync_market_snapshot(self, use_jsonl: bool = True) -> List[StockValue]:
        
        logger.info(f"{self.exchange} Daily Market Data Sync Started...")
        
        tickers = self.get_available_tickers()
        if not tickers:
            logger.info(f"Ticker list read error.")
            return []

        logger.info(f"Total {len(tickers)} shares in the pool.")
                
        scanner = MarketScanner(self.exchange, tickers, self.market_db, use_jsonl=use_jsonl)
        stockvalues = scanner.orchestrate()
        
        pipeline = MarketDataPipeline(self.market_db)
        pipeline.insert_batch(stockvalues)
        
        logger.info(f"Sync Complete. Processed: {len(stockvalues)}/{len(tickers)}")
        return stockvalues
        
class MarketDataPipeline:
    
    def __init__(self, market_db: MarketDatabase):
        
        self.market_db = market_db

    def insert_batch(self, stockvalues: list[StockValue]):
        
        if not stockvalues:
            logger.warning("No StockValue data provided to insert.")
            return
        
        try:
            data_list = [s.to_dict() for s in stockvalues]
            df = pd.DataFrame(data_list)
            
            conn = self.market_db.get_connection()
            conn.execute("""INSERT OR REPLACE INTO daily_prices (symbol, event_date, query_time, open, high, low, close, volume) 
                            SELECT symbol, event_date, query_time, open, high, low, close, volume FROM df""")
            logger.info(f"Successfully loaded {len(df)} rows to GlobalMarket.duckdb")
            
        except Exception as e:
            logger.error(f"Bulk Loading Error: {e}")    
        
class MarketScanner:
    
    def __init__(self, exchange, symbols, market_db: MarketDatabase, use_jsonl: bool = True):
        
        self.exchange = exchange
        self.market_db = market_db
        self.use_jsonl = use_jsonl
        self.failed_symbols = []
        
        
        self.failed_symbols = []
        self.results = []
        self.tv = None  # lazy-loading
        self.reporter = ScanReporter()
        
        self._lock = threading.Lock()
        self.today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        self.symbols = self._filter_already_processed(symbols)

    def _filter_already_processed(self, all_symbols: List[str]) -> List[str]:
        
        try:
            conn = self.market_db.get_connection()
            
            existing = conn.execute("SELECT symbol FROM daily_prices WHERE CAST(event_date AS DATE) = ?", [self.today_str]).fetchall()
            
            processed_set = {row[0] for row in existing}
            remaining = [s for s in all_symbols if s not in processed_set]
            
            if len(processed_set) > 0:
                logger.info(f"RESUME AKTİF: {len(processed_set)} sembol zaten mevcut. {len(remaining)} sembol taranacak.")
            
            return remaining
        except Exception as e:
            logger.error(f"Resume check failed: {e}. Full scan will be performed.")
            return all_symbols

    def _get_checkpoint_path(self):
        
        base_dir = "/home/valjean/Server/data/raw"
        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, f"{self.today_str}_{self.exchange}.jsonl")

    def _write_checkpoint(self, stock_value: StockValue):
        """Thread-Safe bir şekilde diske yazar."""
        if not self.use_jsonl:
            return

        file_path = self._get_checkpoint_path()
        data_dict = stock_value.to_dict()

        with self._lock:
            try:
                with open(file_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(data_dict) + '\n')
            except Exception as e:
                logger.error(f"Checkpoint Write Error ({stock_value.symbol}): {e}")

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
            
            for item in data:
                self._write_checkpoint(item)
                
            self.reporter.log_fetch_success(self.exchange, item)
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
        
        if not to_process:
            logger.info("All symbols are already up-to-date. No transactions will be made.")
            return []
        
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


# from tvDatafeed import TvDatafeed, Interval

# tv = TvDatafeed()
# def query_tv_data(exchange: Exchange, symbol: str, interval = Interval.in_daily, n_bars=1) -> pd.DataFrame:
    
#     return tv.get_hist(exchange=exchange, symbol=symbol, interval=interval, n_bars=n_bars)
    
# bist100 = query_tv_data(Exchange.BIST, 'XU100')
# print(f"XU100 Kapanış: {bist100['close'].iloc[-1]}")

# # Dollar Index (DXY), Exchange: 'TVC'
# dxy = query_tv_data(Exchange.TVC, 'DXY')
# print(f"DXY Kapanış: {dxy['close'].iloc[-1]}")

# usdtry = query_tv_data(Exchange.FX_IDC, 'USDTRY')
# print(f"Dolar/TL: {usdtry['close'].iloc[-1]}")

class TradingViewQuery:
    
    def __init__(self):
        
        from tvDatafeed import TvDatafeed, Interval

        tv = TvDatafeed()

        # 1. BIST 100 Endeksi (Zaten BIST exchange kullandığın için kolay)
        bist100 = tv.get_hist(
            symbol='XU100', 
            exchange='BIST', 
            interval=Interval.in_daily, 
            n_bars=1
        )
        print(f"XU100 Kapanış: {bist100['close'].iloc[-1]}")

        # 2. Dolar Endeksi (DXY) - Exchange 'TVC' olmalı!
        dxy = tv.get_hist(
            symbol='DXY', 
            exchange='TVC',  # <-- Burası kritik
            interval=Interval.in_daily, 
            n_bars=1
        )
        print(f"DXY Kapanış: {dxy['close'].iloc[-1]}")

        # 3. USD/TRY Kuru
        usdtry = tv.get_hist(
            symbol='USDTRY', 
            exchange='FX_IDC', 
            interval=Interval.in_daily, 
            n_bars=1
        )
        print(f"Dolar/TL: {usdtry['close'].iloc[-1]}")

            
# class TradingViewService(StockService):
#     # ... init ve diğer metodlar ...

#     def _get_processed_symbols(self, date_str: str, is_daily_close: bool) -> set:
#         """DB'den o gün ve o modda (Final veya Snapshot) kaydedilenleri döner."""
#         conn = self.market_db.get_connection()
#         query = "SELECT symbol FROM daily_prices WHERE event_date = ? AND is_daily_close = ?"
#         rows = conn.execute(query, [date_str, is_daily_close]).fetchall()
#         return {row[0] for row in rows}

#     def sync_market_snapshot(self, resume: bool = False):
#         """Gün içi anlık fiyat günceller. Resume istenirse sadece eksikleri tamamlar."""
#         today_str = datetime.datetime.now().strftime("%Y-%m-%d")
#         all_tickers = self.get_available_tickers()
        
#         to_process = all_tickers
#         if resume:
#             processed = self._get_processed_symbols(today_str, is_daily_close=False)
#             to_process = [s for s in all_tickers if s not in processed]
#             logger.info(f"Snapshot Resume: {len(to_process)} sembol kaldı.")

#         self._execute_scan(to_process, is_daily_close=False)

#     def finalize_market_day(self):
#         """Resmi gün sonu kapanışını mühürler. Her zaman Resume desteklidir."""
#         today_str = datetime.datetime.now().strftime("%Y-%m-%d")
#         all_tickers = self.get_available_tickers()
        
#         # Gün sonu mühürlemesinde Resume her zaman mantıklıdır (40 dk sürdüğü için)
#         processed = self._get_processed_symbols(today_str, is_daily_close=True)
#         to_process = [s for s in all_tickers if s not in processed]
        
#         if not to_process:
#             logger.info("Günlük kapanış zaten mühürlenmiş. İşlem yok.")
#             return

#         logger.info(f"Finalizing Market Day: {len(to_process)} symbols remaining.")
#         self._execute_scan(to_process, is_daily_close=True)

#     def _execute_scan(self, symbols: List[str], is_daily_close: bool):
#         """Scanner ve Pipeline'ı çalıştıran ortak iç metod."""
#         scanner = MarketScanner(self.exchange, symbols, is_daily_close=is_daily_close)
#         stockvalues = scanner.orchestrate()
        
#         pipeline = MarketDataPipeline(self.market_db)
#         # Pipeline artık is_daily_close bilgisini de tabloya yazacak
#         pipeline.insert_batch(stockvalues, is_daily_close=is_daily_close)