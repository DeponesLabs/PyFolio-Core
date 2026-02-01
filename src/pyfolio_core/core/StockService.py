import time
import requests
import logging
from tvDatafeed import TvDatafeed, Interval
from typing import Optional, List, Union

from pyfolio_core.core.Interfaces import StockService
from pyfolio_core.core.database import MarketDatabase, PortfolioDatabase
from pyfolio_core.core.enums import Exchange
from pyfolio_core.core.constants import SCALING_FACTOR
from pyfolio_core.core.domainobjects import StockValue

logger = logging.getLogger("TradingViewService")
logger.setLevel(logging.INFO)

# 1. STDOUT to Screen
if not logger.handlers:
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    # 2. STDERR to File
    f_handler = logging.FileHandler('error.log')
    f_handler.setLevel(logging.ERROR)
    f_format = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

class TradingViewService(StockService):

    def __init__(self, market_db_path: str, pfolio_db_path: str, exchange: Union[Exchange, str] = Exchange.BIST):
        
        self.market_db = MarketDatabase(market_db_path)
        self.pfolio_db = PortfolioDatabase(pfolio_db_path)
        
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

    def update_single_price(self, symbol: str) -> bool:

        clean_sym = self._clean_symbol(symbol)
        
        price_float = self.fetch_price(clean_sym)
        if price_float is None:
            return False
        
        price = int(round(price_float * SCALING_FACTOR))

        try:
            conn = self.market_db.get_connection()
            conn.execute("""
                UPDATE portfolio_assets 
                SET current_price = ?,
                    last_updated = current_timestamp
                WHERE symbol = ?
            """, (price, clean_sym))
            
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
        tv = self._get_server_connection()
        
        print(f"Toplam {len(tickers)} hisse işlenecek.")
                
        success_count = 0
        
        stockvalues: list[StockService] = []
        for symbol in tickers:
            try:
                df = tv.get_hist(symbol=symbol, exchange=self.exchange, interval=Interval.in_daily, n_bars=1)
                
                if df is not None and not df.empty:
                    row = df.iloc[-1]
                    stockvalue = StockValue.from_tv_dataframe(symbol, row)
                    event_date = row.name.strftime('%Y-%m-%d')
                    open = int(round(row['open'] * SCALING_FACTOR))
                    high = int(round(row['high'] * SCALING_FACTOR))
                    low = int(round(row['low'] * SCALING_FACTOR))
                    close = int(round(row['close'] * SCALING_FACTOR))
                    volume = float(row['volume'])
                    
                    stockvalues.append(stockvalue)

                    # DuckDB Upsert (Insert or Replace)
                    conn.execute("""
                        INSERT INTO daily_prices (symbol, event_date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(symbol, event_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                        """, (
                        symbol, event_date, 
                        open, high, low, close, 
                        volume
                    ))
                    
                    success_count += 1
                    time.sleep(0.1)
                    
                return stockvalues
                    
            except Exception as e:
                # A single stock mistake shouldn't break the entire cycle.
                logger.error(f"SYNC_FAIL | Exchange: {self.exchange} | Symbol: {symbol} | Reason: {e}")
        
        logger.info(f"Sync Complete. Processed: {success_count}/{len(tickers)}")