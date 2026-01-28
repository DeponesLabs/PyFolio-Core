from datetime import datetime, timedelta
import sqlite3
import logging
from tefas import Crawler
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FundService")

class FundDataService:

    def __init__(self, db_path: str):
        
        self.db_path = db_path
        self.crawler = Crawler()

    def _get_latest_fund_data(self):
        
        # Bugün ve Dün tarihlerini hazırla
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        try:
            logger.info(f"Fetching TEFAS data ({today})...")
            df = self.crawler.fetch(start=today)
            
            if df is None or df.empty:
                logger.info(f"We don't have today's data yet. Trying yesterday's data...")
                df = self.crawler.fetch(start=yesterday)
                
            if df is not None and not df.empty:
                price_dict = pd.Series(df.price.values, index=df.code).to_dict()
                return price_dict
            else:
                logger.error("Cannot retrieve data from TEFAS.")
                return {}
        
        except Exception as e:
            logger.error(f"TEFAS connection error: {e}")
            return {}

    def update_portfolio_funds(self):

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM portfolio_assets WHERE asset_type = 'FUND'")
        db_symbols = {row[0] for row in cursor.fetchall()} # Create a Set
        
        # Fetch all Funds from TEFAS
        tefas_data = self._get_latest_fund_data()
        
        if not tefas_data:
            conn.close()
            return

        update_count = 0
        logger.info("Inserting Fund prices into the database...")

        for symbol in db_symbols:
            if symbol in tefas_data:
                price_float = tefas_data[symbol]
                
                price_cents = int(round(price_float * 100))
                
                try:
                    cursor.execute("""
                        UPDATE portfolio_assets 
                        SET current_price_cents = ?, last_updated = CURRENT_TIMESTAMP
                        WHERE symbol = ?""", (price_cents, symbol))
                    
                    update_count += 1
                    logger.info(f"{symbol}: {price_float:.4f} TL updated to {price_cents} cents.")
                    
                except Exception as e:
                    logger.error(f"DB Error ({symbol}): {e}")

        conn.commit()
        conn.close()
        logger.info(f"Fund update complete. {update_count} funds updated.")



    def get_tv_session():
        
        load_dotenv()
        user = os.getenv("TV_USERNAME")
        password = os.getenv("TV_PASSWORD")
        if user and password:
            return TvDatafeed(username=user, password=password)
        return TvDatafeed()

    def fetch_bist_tickers():
        print("🌍 Scanning market (Scanner)...")
        url = "https://scanner.tradingview.com/turkey/scan"
        payload = {
            "filter": [{"left": "type", "operation": "equal", "right": "stock"}],
            "options": {"lang": "tr"},
            "symbols": {"query": {"types": []}, "tickers": [], "groups": []},
            "columns": ["name"],
            "range": [0, 2000]
        }
        try:
            response = requests.post(url, json=payload)
            data = response.json()
            return [item['d'][0] for item in data['data']]
        except Exception:
            return []

    def get_stock_data(tv, symbol) -> StockValue | None:

        try:
            df = tv.get_hist(symbol=symbol, exchange=EXCHANGE, interval=Interval.in_daily, n_bars=1)
            if df is not None and not df.empty:
                df = df.reset_index()
                return StockValue.from_tv_row(symbol, df.iloc[-1])
        except Exception:
            pass
        return None