import os
import logging
import sqlite3
import duckdb
import pandas as pd
from datetime import datetime
from typing import List

logger = logging.getLogger("PyFolio-Core")

class MarketDatabase:
    
    def __init__(self, db_path: str = "data/GlobalMarket.duckdb"):
        
        self.db_path = db_path
        self._conn = None
        self._connect()
        
    def _connect(self):
        
        schema_exists = os.path.exists(self.db_path)
        if not self._conn:
            try:
                self._conn = duckdb.connect(self.db_path)
                logger.info(f"Connected to DuckDB: {self.db_path}")
                if not schema_exists:
                    self._init_schema()
            except IOError as ioerror:
                logger.error(f"DuckDB IO Error: {e}")
                raise  
            except Exception as e:
                logger.error(f"DuckDB Connection Error: {e}")
                raise  

    def _init_schema(self):
        
        try:
            ### CREATE TABLES
            # TABLE: "DailyPrices"
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_prices (
                    symbol VARCHAR,
                    event_date DATE,
                    open BIGINT,
                    high BIGINT,
                    low BIGINT,
                    close BIGINT,
                    volume DOUBLE,        -- Hacim para değildir, adet/lot küsuratlı olabilir.
                    PRIMARY KEY (symbol, event_date)
                );
            """)
            
            self._conn.execute("""
                CREATE VIEW view_market_signals AS
                    SELECT 
                        symbol,
                        event_date,
                        close,
                        LAG(close) OVER (PARTITION BY symbol ORDER BY event_date) as prev_close,
                        ROUND(((close - prev_close) * 1.0 / prev_close) * 100, 2) as daily_change_pct
                    FROM daily_prices;
            """)

            self._conn.commit()
            logger.info("DuckDB Schema initialized (STRICT INTEGER MODE).")
            
        except Exception as e:
            logger.error(f"DuckDB Schema Initialization Error: {e}")
            raise

    def create_analysis_views(self):
        
        sql_view_technical_signals = """ 
            CREATE VIEW IF NOT EXISTS view_technical_signals AS
                SELECT 
                    symbol,
                    event_date,
                    close / 1000000.0 AS close_price,
                    -- 5 and 20-Day Moving Averages
                    AVG(close) OVER (PARTITION BY symbol ORDER BY event_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) / 1000000.0 AS sma_5,
                    AVG(close) OVER (PARTITION BY symbol ORDER BY event_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / 1000000.0 AS sma_20,
                    -- Daily Percentage Change
                    ROUND(((close * 1.0 / LAG(close) OVER (PARTITION BY symbol ORDER BY event_date)) - 1) * 100, 2) AS daily_change_pct,
                    -- Volume Change (To understand whether interest is increasing or decreasing)
                    ROUND(((volume * 1.0 / AVG(volume) OVER (PARTITION BY symbol ORDER BY event_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) - 1) * 100, 2) AS volume_momentum_pct
                FROM daily_prices;
        """
        
        sql_view_quantum_radar = """
            CREATE VIEW IF NOT EXISTS view_quantum_radar AS
                WITH IndexChange AS (
                    -- Örn: XU100 endeksinin o günkü değişimini baz alalım
                    SELECT event_date, daily_change_pct as index_change
                    FROM view_technical_signals
                    WHERE symbol = 'XU100' -- Endeks verisini çektiğini varsayıyoruz
                )
                SELECT 
                    t.symbol,
                    t.event_date,
                    t.daily_change_pct,
                    i.index_change,
                    -- Alfa: Hisse endeksten ne kadar fazla/az kazandırmış?
                    (t.daily_change_pct - i.index_change) AS relative_strength_alpha
                FROM view_technical_signals t
                JOIN IndexChange i ON t.event_date = i.event_date
                WHERE t.symbol != 'XU100';
        """
        
        try:
            self._conn.execute(sql_view_technical_signals)
            self._conn.execute(sql_view_quantum_radar)
            
            self._conn.commit()
            logger.info("DuckDB Views initialized (STRICT INTEGER MODE).")
            
        except Exception as e:
            logger.error(f"DuckDB Views Initialization Error: {e}")
            raise

    def create_analysis_views(self):
        """Injects analytical intelligence (Views) into DuckDB."""
            
        sql_view_technical_signals = """
            CREATE VIEW IF NOT EXISTS view_technical_signals AS
            SELECT 
                symbol,
                event_date,
                close / 1000000.0 AS close_price,
                AVG(close) OVER (PARTITION BY symbol ORDER BY event_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) / 1000000.0 AS sma_5,
                AVG(close) OVER (PARTITION BY symbol ORDER BY event_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / 1000000.0 AS sma_20,
                ROUND(((close * 1.0 / LAG(close) OVER (PARTITION BY symbol ORDER BY event_date)) - 1) * 100, 2) AS daily_change_pct,
                volume
            FROM daily_prices;
        """

        sql_view_quantum_radar = """
            CREATE VIEW IF NOT EXISTS view_quantum_radar AS
            WITH IndexChange AS (
                SELECT event_date, daily_change_pct as index_change
                FROM view_technical_signals
                WHERE symbol = 'XU100'
            )
            SELECT 
                t.symbol,
                t.event_date,
                t.close_price,
                t.daily_change_pct,
                i.index_change,
                (t.daily_change_pct - i.index_change) AS alpha
            FROM view_technical_signals t
            JOIN IndexChange i ON t.event_date = i.event_date;
        """
        
        try:
            self._conn = self.get_connection()
            self._conn.execute(sql_view_technical_signals)
            self._conn.execute(sql_view_quantum_radar)
            logger.info("MarketFlow Analysis Views injected successfully.")
        except Exception as e:
            logger.error(f"View Injection Error: {e}")
            raise

    def get_market_leaders(self, limit=10):
        """It brings in the most underperforming (Alpha) stocks from the index."""
        query = """
            SELECT symbol, close_price, daily_change_pct, alpha 
            FROM view_quantum_radar 
            WHERE event_date = (SELECT MAX(event_date) FROM view_quantum_radar)
            ORDER BY alpha DESC 
            LIMIT ?;
        """
        return self.get_connection().execute(query, [limit]).fetchdf()

    def close(self):
        
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB Connection Closed.")
    
    def get_connection(self):

        return self._connect()
    
    def _get_cursor(self):
        
        if not self._conn:
            self._connect()
        return self._conn.cursor()

class PortfolioDatabase:
    
    def __init__(self, db_path: str = "data/Portfolio.db"):
        
        self.db_path = db_path
        self._conn = None
        self._connect()
        
    def _connect(self):
        
        schema_exists = os.path.exists(self.db_path)
        if not self._conn:
            try:
                self._conn = sqlite3.connect(self.db_path)
                logger.info(f"Connected to Sqlite: {self.db_path}")
                if not schema_exists:
                    self._init_schema()
            except Exception as e:
                logger.error(f"Sqlite Connection Error: {e}")
                raise  

    def _init_schema(self):
        
        try:
            ### CREATE TABLES
           
            # TABLE: "Portfolio Assets"
            self.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_assets (
                        symbol TEXT PRIMARY KEY,           -- Unique Identifier
                        strategy_mode TEXT DEFAULT 'TANK', -- 'TANK' (Safe) or 'ATTACK' (Aggressive)
                        total_quantity INTEGER DEFAULT 0,  -- Current holding amount
                        
                        -- COST & PRICE TRACKING
                        average_cost INTEGER DEFAULT 0,
                        current_price INTEGER DEFAULT 0,
                        stop_loss INTEGER DEFAULT 0,        -- Safety exit price
                        target_price INTEGER DEFAULT 0,     -- Take profit price
                        
                        asset_type TEXT DEFAULT 'STOCK',
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            
            # TABLE: "Trade Logs"
            self.execute("""
                    CREATE TABLE trade_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,              -- e.g., 'TSKB', 'KONTR'
                        operation_type TEXT NOT NULL,      -- 'BUY' or 'SELL'
                        date TEXT NOT NULL,                -- Format: 'YYYY-MM-DD'
                        quantity INTEGER NOT NULL,         -- Number of shares
                        
                        -- PRICES IN INTEGER (Avoids floating point errors, add 6 digits after floating point)
                        -- Example: 10.50 TL -> Enter 10500000
                        price INTEGER NOT NULL,
                        
                        -- Auto-calculated total amount (Quantity * Price)
                        total_amount INTEGER GENERATED ALWAYS AS (quantity * price) VIRTUAL,
                        
                        commission INTEGER DEFAULT 0,       -- Bank commission fees
                        notes TEXT                          -- e.g., 'Entry for Low-Level Quantum Strategy'
                    );
                """)
            
            self.execute("""
                    CREATE TABLE weekly_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        total_portfolio_value INTEGER,
                        
                        -- WEEKLY PERFORMANCE
                        weekly_buy_volume INTEGER DEFAULT 0,
                        weekly_sell_volume INTEGER DEFAULT 0,
                        tx_count INTEGER DEFAULT 0,                 -- Total trade count
                        
                        -- DIFFERENCE COMPARED TO LAST WEEK (Delta)
                        value_change_from_last_week INTEGER DEFAULT 0
                    );
                """)
            
            self.execute("""
                    CREATE VIEW view_portfolio_summary AS
                        SELECT 
                            symbol,
                            total_quantity,
                            
                            ROUND(average_cost / 1000000.0, 2) as average_cost,
                            ROUND(current_price / 1000000.0, 2) as current_price,
                            
                            ROUND((total_quantity * current_price) / 1000000.0, 2) as market_value,
                            
                            ROUND(((current_price - average_cost) * total_quantity) / 1000000.0, 2) as profit_loss,
                            
                            CASE 
                                WHEN average_cost > 0 THEN 
                                    ROUND(
                                        ((current_price - average_cost) * 1.0 / average_cost) * 100, 
                                        2
                                    )
                                ELSE 0 
                            END as profit_loss_pct,
                            
                            last_updated
                            
                        FROM portfolio_assets
                        WHERE total_quantity > 0
                        ORDER BY market_value_tl DESC
                """)
            
            self.execute("""
                    CREATE VIEW view_weekly_report AS
                        SELECT 
                            id,
                            date(report_date) as Tarih,
                            (total_portfolio_value / 1000000.0) as Total_Portfolio_Value,
                            (value_change_from_last_week / 1000000.0) as Weekly_Change,
                            (weekly_buy_volume / 1000000.0) as Buy_Volume_TL,
                            (weekly_sell_volume / 1000000.0) as Sell_Volume_TL,
                            tx_count as Trade_Count
                        FROM weekly_snapshots
                        ORDER BY id DESC
                """)
            
            self._conn.commit()
            logger.info("Sqlite Schema initialized (STRICT INTEGER MODE).")
            
        except Exception as e:
            logger.error(f"Sqlite Schema Initialization Error: {e}")
            raise

    def close(self):
        
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Sqlite Connection Closed.")
            
    def get_connection(self):

        if not self._conn:
            self._connect()
        return self._conn
    
    def _get_cursor(self):
        
        if not self._conn:
            self._connect()
        return self._conn.cursor()
