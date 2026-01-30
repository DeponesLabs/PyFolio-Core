import duckdb
import logging

logger = logging.getLogger("PyFolio-Core")

class LocalDatabase:
    
    def __init__(self, db_path: str = "data/portfolio.duckdb"):
        self.db_path = db_path
        self._conn = None
        self._connect()
        self._init_schema()
        
    def _connect(self):
        if not self._conn:
            try:
                self._conn = duckdb.connect(self.db_path)
                logger.info(f"Connected to DB: {self.db_path}")
            except Exception as e:
                logger.error(f"DB Connection Error: {e}")
                raise  

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DB Connection Closed.")
    
    def _get_connection(self):

        if not self._conn:
            self._connect()
        return self._conn
    
    def _get_cursor(self):
        
        if not self._conn:
            self._connect()
        return self._conn.cursor()
    
    def _init_schema(self):
        
        conn = self.get_connection()
        
        try:
        
            ### CREATE TABLES
            # TABLE: "DailyPrices"
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_prices (
                    symbol VARCHAR,
                    event_date DATE,
                    open_integer BIGINT,
                    high_integer BIGINT,
                    low_integer BIGINT,
                    close_integer BIGINT,
                    volume DOUBLE,        -- Hacim para değildir, adet/lot küsuratlı olabilir.
                    PRIMARY KEY (symbol, event_date)
                );
            """)
            
            # TABLE: "Portfolio Assets"
            conn.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_assets (
                        symbol VARCHAR PRIMARY KEY,
                        total_quantity DOUBLE DEFAULT 0,
                        average_cost_integer BIGINT DEFAULT 0,
                        current_price_integer BIGINT DEFAULT 0,
                        asset_type VARCHAR DEFAULT 'STOCK',
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            
            conn.commit()
            
            logger.info("Schema initialized (STRICT INTEGER MODE).")
            
        except Exception as e:
            logger.error(f"Schema Initialization Error: {e}")
            raise
