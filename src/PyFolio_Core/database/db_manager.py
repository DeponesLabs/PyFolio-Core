import duckdb
from pyfolio_core.core.domain_objects import StockValue

class DbManager:
    
    def init_database(db_path):
        
        conn = duckdb.connect(db_path)
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS daily_prices (
                symbol VARCHAR,
                event_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (symbol, event_date)
            )
        ''')
        return conn

    def save_stock_value(conn, stock: StockValue):
        
        try:
            conn.execute('''
                INSERT INTO daily_prices VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, event_date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            ''', stock.to_tuple())
            return True
        
        except Exception as error:
            print(f"DB Error ({stock.symbol}): {error}")
            return False
        
    def close(self):
        if self.conn:
            self.conn.close()