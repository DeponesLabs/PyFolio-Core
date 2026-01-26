import sqlite3

DB_PATH = "data/portfolio.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabloyu oluştur
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_assets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE NOT NULL,
        total_quantity REAL DEFAULT 0,
        average_cost_cents INTEGER DEFAULT 0,
        current_price_cents INTEGER DEFAULT 0,
        asset_type TEXT DEFAULT 'STOCK', -- 'STOCK' or 'FUND'
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Sample data (Optional)
    try:
        cursor.execute("INSERT INTO portfolio_assets (symbol, asset_type) VALUES ('THYAO', 'STOCK')")
        cursor.execute("INSERT INTO portfolio_assets (symbol, asset_type) VALUES ('AFT', 'FUND')")
        print("Database and sample data have been created.")
    except sqlite3.IntegrityError:
        print("Table already exists.")
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
