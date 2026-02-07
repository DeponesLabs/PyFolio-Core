import pytest

from pyfolio_core.core.database import MarketDatabase

def test_market_leaders():
    
    db_path = "/home/valjean/Documents/Databases/Portfolio/duckdb/GlobalMarket.duckdb"
    db = MarketDatabase(db_path)
    db.create_analysis_views()

    # Get Alpha Leaders
    leaders = db.get_market_leaders()
    
    print("--- TODAY'S OUTPERFORMING CANDIDATES (ALPHA) ---")
    print(leaders)
    
    assert leaders is not None
    