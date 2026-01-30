from pyfolio_core.core.Interfaces import StockService
from pyfolio_core.core.enums import Exchange

from pyfolio_core.core.StockService import TradingViewService
from pyfolio_core.core.FundService import FundDataService

SQLITE_PATH = "/home/valjean/Documents/Databases/Portfolio/sqlite/portfolio.db"
DUCKDB_PATH = "/home/valjean/Documents/Databases/Portfolio/duckdb/portfolio.duckdb"

if __name__ == "__main__":
    
    # ******************************************************************
    # Stock Market
    # ******************************************************************
    stock_bot: StockService = TradingViewService(DUCKDB_PATH, exchange=Exchange.BIST)
    stock_bot.run_full_update()
    # Update single symbol
    # unit_price = stock_bot.fetch_price("THYAO")
    # if unit_price:
    #     stock_bot.update_single_stock("THYAO", unit_price)

    # # Update all
    # stock_bot.run_full_update()
    # print("-" * 30)
    # ******************************************************************
    # Fund Service
    # ******************************************************************
    # service = FundDataService(DB_PATH)
    # service.update_portfolio_funds()

    df_assets = stock_bot.get_all_bist_symbols()
    if df_assets is not None:
        print(df_assets.head(10))
        print("\nTam liste hafızada hazır.")