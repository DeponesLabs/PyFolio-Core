from pyfolio_core.core.Interfaces import StockService
from pyfolio_core.core.enums import Exchange

from pyfolio_core.core.services import TradingViewService
from pyfolio_core.core.FundService import FundDataService

from pyfolio_core.core.logging import setup_global_logging

PFOLIO_DB_PATH = "/home/valjean/Documents/Databases/Portfolio/sqlite/Portfolio.db"
MARKET_DB_PATH = "/home/valjean/Documents/Databases/Portfolio/duckdb/GlobalMarket.duckdb"

setup_global_logging()

if __name__ == "__main__":
    
    # ******************************************************************
    # Stock Market
    # ******************************************************************
    stock_bot: StockService = TradingViewService(MARKET_DB_PATH, PFOLIO_DB_PATH, exchange=Exchange.BIST)
    # tickers = stock_bot.sync_market_snapshot()
    # stock_bot.run_full_update()
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
