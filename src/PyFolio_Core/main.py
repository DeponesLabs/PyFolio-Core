from PyFolio_Core.core.Interfaces import StockService
from PyFolio_Core.core.enums import Exchange

from PyFolio_Core.core.StockService import TradingViewService
from PyFolio_Core.core.FundService import FundDataService

DB_PATH = "data/portfolio.db"

if __name__ == "__main__":
    
    # ******************************************************************
    # Stock Market
    # ******************************************************************
    stock_bot: StockService = TradingViewService(DB_PATH, exchange=Exchange.BIST)

    # Update single symbol
    unit_price = stock_bot.fetch_price("THYAO")
    if unit_price:
        stock_bot.update_single_stock("THYAO", unit_price)

    # Update all
    stock_bot.run_full_update()
    print("-" * 30)
    # ******************************************************************
    # Fund Service
    # ******************************************************************
    service = FundDataService(DB_PATH)
    service.update_portfolio_funds()
