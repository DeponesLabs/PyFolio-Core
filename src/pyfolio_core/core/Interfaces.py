from abc import ABC, abstractmethod
from typing import Optional, List

class StockService(ABC):
    """
    The primary interface used for retrieving and processing stock data. 
    All stock exchange services (TradingView, Yahoo, Bloomberg, etc.) must adhere to these rules.
    """
    @abstractmethod
    def fetch_price(self, symbol: str) -> Optional[float]:
        """
        Returns the price of a single share. 
        :param symbol: Share symbol (e.g., THYAO)
        :return: Price (float) or None
        """
        pass
    
    @abstractmethod
    def update_portfolio_prices(self) -> None:
        """
        Scans the database and updates the prices of ALL assets 
        (asset_type='STOCK') in the portfolio (Mass Update).
        """
        pass
    
    @abstractmethod
    def update_single_price(self, symbol: str) -> bool:
        """
        It only updates and writes the price of a single specified stock to the DB. 
        Scenario: Instead of scanning the entire list when a user adds a new stock, 
        it is used to update only that stock.
        """
        pass

    @abstractmethod
    def sync_market_snapshot(self) -> None:
        """
        Cron Job Method:
        It scans ALL stocks on the selected exchange (self.exchange), extracts 
        the last daily closing (OHLC) data, and writes it to the 'daily_prices' table.
        """
        pass
    
    @abstractmethod
    def get_available_tickers(self) -> List[str]:
        """
        Returns a list of all available stocks on the exchange.
        Usage: Required for 'Autocomplete' in the UI or for the stock search box.
        """
        pass
