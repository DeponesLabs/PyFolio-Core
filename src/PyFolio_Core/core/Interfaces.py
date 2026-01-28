from abc import ABC, abstractmethod
from typing import Optional
from enums import Exchange

class StockService(ABC):
    """
    The primary interface used for retrieving and processing stock data. 
    All stock exchange services (TradingView, Yahoo, Bloomberg, etc.) must adhere to these rules.
    """

    @abstractmethod
    def fetch_price(self, symbol: str, exchange: Exchange) -> Optional[float]:
        """
        Returns the price of a single share. 
        :param symbol: Share symbol (e.g., THYAO)
        :return: Price (float) or None
        """
        pass

    @abstractmethod
    def run_full_update(self) -> None:
        """
        It scans the database for relevant entities (asset_type='STOCK')
        and initiates the bulk update process.
        """
        pass
    
    @abstractmethod
    def fetch_market_daily_close(self, exchange: Exchange) -> None:
        """
        It scans ALL stocks on the specified exchange (e.g., BIST),
        retries the latest daily closing data, and writes it to the database. 
        (This is the method that will be called by the Cron Job)
        """
        pass
