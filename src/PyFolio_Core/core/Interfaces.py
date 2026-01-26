from abc import ABC, abstractmethod
from typing import Optional

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
    def run_full_update(self) -> None:
        """
        It scans the database for relevant entities (asset_type='STOCK')
        and initiates the bulk update process.
        """
        pass