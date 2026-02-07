from dataclasses import dataclass, field
from datetime import datetime, date
import pandas as pd

from pyfolio_core.core.constants import SCALING_FACTOR

@dataclass(slots=True)
class StockValue:
    """
    An immutable value object that represents the price movement 
    of a single stock over a single day.
    """
    symbol: str
    _event_date: date = field(default_factory=date.today)
    query_time: datetime = field(default_factory=datetime.now)
    
    _open: int = 0
    _high: int = 0
    _low: int = 0
    _close: int = 0
    volume: float = 0.0
    
    @property
    def event_date(self) -> date:
        return self._event_date

    @event_date.setter
    def event_date(self, value):
        if isinstance(value, datetime):
            self._event_date = value.date()
        elif isinstance(value, str):
            self._event_date = datetime.strptime(value, '%Y-%m-%d').date()
        else:
            self._event_date = value
            
    @property
    def close(self) -> float:
        return self._close / SCALING_FACTOR

    @close.setter
    def close(self, value: float):
        self._close = int(round(value * SCALING_FACTOR))

    @property
    def high(self) -> float:
        return self._high / SCALING_FACTOR

    @high.setter
    def high(self, value: float):
        self._high = int(round(value * SCALING_FACTOR))

    @property
    def low(self) -> float:
        return self._low / SCALING_FACTOR

    @low.setter
    def low(self, value: float):
        self._low = int(round(value * SCALING_FACTOR))

    @property
    def open(self) -> float:
        return self._open / SCALING_FACTOR

    @open.setter
    def open(self, value: float):
        self._open = int(round(value * SCALING_FACTOR))

    @classmethod
    def from_tv_dataframe(cls, symbol: str, row) -> 'StockValue':
        
        obj = cls(symbol=symbol)
        obj.event_date = row.Index 
        obj.open = float(row.open)
        obj.high = float(row.high)
        obj.low = float(row.low)
        obj.close = float(row.close)
        obj.volume = float(row.volume)
        return obj

    def to_tuple(self) -> tuple:
        """It returns an ordered tuple to write to the database."""
        return (self.symbol, self.event_date, self.open, self.high, self.low, self.close, self.volume)

    def to_dict(self) -> dict:
        """ 
        Returns dict for JSONL serialization.
        For JSONL and UI: Returns float values.
        """
        return {
            "symbol": self.symbol,
            "event_date": self.event_date.isoformat(), # YYYY-MM-DD
            "query_time": self.query_time.isoformat(),  # YYYY-MM-DD HH:MM:SS.ms
            "open": self._open,
            "high": self._high,
            "low": self._low,
            "close": self._close,
            "volume": self.volume
        }
        
    def to_db_row(self) -> dict:
        """For DuckDB: It returns raw integer values."""
        return {
            "symbol": self.symbol,
            "event_date": self.event_date,
            "query_time": self.query_time,
            "open": self._open,
            "high": self._high,
            "low": self._low,
            "close": self._close, # 31700000
            "volume": self.volume
        }

