from dataclasses import dataclass
from datetime import datetime, date
from pyfolio_core.core.constants import SCALING_FACTOR

@dataclass(slots=True)
class StockValue:
    """
    An immutable value object that represents the price movement 
    of a single stock over a single day.
    """
    symbol: str
    event_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_tv_dataframe(cls, symbol: str, row) -> 'StockValue':
        """
        Factory Method: Creates an object from the Pandas line. 
        """
        # Convert Pandas Timestamp to Python date
        py_date = row.Index.strftime('%Y-%m-%d')
        
        return cls(
            symbol=symbol,
            event_date=py_date,
            open=int(float(row.open) * SCALING_FACTOR),
            high=int(float(row.high) * SCALING_FACTOR),
            low=int(float(row.low) * SCALING_FACTOR),
            close=int(float(row.close) * SCALING_FACTOR),
            volume=float(row.volume)
        )

    def to_tuple(self) -> tuple:
        """It returns an ordered tuple to write to the database."""
        return (
            self.symbol, 
            self.event_date, 
            self.open, 
            self.high, 
            self.low, 
            self.close, 
            self.volume
        )
        
    def to_int(self, value: float) -> int:
        return int(round(value * SCALING_FACTOR))

    def to_float(self, value: int) -> float:
        return value / float(SCALING_FACTOR)
