from dataclasses import dataclass
from datetime import datetime, date

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
    def from_tv_dataframe(cls, symbol: str, df_row) -> 'StockValue':
        """
        Factory Method: Creates an object from the Pandas line. 
        """
        # Convert Pandas Timestamp to Python date
        py_date = df_row['datetime'].date() 
        
        return cls(
            symbol=symbol,
            event_date=py_date,
            open=float(df_row['open']),
            high=float(df_row['high']),
            low=float(df_row['low']),
            close=float(df_row['close']),
            volume=float(df_row['volume'])
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
