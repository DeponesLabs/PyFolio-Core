from enum import Enum

class Exchange(Enum):
    """
    Exchange codes supported by TradingView.
    String values ​​are the exact equivalents expected by TradingView.
    """
    BIST = "BIST"       # Borsa İstanbul
    NASDAQ = "NASDAQ"   # ABD Tech Market
    NYSE = "NYSE"       # New York Market
    AMEX = "AMEX"       # American Stock Exchange
    LSE = "LSE"         # London Stock Exchange
    XETRA = "XETRA"     # German Market
    BINANCE = "BINANCE" # Kripto Money (Optional)
    FOREX = "FX_IDC"    # Exchange Rates (Optional)

    @classmethod
    def list_all(cls):
        return [e.value for e in cls]
