from enum import Enum

class Exchange(Enum):
    """
    Exchange codes supported by TradingView.
    String values ​​are the exact equivalents expected by TradingView.
    """
    BIST = "BIST"       # Borsa İstanbul
    NASDAQ = "NASDAQ"   # ABD Teknoloji Borsası
    NYSE = "NYSE"       # New York Borsası
    AMEX = "AMEX"       # American Stock Exchange
    LSE = "LSE"         # London Stock Exchange
    XETRA = "XETRA"     # Almanya Borsası
    BINANCE = "BINANCE" # Kripto Para (Opsiyonel)
    FOREX = "FX_IDC"    # Döviz Kurları (Opsiyonel)

    @classmethod
    def list_all(cls):
        return [e.value for e in cls]
