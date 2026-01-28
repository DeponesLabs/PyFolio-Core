import os
from dotenv import load_dotenv
from tvDatafeed import TvDatafeed, Interval

# 1. Şifreleri yükle
load_dotenv()
user = os.getenv("TV_USERNAME")
password = os.getenv("TV_PASSWORD")

print("🔌 Connecting TradingView...")

# 2. Bağlantı (Login olamazsa Guest modunda dener)
if user and password:
    tv = TvDatafeed(username=user, password=password)
    print("Authenticated.")
else:
    tv = TvDatafeed()
    print("Username not found, connecting in Guest mode.")

# 3. Veri Çekme (BIST:THYAO)
print("THYAO data is requested...")
try:
    # Exchange 'BIST' olarak belirtilmeli
    df = tv.get_hist(symbol='THYAO', exchange='BIST', interval=Interval.in_daily, n_bars=100)
    
    if df is not None and not df.empty:
        print("SUCCESSFUL! Top 5 data points:")
        print(df.head())
        print(f"\nTotal data point counts: {len(df)}")
    else:
        print("The data was returned empty. It could be a symbol or exchange error.")

except Exception as e:
    print(f"💥 An error occurred: {e}")