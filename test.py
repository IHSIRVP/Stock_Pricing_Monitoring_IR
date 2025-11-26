import pandas_market_calendars as mcal
import datetime as dt

# NSE Trading Calendar
nse = mcal.get_calendar('NSE')

def get_trading_day_n_days_back(today, n=63):
    schedule = nse.schedule(start_date=today - timedelta(days=200), end_date=today)
    trading_days = schedule.index.to_pydatetime()
    
    # Find the index of today or previous trading day
    idx = max(i for i, d in enumerate(trading_days) if d <= today)
    
    # Go back n days
    target_idx = idx - n
    
    return trading_days[target_idx].date()
