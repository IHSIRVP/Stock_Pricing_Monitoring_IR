import requests

def get_52week_high_low(symbol: str):
    """
    Fetch 52-week high and low from NSE for the given symbol.
    Returns (high, low) or (None, None) if data not available.
    """

    url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol.upper()}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
    }

    # Start a session to store cookies
    session = requests.Session()
    # First request to set cookies
    session.get("https://www.nseindia.com", headers=headers)

    # Actual API hit
    response = session.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error {response.status_code} for {symbol}")
        return None, None

    data = response.json()

    try:
        week_info = data["securityInfo"]["weekHighLow"]
        high = week_info.get("max")
        low = week_info.get("min")
        return high, low
    except:
        return None, None
