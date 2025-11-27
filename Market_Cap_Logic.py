import requests
import pandas as pd
import time

scrip_map = {
 'Adtiya Birla Real estate Ltd': 500040,
 'PHOENIX MILLS': 503100,
 'Marathon Nextgen Realty Ltd.': 503101,
 'UNITECH': 507878,
 'Sunteck Realty': 512179,
 'Ajmera Realty & Infra India Ltd': 513349,
 'ANANT RAJ': 515055,
 'ASHIANA HOUSING': 523716,
 'Ganesh Housing Ltd.': 526367,
 'MAHINDRA LIFESPACES': 532313,
 'Welspun Enterprise Ltd': 532553,
 'SOBHA LTD.': 532784,
 'Hubtown Ltd.': 532799,
 'Ahluwalia Contracts (India) Ltd.': 532811,
 'Embassy Development Ltd': 532832,
 'DLF': 532868,
 'HDIL': 532873,
 'OMAXE': 532880,
 'Puravankara': 532891,
 'KOLTE PATIL LTD.': 532924,
 'BRIGADE ENTERPRISES': 532929,
 'GODREJ PROPERTIES Ltd.': 533150,
 'Valor Estate Ltd': 533160,
 'OBEROI REALTY': 533273,
 'PRESTIGE ESTATE': 533274,
 'NBCC (India) Ltd.': 534309,
 'EMBASSY': 542602,
 'MINDSPACE': 543217,
 'Hemisphere Properties Ltd.': 543242,
 'TARC Ltd.': 543249,
 'BROOKFIELD': 543261,
 'Lodha Macrotech': 543287,
 'RUSTOMJEE KEYSTONE': 543669,
 'Signature Global': 543990,
 'Max Estate Ltd': 544008,
 'Suraj Estate': 544054,
 'Arkade Developers': 544261,
 'Raymond Realty': 544420,
 'Kalpataru Ltd': 544423,
 'Sri Lotus Developers': 544469,
 'Knowledge Realty Trust': 544481
 }

def bse_stock_api(scripcode):
    url = "https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w"

    params = {
        "flag": "",
        "quotetype": "EQ",
        "scripcode": str(scripcode)
    }

    headers = {
        "sec-ch-ua-platform": "\"Android\"",
        "Referer": "https://www.bseindia.com/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Chromium\";v=\"142\"",
        "DNT": "1",
        "sec-ch-ua-mobile": "?1"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    
    except Exception as e:
        print(f"❌ Error fetching BSE data for {scripcode}: {e}")
        return None


def extract_marketcap(data):
    if not data:
        return None
    print(data['MktCapFull'])
    return data['MktCapFull'], data['MktCapFF']

def add_marketcap_column(df, max_retries=3):

    attempt = 1

    while attempt <= max_retries:
        print(f"\n🔁 add_marketcap_column Attempt {attempt}/{max_retries}\n")

        try:
            df["Market Cap"] = None
            df["Market FF"] = None

            for idx, row in df.iterrows():
                company = row["Company"]
                scripcode = scrip_map.get(company)

                time.sleep(2)

                if scripcode:
                    try:
                        data = bse_stock_api(scripcode)   # may raise HTTPError
                    except requests.exceptions.HTTPError as e:
                        print(f"\n❌ HTTP ERROR for {company}: {e}")
                        print("🔄 Restarting entire add_marketcap_column...\n")
                        raise   # trigger retry by going to outer except

                    marketcap, marketFF = extract_marketcap(data)

                    df.at[idx, "Market Cap"] = marketcap
                    df.at[idx, "Market FF"] = marketFF

            # If completed without error → return normally
            return df

        except requests.exceptions.HTTPError:
            attempt += 1
            time.sleep(2)

    print("⛔ All retries failed inside add_marketcap_column.")
    return df
