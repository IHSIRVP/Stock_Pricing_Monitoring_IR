import requests
import os
import pandas as pd 

def Today_BSE_Bhav(date_parm):
    today_str = date_parm.strftime('%Y%m%d')
    url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{today_str}_F_0000.CSV"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bseindia.com/",
        "Accept": "*/*",
    }

    # Create folder if it doesn't exist
    save_folder = os.path.join(".", "BSEDATA")
    os.makedirs(save_folder, exist_ok=True)

    save_path = os.path.join(save_folder, "BSE_bhavcopy_TODAY.csv")

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)

        print("CSV downloaded successfully!")
        print("Saved at:", save_path)

        # READ FROM SAME RELATIVE PATH
        return pd.read_csv(save_path)

    else:
        print("Failed:", response.status_code, response.text[:200])
        return None
