import requests
import os

def Today_BSE_Bhav(date_parm):
    today_str = date_parm.strftime('%Y%m%d')
    url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{today_str}_F_0000.CSV"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/142.0.0.0 Safari/537.36",
        "Referer": "https://www.bseindia.com/",
        "Sec-Fetch-Site": "same-origin",
        "Accept": "*/*",
    }

    # Create folder if it doesn't exist
    save_folder = "./BSEDATA"
    os.makedirs(save_folder, exist_ok=True)

    save_path = os.path.join(save_folder, "BSE_bhavcopy_TODAY.csv")

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open(save_path, "wb") as f:
            f.write(response.content)
        print("CSV downloaded successfully!")
        print("Saved at:", save_path)
    else:
        print("Failed:", response.status_code, response.text[:200])


