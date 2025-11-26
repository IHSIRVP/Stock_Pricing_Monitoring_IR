import requests
import os
from datetime import datetime, timedelta


def dataFunct(run_str, date_str, date_label):

    # Convert string → datetime
    current_date = datetime.strptime(date_str, "%Y%m%d")

    # Safety lower bound (NSE started ZIP format around 2000)
    earliest_date = datetime(2000, 1, 1).date()

    folder_name = f"data/RunDate-{run_str}/"
    os.makedirs(folder_name, exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    while True:

        # Date being tried
        date_try_str = current_date.strftime("%Y%m%d")        # for URL
        actual_date_fmt = current_date.strftime("%d-%m-%Y")   # for filename

        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_try_str}_F_0000.csv.zip"

        # Filename now uses DD-MM-YYYY
        output_file = f"{folder_name}/BhavCopy_{date_label}_{actual_date_fmt}.csv.zip"

        print(f"Trying date: {date_try_str} ...")

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            with open(output_file, "wb") as f:
                f.write(response.content)
            print(f"✅ Download successful: {output_file}")
            return response, output_file

        else:
            print(f"❌ Failed for {date_try_str}. Status:", response.status_code)

            # Stop if we hit earliest possible date
            if current_date.date() <= earliest_date:
                print("⛔ Reached earliest supported date. No further retry possible.")
                return None, None

            # Try previous day
            current_date += timedelta(days=1)
            print("⬅️ Trying previous day...")
