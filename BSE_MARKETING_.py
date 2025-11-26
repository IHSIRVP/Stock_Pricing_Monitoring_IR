import requests

url = "https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode=512179"

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin": "https://www.bseindia.com",
    "priority": "u=1, i",
    "referer": "https://www.bseindia.com/",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers)

print("Status Code:", response.status_code)

if response.status_code == 200:
    try:
        data = response.json()
        print(data)
    except Exception:
        print("Response is not JSON:")
        print(response.text)
else:
    print("Error:", response.text)
