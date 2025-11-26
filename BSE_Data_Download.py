import requests

def send_conversion_request():
    url = "https://www.google-analytics.com/privacy-sandbox/register-conversion"

    params = {
        "_c": "1",
        "cid": "1422545866.1763358175",
        "dbk": "17465179590646983565",
        "dma": "0",
        "en": "file_download",
        "gtm": "45je5bi1h1v888160933za200zb851866778zd851866778",
        "npa": "0",
        "tid": "G-TM52BJH9HF",
        "dl": "https://www.bseindia.com?"
    }

    headers = {
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "dnt": "1",
        "priority": "u=1, i",
        "referer": "https://www.bseindia.com/",
        "sec-ch-ua": '"Not_A Brand";v="99", "Chromium";v="142"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "sec-fetch-storage-access": "active",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36",
    }

    cookies = {
        "ar_debug": "1"
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            timeout=10  # seconds
        )

        # Check HTTP status
        response.raise_for_status()

        print("Request successful.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)

    except requests.exceptions.Timeout:
        print("Error: The request timed out.")

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err} (Status Code: {response.status_code})")

    except requests.exceptions.ConnectionError:
        print("Error: Connection error occurred.")

    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred: {err}")


send_conversion_request()

