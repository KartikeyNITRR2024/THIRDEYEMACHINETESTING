import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from dateutil import parser
import logging

# Constants
UNIQUEID = 126
MACHINE_NO = int(os.getenv("MACHINE_NO", 1))+4

# API Endpoints
STOCKS_MANAGER_BASE_URL = os.getenv("STOCKS_MANAGER_BASE_URL", "https://google.com")
STOCK_MARKET_VIEWER_BASE_URL = os.getenv("STOCK_MARKET_VIEWER_BASE_URL", "https://google.com")
MORNING_PRICE_UPDATER_BASE_URL = os.getenv("MORNING_PRICE_UPDATER_BASE_URL", "https://google.com")
STOCKBATCH_API = f"{STOCKS_MANAGER_BASE_URL}/api/stocksbatch/{UNIQUEID}/stocksbatch/{MACHINE_NO}"
LIVE_STOCK_API = "https://www.google.com/finance/quote/"
SEND_DATA_API = f"{STOCK_MARKET_VIEWER_BASE_URL}/api/livemarket/124/Machine{MACHINE_NO}"
MORNING_API = f"{MORNING_PRICE_UPDATER_BASE_URL}/api/morningstockprice/morninglivemarketdata/130/Machine{MACHINE_NO}/{MACHINE_NO}"

TIME_TO_SEND_PAYLOAD = None
TIME_TO_SEND_MORNINGDATA = None
live_stock_payloads = []

# Set up logging
class CustomLogFilter(logging.Filter):
    def filter(self, record):
        record.machine_no = f"LivestockMachine_{MACHINE_NO}"
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(machine_no)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.getLogger().addFilter(CustomLogFilter())

class LiveStockPayload:
    def __init__(self, batch_id, stock_id, time, price):
        self.batch_id = batch_id
        self.stock_id = stock_id
        self.time = time
        self.price = price

    def to_dict(self):
        return {
            "batchId": self.batch_id,
            "stockId": self.stock_id,
            "time": self.time.isoformat(),
            "price": self.price
        }

    def __repr__(self):
        return f"LiveStockPayload(batchId={self.batch_id}, stockId={self.stock_id}, time={self.time}, price={self.price})"

stocklist = []

def fetch_api_data(api_url):
    logging.info("Fetching live stock data to send.")
    global stocklist
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        stocklist = [[key, value] for key, value in response.json().items()]
        return True
    except Exception as e:
        logging.error(f"Error fetching API data: {e}")
        return False

def fetch_live_stock_info(key, value):
    stock_info = value.split(" ")
    url = f"{LIVE_STOCK_API}{stock_info[0]}:{stock_info[1]}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            class1 = "YMlKec fxKbKc"
            price_element = soup.find(class_=class1)
            if price_element:
                price_text = price_element.text.strip()
                try:
                    price = float(price_text[1:].replace(",", ""))
                    payload = LiveStockPayload(batch_id=MACHINE_NO, stock_id=key, time=datetime.now(), price=price)
                    live_stock_payloads.append(payload)
                except ValueError:
                    logging.error(f"Could not parse price for {key}: {price_text}")
            else:
                logging.error(f"Price element not found for {key}")
        else:
            logging.error(f"Failed to fetch price for {key}. Status code: {response.status_code}")
    except requests.RequestException as e:
        logging.error(f"Error fetching price for {key}: {e}")

def create_time_to_send_payload():
    global TIME_TO_SEND_PAYLOAD, TIME_TO_SEND_MORNINGDATA
    current_time = datetime.now(timezone.utc)
    next_minute = (current_time + timedelta(minutes=1)).replace(second=0, microsecond=0)
    TIME_TO_SEND_PAYLOAD = next_minute + timedelta(seconds=MACHINE_NO - 1)
    TIME_TO_SEND_MORNINGDATA = TIME_TO_SEND_PAYLOAD
    logging.info(f"Updated TIME_TO_SEND_PAYLOAD: {TIME_TO_SEND_PAYLOAD}")
    logging.info(f"Updated TIME_TO_SEND_MORNINGDATA: {TIME_TO_SEND_MORNINGDATA}")

def update_time_to_send_payload(next_time):
    global TIME_TO_SEND_PAYLOAD
    TIME_TO_SEND_PAYLOAD = parser.isoparse(next_time) - timedelta(hours=5, minutes=30)
    logging.info(f"Updated TIME_TO_SEND_PAYLOAD: {TIME_TO_SEND_PAYLOAD}")

def update_time_to_send_morning_payload(next_time):
    global TIME_TO_SEND_MORNINGDATA
    TIME_TO_SEND_MORNINGDATA = parser.isoparse(next_time) - timedelta(hours=5, minutes=30)
    logging.info(f"Updated TIME_TO_SEND_MORNINGDATA: {TIME_TO_SEND_MORNINGDATA}")

def send_live_market_data():
    global TIME_TO_SEND_PAYLOAD, live_stock_payloads
    if not live_stock_payloads:
        logging.info("No live stock data to send.")
        return

    payload_data = [payload.to_dict() for payload in live_stock_payloads]

    if TIME_TO_SEND_PAYLOAD is None:
        create_time_to_send_payload()

    while datetime.now(timezone.utc) < TIME_TO_SEND_PAYLOAD:
        remaining_time = (TIME_TO_SEND_PAYLOAD - datetime.now(timezone.utc)).total_seconds()
        if remaining_time > 0:
            logging.info(f"Remaining Time: {remaining_time:.2f} seconds")
            time.sleep(min(60, remaining_time))

    try:
        response = requests.post(SEND_DATA_API, json=payload_data)
        response.raise_for_status()
        logging.info(f"Successfully sent live stock data. Response: {response.json()}")
        next_time = response.json().get('nextIterationTime')
        live_stock_payloads = []
        if next_time:
            update_time_to_send_payload(next_time)
    except requests.RequestException as e:
        logging.error(f"Error sending live stock data: {e}")

if __name__ == "__main__":
    if fetch_api_data(STOCKBATCH_API):
        with ThreadPoolExecutor(max_workers=len(stocklist)) as executor:
            while True:
                futures = [executor.submit(fetch_live_stock_info, key, value) for key, value in stocklist]
                for future in as_completed(futures):
                    future.result()
                send_live_market_data()
    else:
        logging.error("Failed to initialize stock data.")
