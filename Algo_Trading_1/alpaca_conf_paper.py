"""
Put secret Keys as environment variables
"""
import os

ALPACA_ENDPOINT = "https://paper-api.alpaca.markets"
ALPACA_KEYID = "PKS04YCTF0NJ57P1KBCC"
ALPACA_SECRETKEY = "6tCfGI4VLWGL8X40P5vwqieDW/VBaBbhTQ2X5HS8"

os.environ["APCA_API_KEY_ID"] = ALPACA_KEYID
os.environ["APCA_API_SECRET_KEY"] = ALPACA_SECRETKEY
os.environ["APCA_API_BASE_URL"] = ALPACA_ENDPOINT
