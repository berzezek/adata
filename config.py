from dotenv import dotenv_values

config = dotenv_values(".env")

URL = config["URL"]

tokens_auth_list = config["TOKENS"].split(",")

CHECK_URL = config["CHECK_URL"]

MAX_RETRIES = int(config["MAX_RETRIES"])
RETRY_INTERVAL = int(config["RETRY_INTERVAL"])

EXCEL_FILE = "example.xlsx"
OUTPUT_FILE = "output_example.xlsx"
