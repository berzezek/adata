import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

api_endpoints = {
    "info": "api/company/info/",
    "profit": "api/company/profit/",
    "tax": "api/company/tax/",
    "estimated-wage-fund": "api/company/estimated-wage-fund/",
    "tax-deduction_kbk": "api/company/tax-deduction/kbk/",
    "tax-deduction_extended": "api/company/tax-deduction/extended/",
    "tax-deduction_dynamics": "api/company/tax-deduction/dynamics/",
    "rating": "api/company/rating/",
    "market-dynamics": "api/company/market-dynamics/",
    "contract_status": "api/company/contract/status/",
}

proxy_url = os.getenv("HTTPS_PROXY")
base_url = "https://api.adata.kz/"
API_TOKEN = os.getenv("API_TOKEN")
batch_size = int(os.getenv("BATCH_SIZE"))
ERROR_LOG_FILE = "error_log.json"

df = pd.read_excel("data.xlsx")
# print(df, df.shape)

exclude = pd.read_csv("exclude.csv", dtype={"bin": str})
# print(exclude, exclude.shape)

df = df[~df['bin'].isin(exclude['bin'])]
# print(df, df.shape)


def send_request(api_name, api_endpoint, bin, retries=3, delay=30):
    """Отправляет GET-запрос к API через прокси"""
    url = f"{base_url}{api_endpoint}{API_TOKEN}?iinBin={bin}"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, proxies={"https": proxy_url}, verify=False, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and "token" in data:
                    token = data["token"]
                    return {"bin": bin, "endpoint": api_name, "token": token}
                else:
                    return {"error": f"Success response received, but no token found for BIN {bin}"}
            elif response.status_code == 404:
                return {"bin": bin, "endpoint": api_name, "token": None, "status": "not_found"}
            # elif response.status_code in (500, 504):
            else:
                log_error({"bin": bin, "endpoint": api_name, "status": response.status_code, "error": "Server error or timeout"})
                print(f"⚠️ Попытка {attempt + 1}/{retries}: Сервер вернул {response.status_code} (BIN {bin})")
            # else:
            #     return {"error": f"Request failed with status {response.status_code}"}
        except requests.exceptions.Timeout:
            log_error({"bin": bin, "endpoint": api_name, "error": "Timeout error"})
            print(f"⏳ Тайм-аут для BIN {bin}. Попытка {attempt + 1}/{retries}...")
        except requests.exceptions.RequestException as e:
            log_error({"bin": bin, "endpoint": api_name, "error": f"Client error: {e}"})
            print(f"⚠️ Клиентская ошибка: {e}. Попытка {attempt + 1}/{retries}...")

        if attempt < retries - 1:  # Задержка перед новой попыткой
            print(f"🔄 Ожидание {delay} секунд перед повторной попыткой...")
            time.sleep(delay)
    
    return {"bin": bin, "endpoint": api_name, "error": "Failed after retries"}

def process_batches(start_index, end_index):
    """Обрабатывает запросы пакетами по batch_size"""
    responses = []
    processed_bins = 0

    try:
        bins = df['bin'][start_index:end_index].tolist()

        for i in range(0, len(bins), batch_size):
            start_time = time.monotonic()

            batch = bins[i:i + batch_size]  # Выбираем 100 BIN-ов

            for bin_value in batch:
                for key, value in api_endpoints.items():
                    response = send_request(key, value, bin_value)
                    responses.append(response)

                    # Ожидание в 1 секунду между запросами
                    # time.sleep(1)

                processed_bins += 1
            print(f"✅ Завершено {processed_bins}/{len(bins)} BIN-ов")

            elapsed_time = time.monotonic() - start_time  # Вычисляем время выполнения батча

            # Если батч закончился быстрее 60 секунд, ждем оставшееся время
            if elapsed_time < 60:
                remaining_time = 60 - elapsed_time
                print(f"⏳ Ожидание {remaining_time:.2f} секунд для соблюдения минимального времени батча...")
                time.sleep(remaining_time)

    except Exception as e:
        print(f"⚠️ Произошла ошибка: {e}")
    finally:
        # Сохранение данных в файл
        print("📄 Сохранение данных при завершении...")
        save_responses_to_file(responses)
        print("\n✅ Данные записаны!")

def save_responses_to_file(response_list, filename="response_tokens.json"):
    """Записывает список ответов в JSON-файл"""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            existing_data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    existing_data.extend(response_list)

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(existing_data, file, ensure_ascii=False, indent=4)

def log_error(error_data):
    """Добавляет ошибки в лог-файл"""
    try:
        with open(ERROR_LOG_FILE, "r", encoding="utf-8") as file:
            existing_errors = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_errors = []

    existing_errors.append(error_data)

    with open(ERROR_LOG_FILE, "w", encoding="utf-8") as file:
        json.dump(existing_errors, file, ensure_ascii=False, indent=4)

def get_tokens(start_index, end_index):
    process_batches(start_index, end_index)

if __name__ == "__main__":
    start_index = int(input("Введите начальный индекс BIN: "))
    end_index = int(input("Введите конечный индекс BIN: "))
    get_tokens(start_index, end_index)
