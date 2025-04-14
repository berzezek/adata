import json
import aiohttp
import asyncio
import os
from dotenv import load_dotenv
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

proxie_urls = {
    "http": os.getenv("HTTP_PROXY"),
    "https": os.getenv("HTTPS_PROXY"),
}

base_url = "https://api.adata.kz/api/company/info/check/"
API_TOKEN = os.getenv("API_TOKEN")
batch_size = int(os.getenv("BATCH_SIZE"))*10
ERROR_LOG_FILE = "response_tokens/error_log_token_responses.json"

import requests
import time

def send_get_request(token, bin, endpoint, proxies, retries=3, delay=30):
    """Отправляет GET-запрос и возвращает данные для последующей записи"""
    url = f"{base_url}{API_TOKEN}?token={token}"
    for attempt in range(retries):
        try:
            response = requests.get(url, proxies=proxies, verify=False, timeout=30)
            if response.status_code == 200:
                response_json = response.json()
                message = response_json.get("message", "")

                if message == "wait":
                    return {"bin": bin, "endpoint": endpoint, "token": token, "status": "wait"}
                elif message == "ready":
                    data = response_json.get("data", {})
                    return {"bin": bin, "endpoint": endpoint, "data": data, "status": "ready"}
            elif response.status_code == 404:
                return {"bin": bin, "endpoint": endpoint, "token": token, "status": "not_found"}
            else:
                log_error({"bin": bin, "endpoint": endpoint, "status": response.status_code, "error": "Server error or timeout"})
                print(f"⚠️ Попытка {attempt + 1}/{retries}: Сервер вернул {response.status_code} (BIN {bin})")
        except requests.exceptions.Timeout:
            log_error({"bin": bin, "endpoint": endpoint, "error": "Timeout error"})
            print(f"⏳ Тайм-аут для BIN {bin}. Попытка {attempt + 1}/{retries}...")
        except requests.exceptions.RequestException as e:
            log_error({"bin": bin, "endpoint": endpoint, "error": f"Client error: {e}"})
            print(f"⚠️ Клиентская ошибка: {e}. Попытка {attempt + 1}/{retries}...")

        if attempt < retries - 1:  # Задержка перед новой попыткой
            print(f"🔄 Ожидание {delay} секунд перед повторной попыткой...")
            time.sleep(delay)

    return {"bin": bin, "endpoint": endpoint, "status": "Failed after retries"}


def process_batches(start_index, end_index, proxies):
    """Обрабатывает запросы пакетами по batch_size"""
    responses = load_responses(f"response_tokens/response_tokens_{start_index}_{end_index}.json")
    if not isinstance(responses, list):
        print("Ошибка загрузки данных из response_tokens.json")
        return

    wait_data = []
    ready_data = []
    not_found_data = []
    error = []

    try:
        for i in range(0, len(responses), batch_size):
            try:
                start_time = time.monotonic()

                batch = responses[i:i + batch_size]  # Берем по batch_size записей

                for response in batch:
                    # Проверяем, что токен, BIN и endpoint существуют
                    if response.get("token") and response.get("bin") and response.get("endpoint"):
                        # Выполняем запрос для каждого BIN
                        result = send_get_request(response["token"], response["bin"], response["endpoint"], proxies)
                        if result:
                            if result["status"] == "wait":
                                wait_data.append(result)
                            elif result["status"] == "ready":
                                ready_data.append(result)
                            elif result["status"] == "not_found":
                                not_found_data.append(result)
                            else:
                                error.append(result)

                        # Добавляем паузу в 1 секунду между запросами
                        # time.sleep(1)
                print(f"✅ Завершено {min(i + batch_size, len(responses))}/{len(responses)} записей.")

                elapsed_time = time.monotonic() - start_time  # Вычисляем время выполнения батча

                # Ждём, если выполнение заняло меньше 1 минуты
                if elapsed_time < 60:
                    remaining_time = 60 - elapsed_time
                    print(f"⏳ Ожидание {remaining_time:.2f} секунд для соблюдения минимального времени батча...")
                    time.sleep(remaining_time)
            except Exception as e:
                print(f"⚠️ Произошла ошибка: {e}")

    except Exception as e:
        print(f"⚠️ Произошла ошибка: {e}")
    finally:
        save_responses_to_file(f"results/wait_{start_index}_{end_index}.json", wait_data)
        save_responses_to_file(f"results/responses_{start_index}_{end_index}.json", ready_data)
        save_responses_to_file(f"results/fl_{start_index}_{end_index}.json", not_found_data)
        save_responses_to_file(f"results/error_{start_index}_{end_index}.json", error)
        print("\n✅ Все запросы обработаны!")


def save_responses_to_file(filename, data_list):
    """Записывает массив данных в JSON-файл"""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            existing_data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []

    existing_data.extend(data_list)

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

def load_responses(filename="response_tokens/response_tokens.json"):
    """Читает JSON-файл и загружает данные"""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            response_data = json.load(file)
        return response_data
    except FileNotFoundError:
        return {"error": f"Файл '{filename}' не найден"}
    except json.JSONDecodeError:
        return {"error": "Ошибка декодирования JSON"}

def get_data(start_index, end_index, proxies):
    process_batches(start_index, end_index, proxies)

if __name__ == "__main__":
    start_index = int(input("Введите начальный индекс BIN: "))
    end_index = int(input("Введите конечный индекс BIN: "))
    get_data(start_index, end_index, proxie_urls)
