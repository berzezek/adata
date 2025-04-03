import json
import aiohttp
import asyncio
import os
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

def load_responses(filename="response_tokens.json"):
    """Читает JSON-файл и загружает данные"""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            response_data = json.load(file)
        return response_data
    except FileNotFoundError:
        return {"error": f"Файл '{filename}' не найден"}
    except json.JSONDecodeError:
        return {"error": "Ошибка декодирования JSON"}

proxies = {
    "http": os.getenv("HTTP_PROXY"),
    "https": os.getenv("HTTPS_PROXY"),
}

base_url = "https://api.adata.kz/api/company/info/check/"
API_TOKEN = os.getenv("API_TOKEN")

# start_index = 100
# end_index = 1000
batch_size = 100  # Размер пакета запросов

async def send_get_request(session, token, bin, endpoint):
    """Асинхронно отправляет GET-запрос и возвращает данные для последующей записи"""
    url = f"{base_url}{API_TOKEN}?token={token}"

    async with session.get(url, proxy=proxies.get("https"), ssl=False) as response:
        if response.status == 200:
            response_json = await response.json()
            message = response_json.get("message", "")

            if message == "wait":
                return {"bin": bin, "endpoint": endpoint, "token": token, "status": "wait"}
            elif message == "ready":
                data = response_json.get("data", {})
                return {"bin": bin, "endpoint": endpoint, "data": data, "status": "ready"}
        elif response.status == 404:
            return {"bin": bin, "endpoint": endpoint, "token": token, "status": "not_found"}
        elif response.status == 500:
            return {"bin": bin, "endpoint": endpoint, "token": token, "status": "server error"}
        else:
            print(f"Ошибка запроса {url}: {response.status}")
            return None

async def process_batches():
    """Обрабатывает запросы пакетами по batch_size"""
    responses = load_responses()
    if not isinstance(responses, list):
        print("Ошибка загрузки данных из response_tokens.json")
        return

    wait_data = []
    ready_data = []
    not_found_data = []
    error = []

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(responses), batch_size):
            batch = responses[i:i + batch_size]  # Берем по batch_size записей
            tasks = [send_get_request(session, response["token"], response["bin"], response["endpoint"]) 
                     for response in batch if response.get("token") and response.get("bin") and response.get("endpoint")]

            results = await asyncio.gather(*tasks)  # Дожидаемся выполнения пакета

            for result in results:
                if result:
                    if result["status"] == "wait":
                        wait_data.append(result)
                    elif result["status"] == "ready":
                        ready_data.append(result)
                    elif result["status"] == "not_found":
                        not_found_data.append(result)
                    elif result["status"] == "server error":
                        error.append(result)

            print(f"✅ Завершено {min(i + batch_size, len(responses))}/{len(responses)} записей.")

    save_responses_to_file("results/wait.json", wait_data)
    save_responses_to_file(f"results/responses.json", ready_data)
    save_responses_to_file("results/fl.json", not_found_data)
    save_responses_to_file("results/error.json", error)
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

def get_data():
    asyncio.run(process_batches())
