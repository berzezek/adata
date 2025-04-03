import pandas as pd
import aiohttp
import asyncio
import json
import os
from dotenv import load_dotenv
import urllib3

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

df = pd.read_excel("data.xlsx")

# start_index = int(input("Введите начальный индекс BIN: "))
# end_index = int(input("Введите конечный индекс BIN: "))
batch_size = 100  # Размер пакета запросов

async def send_request(session, api_name, api_endpoint, bin):
    """Асинхронно отправляет GET-запрос к API через прокси"""
    url = f"{base_url}{api_endpoint}{API_TOKEN}?iinBin={bin}"
    
    async with session.get(url, proxy=proxy_url, ssl=False) as response:
        if response.status == 200:
            data = await response.json()
            if data.get("success") and "token" in data:
                token = data["token"]
                return {"bin": bin, "endpoint": api_name, "token": token}
            else:
                return {"error": "Success response received, but no token found"}
        else:
            return {"error": f"Request failed with status {response.status}"}

async def process_batches(start_index, end_index):
    """Обрабатывает запросы пакетами по batch_size"""
    response_list = []
    processed_bins = 0

    async with aiohttp.ClientSession() as session:
        bins = df['bin'][start_index:end_index].tolist()

        for i in range(0, len(bins), batch_size):
            batch = bins[i:i + batch_size]  # Выбираем 100 BIN-ов
            tasks = [send_request(session, key, value, bin_value)
                     for bin_value in batch
                     for key, value in api_endpoints.items()]

            responses = await asyncio.gather(*tasks)  # Дожидаемся выполнения пакета

            response_list.extend(responses)  # Сохраняем полученные данные
            processed_bins += len(batch)
            print(f"✅ Завершено {processed_bins}/{len(bins)} BIN-ов")

    save_responses_to_file(response_list)
    print("\n✅ Все запросы обработаны!")

def save_responses_to_file(response_list, filename="response_tokens.json"):
    """Записывает список ответов в JSON-файл"""
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(response_list, file, ensure_ascii=False, indent=4)

def get_tokens(start_index, end_index):
    asyncio.run(process_batches(start_index, end_index))
