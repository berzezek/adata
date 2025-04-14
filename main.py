import time
import argparse
from test_1_async import get_tokens
from test_2_async import get_data
import os
from dotenv import load_dotenv
load_dotenv()

def process_data(start_index, end_index, use_proxy):
    """Общий процесс обработки: получение токенов и данных с таймерами для каждой функции"""
    print("\n⏳ Запускаем процесс...")


    if use_proxy:
        proxies = {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY"),
        }
        print("Используется прокси...")
    else:
        proxies = None
        print("Прокси отключены...")

    # Таймер для get_tokens()
    start_tokens_time = time.perf_counter()
    get_tokens(start_index, end_index, proxies)
    end_tokens_time = time.perf_counter()
    tokens_duration = end_tokens_time - start_tokens_time

    # Таймер для get_data()
    start_data_time = time.perf_counter()
    get_data(start_index, end_index, proxies)
    end_data_time = time.perf_counter()
    data_duration = end_data_time - start_data_time

    print(f"\n⏱️ Время выполнения get_tokens(): {tokens_duration:.2f} сек")
    print(f"⏱️ Время выполнения get_data(): {data_duration:.2f} сек")
    print(f"\n✅ Процесс завершен! Общее время выполнения: {tokens_duration + data_duration:.2f} сек")

if __name__ == "__main__":
    # Инициализация парсера аргументов командной строки
    parser = argparse.ArgumentParser(description="Обработка BIN диапазона через командную строку.")
    parser.add_argument("start_index", type=int, help="Начальный индекс BIN.")
    parser.add_argument("end_index", type=int, help="Конечный индекс BIN.")
    parser.add_argument("--no-proxy", action="store_true", help="Отключить использование прокси.")

    # Парсинг аргументов
    args = parser.parse_args()

    # Если аргумент --no-proxy указан, прокси отключается
    use_proxy = not args.no_proxy

    # Запуск объединенного процесса с переданными аргументами
    process_data(args.start_index, args.end_index, use_proxy)


# python main.py 2000 2100
