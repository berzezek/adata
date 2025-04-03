import time
from test_1_async import get_tokens
from test_2_async import get_data

def process_data():
    """Общий процесс обработки: получение токенов и данных с таймерами для каждой функции"""
    start_index = int(input("Введите начальный индекс BIN: "))
    end_index = int(input("Введите конечный индекс BIN: "))

    print("\n⏳ Запускаем процесс...")

    # Таймер для get_tokens()
    start_tokens_time = time.perf_counter()
    get_tokens(start_index, end_index)
    end_tokens_time = time.perf_counter()
    tokens_duration = end_tokens_time - start_tokens_time

    print("⏳ Таймер запущен на 5 минут...")
    time.sleep(300)
    print("⏰ Время истекло!")

    # Таймер для get_data()
    start_data_time = time.perf_counter()
    get_data()
    end_data_time = time.perf_counter()
    data_duration = end_data_time - start_data_time

    print(f"\n⏱️ Время выполнения get_tokens(): {tokens_duration:.2f} сек")
    print(f"⏱️ Время выполнения get_data(): {data_duration:.2f} сек")
    print(f"\n✅ Процесс завершен! Общее время выполнения: {tokens_duration + data_duration:.2f} сек")

# Запуск объединенного процесса
if __name__ == "__main__":
    process_data()
