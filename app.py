import pandas as pd
import requests
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Токен для авторизации
tokenAuth = "QSG49tlkZgFpxsdJVjCPTYuq09xFuPye"

# Количество потоков (можно увеличить до 30)
total_threads = 15

# Индекс, с которого нужно продолжить обработку
resume_index = 6230

# Создаем глобальный объект блокировки
lock = Lock()


def save_data(iin, response_json):
    """
    Сохраняет данные в response.json, добавляя новые данные в конец.
    """
    filename = "response.json"

    # Используем блокировку для предотвращения одновременного доступа
    with lock:
        # Проверяем, существует ли файл
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                try:
                    # Загружаем данные, если файл не пустой и корректный
                    data = json.load(f)
                    if not isinstance(data, list):
                        data = []
                except (json.JSONDecodeError, ValueError):
                    # Если файл пуст или поврежден, начинаем с пустого списка
                    data = []
        else:
            # Если файл не существует, создаем новый список
            data = []

        # Проверяем, есть ли уже данные для этого iin
        if not any(item["iin"] == iin for item in data):
            # Добавляем новые данные в список
            data.append({"iin": iin, "data": response_json})

            # Сохраняем обновленные данные в файл
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"✅ Данные добавлены для bin: {iin}")
        else:
            print(f"⚡️ Данные для bin {iin} уже существуют. Пропуск...")


def load_data():
    """
    Загружает данные из data.xlsx.
    """
    df = pd.read_excel("data.xlsx")
    return df


def get_bin_array(df):
    """
    Возвращает массив уникальных bin.
    """
    bins = df["bin"].unique()
    return bins


def fetch_data(iinBin):
    """
    Выполняет запрос данных для одного bin.
    """
    try:
        # Отправляем первый запрос
        response = requests.get(
            f"https://api.adata.kz/api/company/info/{tokenAuth}?iinBin={iinBin}"
        )

        token = response.json().get("token")
        if not token:
            print(f"❌ Не удалось получить токен для bin: {iinBin}")
            return None

        # Ожидаем завершения обработки данных
        attempts = 0
        while attempts < 10:
            print(f"🔄 Попытка {attempts + 1} для bin: {iinBin}")
            check_response = requests.get(
                f"https://api.adata.kz/api/company/info/check/{tokenAuth}?token={token}"
            )

            check_data = check_response.json()
            if check_data.get("message") == "wait":
                time.sleep(15)  # Ждем 15 секунд перед следующей проверкой
                attempts += 1
                continue
            else:
                # Если данные получены, сохраняем и выходим из цикла
                if "data" in check_data and check_data["data"] is not None:
                    save_data(iinBin, check_data["data"])
                    print(f"✅ Данные сохранены для bin: {iinBin}")
                    return check_data["data"]

                # Прерываем, если данные не получены или ошибка
                break

        print(f"⚠️ Данные не получены для bin: {iinBin} после {attempts} попыток.")
        return None
    except Exception as e:
        print(f"❌ Ошибка при обработке bin {iinBin}: {e}")
        return None


def process_batch(bins, start, total_threads, resume_index=6230):
    """
    Обрабатывает данные с шагом total_threads, начиная с определенного индекса (resume_index).
    """
    count = 0
    for i in range(start, len(bins), total_threads):
        # Пропускаем уже обработанные записи
        if i < resume_index:
            continue

        iinBin = bins[i]
        data = fetch_data(iinBin)
        if data:
            count += 1

    print(f"✅ Поток {start + 1} завершил обработку {count} записей.")


def get_data(bins):
    """
    Параллельно обрабатывает данные в 15 потоках с шагом.
    """
    with ThreadPoolExecutor(max_workers=total_threads) as executor:
        # Создаем задачи для каждого потока, начиная с 0 до total_threads
        futures = [executor.submit(process_batch, bins, start, total_threads, resume_index) for start in range(total_threads)]

        # Ожидаем завершения всех потоков
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Ошибка в одном из потоков: {e}")

    print(f"🎉 Все данные обработаны с использованием {total_threads} потоков.")


def main():
    """
    Основная функция для запуска.
    """
    df = load_data()
    bins = get_bin_array(df)
    get_data(bins)


if __name__ == "__main__":
    main()
