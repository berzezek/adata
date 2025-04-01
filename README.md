# Получение данных с Adata

# Установка окружения
```bash
python -m venv venv
source venv/bin/activate # Linux
venv\Scripts\activate # Windows

pip install -r requirements.txt
```
# Создание файла .env
```bash
# Создайте файл .env в корне проекта и добавьте туда ваши переменные окружения
TOKENS=oQSG49tlkZgFpxsdJVjCPTYuq09xFuPye
URL=https://api.adata.kz/

MAX_RETRIES=1
RETRY_INTERVAL=1
CHECK_URL=api/company/info/check/
```

# Запуск
```bash
python app.py
```

```text
В случае ошибки или прекращения работы программы (например истечения действия токена), то можно возобновить с нужной строки
для этого необходимо в app.py изменить RESUME_INDEX = <номер строки с которой нужно продолжить>
```

