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
TOKENS=SOME_TOKEN
URL=https://api.adata.kz/

MAX_RETRIES=1
RETRY_INTERVAL=1
CHECK_URL=api/company/info/check/

API_TOKEN=YOUR_TOKEN
BATCH_SIZE=10

# В рабочей сети используются прокси с авторизацией
HTTP_PROXY=http://{LOGIN}:{PASSWORD}@{HOST}:{PORT}
HTTPS_PROXY=http://{LOGIN}:{PASSWORD}@{HOST}:{PORT}
```

# Запуск парсера
```bash
python main.py
```

# Запуск создания отчетов
```bash
python report.py start_index end_index
```

```text
В случае ошибки или прекращения работы программы (например истечения действия токена), то можно возобновить с нужной строки
для этого необходимо в app.py изменить RESUME_INDEX = <номер строки с которой нужно продолжить>
```

```text
Запускается main.py. Ввоводится диапазон БИН-ов из файла
Скрипт запускает test_1_async.py и test_2_async.py.

test_1_async.py считывает БИН-ы из файла в заданном диапазоне. Делает запросы во все endpoint
и возвращает токены которые записываются в response_tokens.json (файл перезаписывается при каждом запуске).
После выполнения ожидаем некоторое время пока запросы обработуются на сервере (5 мин).

test_2_async.py считывает токены из response_tokens.json и записывает успешные ответы в файл results\responses.json (в файл добавлются новые записи).

Те запросы которые еще не успели обработать на сервере (status=wait) добавляются в results\wait.json.

Ответы 404 могут быть если БИН не находит в Адате, или если этот БИН относиться к физ. лицу (нужны другие endpoint-ы).
Такие запросы записываются в results\fl.json.

Ответы 500 записываются в results\error.json. Пока что не понятна причина.

На данные момент загружены с 0 по 1000.
Следующий должен быть с 1000 по 2000.
```

```text
В env есть прокси, на рабочих компьютерах нужна авторизация.
```

## Доступные данные в сервиcе ADATA для прескрининга по мерчантам

1. Степень риска налогоплательщика - отказ при параметре "высокая" 
  * "endpoint": "info",
  * "data" -> "riskFactor" -> "company" -> "tax_risk_degree" (string)
  * Условние: "tax_risk_degree" != "высокая"

2. Участие в судебных делах - отказ при параметре "Уголовные" более 0
  * "data" -> "riskFactor" -> "head" -> "litigation" -> "total_criminal_count" (int)
  * Условние: "total_criminal_count" == 0

3. Оценочная прибыль компании - выход на возможную ЧП в динамике
  * "endpoint": "profit"
  * "data" -> [{"year": string | null, "ammount": int}]
  * Сводную таблицу

4. Налоговые отчисления - выход на возможный доход
  * "endpoint": "tax"
  * "data" -> "details" [{"year": string | null, "ammount": int}]
  * Сводную таблицу

  * "endpoint": "estimated-wage-fund"
  * "data" -> "bar" -> [{"year": string | null, "part": int, "ammount": int}]
  * "data" -> "line" -> [{"year": string | null, "ammount": int}]
  * Сводную таблицу

5. Налоговые отчисления - выход на возможный доход (Налоговые отчисления по КБК)
  * "endpoint": "tax-deduction_kbk"
  * "data" -> "bar" -> [{"year": string | null, "part": int, "ammount": int}]
  * "data" -> "line" -> [{"year": string | null, "ammount": int}]
  * Сводную таблицу

6. Налоговые отчисления – КБК по КПН, возможен выход на прибыль
  * "endpoint": "tax-deduction_extended"
  * "data" -> "details" -> [{"bcc_name": string, "bcc": int, "ammount": int, "org_name": string, "write_off_date": string, "receipt_date": string, "entry_name": string, "pay_name": string, entry_code: string, "pay_code": string}]
  * Сводную таблицу

7. Задолженности – отказ по первым 3 позициям
  * "endpoint": "info"
  * "data" -> "status" -> "tax_debt" (int)
  * "data" -> "riskFactor" -> company -> "ban_leaving" (bool)
  * "data" -> "riskFactor" -> company -> "enforcement_debt" (bool)

8. Участие в закупках – выход на возможный доход
  * "endpoint": "contract_status"
  * "data" -> "total_count" (int)
  * "data" -> "total_sum" (int)
  * "years" -> [{"year": string | null, "ammount": int}]

9. Рейтинг компании – как вариант лучше, чем у 50% компаний
  * "endpoint": "rating"
    * "data" -> "company" -> "actual" -> "place" (int)
    * "data" -> "company" -> "critical" -> "amount" (int)
    * "data" -> "company" -> "high" -> "place" (int)

10. Наличие активов – как вариант «нет имущества» - отказ
  * "endpoint": "tax-deduction_dynamics"
  * "data" -> "has_auto" (bool)
  * "data" -> "no_land" (bool)
  * "data" -> "has_property" (bool)

11. Динамика рынка – возможные поправки к уровню дохода
  * "endpoint": "market-dynamics"
  * "data" -> "company" -> [{"year": string | null, "ammount": int, place: int}]
  * "data" -> "market" -> [{"year": string | null, "ammount": int, place: int}]
  * Сводную таблицу

## Даты для запусков и параметры ввода
  
  * 12.04.2025:
      Введите начальный индекс BIN: 4900
      Введите конечный индекс BIN: 7900
  * 13.04.2025:
      Введите начальный индекс BIN: 7900
      Введите конечный индекс BIN: 10900
