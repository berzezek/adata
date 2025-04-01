import json
import pandas as pd

# Путь к файлам
json_file = "response.json"
excel_file = "merches.xlsx"
output_file = "updated_merches.xlsx"

# Загрузка данных из JSON
with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Загрузка данных из Excel
existing_df = pd.read_excel(excel_file)

# Приводим bin к строковому типу в Excel
existing_df["bin"] = existing_df["bin"].astype(str)
# Список для обновленных данных
rows = []

# Обходим все записи в JSON
for record in data:
    try:
        info = record.get("data", {})
        if not info:
            continue

        # Получаем нужные данные
        basic = info.get("basic", {})
        risk_factor = info.get("riskFactor", {}).get("company", {})
        litigation = info.get("litigation", {})
        tax_deductions = info.get("taxDeductions", {})
        status = info.get("status", {})
        trustworthy_extended = info.get("trustworthyExtended", {})
        risk_extended = info.get("riskFactorExtended", {}).get("company", {})
        zakup_data = risk_extended.get("zakup", {}).get(
            "unreliable_suppliers_register", {}
        )
        zakup = "да" if zakup_data.get("samruk", False) else "нет"

        # Проверка на 'Степень риска налогоплательщика'
        tax_risk_degree = risk_factor.get("tax_risk_degree")
        if not tax_risk_degree or tax_risk_degree in [None, "null"]:
            tax_risk_degree = "нет"

        # Собираем данные в строку
        row = {
            "bin - iin": str(basic.get("biin", "-")),
            "Степень риска налогоплательщика": tax_risk_degree,
            "Участие в судебных делах (уголовные)": litigation.get(
                "total_criminal_count", 0
            ),
            "Оценочная прибыль компании": tax_deductions.get("sum", 0),
            "Задолженности по налогам и таможенным платежам": status.get("tax_debt", 0),
            "Должник по исполнительным делам": (
                "да"
                if trustworthy_extended.get("enforcement_debt", {}).get("count", 0) > 0
                else "нет"
            ),
            "Должник, временно ограниченный на выезд из Республики Казахстан": (
                "да"
                if trustworthy_extended.get("leaving_restriction", {}).get("count", 0)
                > 0
                else "нет"
            ),
            "участие в закупках": zakup,
        }

        # Добавляем в список для обработки
        rows.append(row)

    except Exception as e:
        print(f"Ошибка при обработке записи: {e}")

# Создаем DataFrame из данных
new_df = pd.DataFrame(rows)

# Обновляем существующие строки или добавляем новые
for index, row in new_df.iterrows():
    bin_value = row["bin - iin"]
    if bin_value in existing_df["bin"].values:
        existing_df.loc[existing_df["bin"] == bin_value, list(row.keys())[1:]] = (
            row.values[1:]
        )
    else:
        # Если bin не найден, добавляем новую строку
        existing_df = pd.concat([existing_df, pd.DataFrame([row])], ignore_index=True)
# Сохранение обновленного файла
existing_df.to_excel(output_file, index=False)

print(f"✅ Файл '{output_file}' успешно создан и обновлен.")
