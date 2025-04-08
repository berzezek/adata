import json
import pandas as pd
from pathlib import Path

# Пути к файлам
excel_path = Path("data.xlsx")
json_path = Path("results/responses.json")

# Загрузка Excel с основными данными по компаниям
data_df = pd.read_excel(excel_path)
data_df["bin"] = data_df["bin"].astype(str)

# Загрузка JSON с endpoint'ами
with open(json_path, "r", encoding="utf-8") as f:
    raw_data = json.load(f)

# Собираем все данные по BIN в словарь вида: {bin: {endpoint: data}}
company_dict_all = {}
for entry in raw_data:
    bin_ = entry.get("bin")
    endpoint = entry.get("endpoint")
    data = entry.get("data")
    if not bin_ or not endpoint:
        continue
    if bin_ not in company_dict_all:
        company_dict_all[bin_] = {}
    company_dict_all[bin_][endpoint] = data

# Подготовка списка колонок сводки
columns = [
    "bin",
    "director_name",
    "phone",
    "company_name",
    "source",
    "01 - Степень риска налогоплательщика",
    "02 - Участие в судебных делах",
    "03 - Оценочная прибыль компании",
    "04 - Налоговые отчисления",
    "05 - Налоговые отчисления по КБК",
    "06 - Налоговые отчисления КБК по КПН",
    "07 - Задолженности",
    "08 - Участие в закупках",
    "09 - Рейтинг компании (лучше, чем у 50% компаний)",
    "10 - Рейтинг компании («нет имущества» - отказ)",
    "11 - Динамика рынка",
]

# Результаты
summary_rows = []
sheets = {
    "Оценочная прибыль": [],
    "Налоговые отчисления": [],
    "Налоговые отчисления по КБК": [],
    "КБК по КПН": [],
    "Задолженности": [],
    "Участие в закупках": [],
    "Наличие активов": [],
    "Динамика рынка - company": [],
    "Динамика рынка - market": [],
}

years_range = [str(y) for y in range(2012, 2026)]

# Обработка каждой компании
for _, row in data_df.iterrows():
    bin_ = str(row["bin"])
    endpoints = company_dict_all.get(bin_)
    summary = {
        "bin": bin_,
        "director_name": row.get("director_name", ""),
        "phone": row.get("phone", ""),
        "company_name": row.get("company_name", ""),
        "source": row.get("source", ""),
    }

    if not endpoints:
        # # Если данных нет — заполняем сводку только основным
        # for col in columns[5:]:
        #     summary[col] = "нет данных"
        # summary_rows.append(summary)
        continue

    try:
        summary["01 - Степень риска налогоплательщика"] = (
            "отказ" if endpoints.get("info", {}).get("riskFactor", {}).get("company", {}).get("tax_risk_degree") == "высокая" else "ок"
        )
    except:
        summary["01 - Степень риска налогоплательщика"] = "ошибка"

    try:
        count = endpoints.get("info", {}).get("riskFactor", {}).get("head", {}).get("litigation", {}).get("total_criminal_count", 0)
        summary["02 - Участие в судебных делах"] = "отказ" if count > 0 else "ок"
    except:
        summary["02 - Участие в судебных делах"] = "ошибка"

    try:
        profit = endpoints.get("profit", [])
        df_profit = pd.DataFrame(profit)
        pivot = df_profit.pivot_table(index=None, columns="year", values="amount", aggfunc="sum")
        pivot = pivot.reindex(columns=years_range, fill_value=0)
        pivot.insert(0, "bin", bin_)
        sheets["Оценочная прибыль"].append(pivot)
        summary["03 - Оценочная прибыль компании"] = "отказ" if pivot.iloc[0, 1:].sum() == 0 else "ок"
    except:
        summary["03 - Оценочная прибыль компании"] = "ошибка"

    try:
        tax = endpoints.get("tax", {}).get("details", [])
        tax_dict = {year: 0 for year in years_range}
        for r in tax:
            y, a = str(r.get("year")), r.get("amount", 0)
            if y in tax_dict:
                tax_dict[y] = a
        tax_dict["bin"] = bin_
        sheets["Налоговые отчисления"].append(pd.DataFrame([tax_dict]))
        summary["04 - Налоговые отчисления"] = "ок"
    except:
        summary["04 - Налоговые отчисления"] = "ошибка"

    try:
        kbk = endpoints.get("tax-deduction_kbk", {}).get("line", [])
        kbk_dict = {year: 0 for year in years_range}
        for r in kbk:
            y, a = str(r.get("year")), r.get("amount", 0)
            if y in kbk_dict:
                kbk_dict[y] = a
        kbk_dict["bin"] = bin_
        sheets["Налоговые отчисления по КБК"].append(pd.DataFrame([kbk_dict]))
        summary["05 - Налоговые отчисления по КБК"] = "ок"
    except:
        summary["05 - Налоговые отчисления по КБК"] = "ошибка"

    try:
        details = endpoints.get("tax-deduction_extended", {}).get("details", [])
        kpn_entries = [r for r in details if "корпоративный подоходный налог" in r.get("bcc_name", "").lower() and r.get("entry_name") == "Платеж" and r.get("pay_code") == "tax"]
        summary["06 - Налоговые отчисления КБК по КПН"] = "ок" if kpn_entries else "отказ"
        if kpn_entries:
            df_kpn = pd.DataFrame(kpn_entries)
            df_kpn.insert(0, "bin", bin_)
            sheets["КБК по КПН"].append(df_kpn)
    except:
        summary["06 - Налоговые отчисления КБК по КПН"] = "ошибка"

    try:
        tax_debt = endpoints.get("info", {}).get("status", {}).get("tax_debt", 0)
        ban = endpoints.get("info", {}).get("riskFactor", {}).get("company", {}).get("ban_leaving", False)
        enforcement = endpoints.get("info", {}).get("riskFactor", {}).get("company", {}).get("enforcement_debt", False)
        summary["07 - Задолженности"] = "отказ" if tax_debt > 0 or ban or enforcement else "ок"
        sheets["Задолженности"].append(pd.DataFrame([{
            "bin": bin_,
            "налоговая задолженность": tax_debt,
            "запрет на выезд": "да" if ban else "нет",
            "исполнительное производство": "да" if enforcement else "нет"
        }]))
    except:
        summary["07 - Задолженности"] = "ошибка"

    try:
        cdata = endpoints.get("contract_status", {})
        total_count = cdata.get("total_count", 0)
        total_sum = cdata.get("total_sum", 0)
        summary["08 - Участие в закупках"] = "есть" if total_count > 0 or total_sum > 0 else "нет"

        purchase_by_year = {year: 0 for year in years_range}
        for item in cdata.get("years", []):
            y, a = str(item.get("year")), item.get("sum", 0)
            if y in purchase_by_year:
                purchase_by_year[y] = a
        purchase_by_year["bin"] = bin_
        sheets["Участие в закупках"].append(pd.DataFrame([purchase_by_year]))
    except:
        summary["08 - Участие в закупках"] = "ошибка"

    try:
        rating = endpoints.get("rating", {}).get("company", {}).get("actual", {}).get("place", "")
        summary["09 - Рейтинг компании (лучше, чем у 50% компаний)"] = rating
    except:
        summary["09 - Рейтинг компании (лучше, чем у 50% компаний)"] = "ошибка"

    try:
        info = endpoints.get("tax-deduction_dynamics", {})
        ha = info.get("has_auto", False)
        nl = info.get("no_land", False)
        hp = info.get("has_property", False)
        summary["10 - Рейтинг компании («нет имущества» - отказ)"] = "есть" if ha or nl or hp else "отсутствует"
        sheets["Наличие активов"].append(pd.DataFrame([{
            "bin": bin_,
            "нет автотранспорта": "да" if not ha else "нет",
            "нет земельного участка": "да" if not nl else "нет",
            "нет имущества": "да" if not hp else "нет",
        }]))
    except:
        summary["10 - Рейтинг компании («нет имущества» - отказ)"] = "ошибка"

    try:
        market = endpoints.get("market-dynamics", {})
        for key, sheetname in [("company", "Динамика рынка - company"), ("market", "Динамика рынка - market")]:
            values = market.get(key, [])
            row_data = {year: 0 for year in years_range}
            for r in values:
                y, a = str(r.get("year")), r.get("amount", 0)
                if y in row_data:
                    row_data[y] = a
            row_data["bin"] = bin_
            sheets[sheetname].append(pd.DataFrame([row_data]))
        summary["11 - Динамика рынка"] = "ок"
    except:
        summary["11 - Динамика рынка"] = "ошибка"

    summary_rows.append(summary)

# Финальная сборка и сохранение
with pd.ExcelWriter("report_data.xlsx", engine="xlsxwriter") as writer:
    pd.DataFrame(summary_rows, columns=columns).to_excel(writer, sheet_name="Сводка", index=False)
    for name, df_list in sheets.items():
        if df_list:
            pd.concat(df_list, ignore_index=True).to_excel(writer, sheet_name=name, index=False)

print("Файл report_data.xlsx создан успешно.")
