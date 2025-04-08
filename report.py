import json
import pandas as pd

company_dict = {}

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

with open("company.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for entry in data:
    endpoint = entry["endpoint"]
    company_dict[endpoint] = entry["data"]


def create_excel_report():
    biin = company_dict["info"]["basic"]["biin"]

    # Сводка
    df_main = pd.DataFrame(columns=columns)
    df_main["bin"] = [biin]
    df_main["director_name"] = [
        company_dict["info"]["basic"].get("fullname_director", "")
    ]
    df_main["phone"] = [""]
    df_main["company_name"] = [company_dict["info"]["basic"]["name_ru"]]
    df_main["source"] = [company_dict["info"]["basic"]["source_link"]]

    # Пункт 1
    df_main["01 - Степень риска налогоплательщика"] = [
        (
            "отказ"
            if company_dict["info"]["riskFactor"]["company"]["tax_risk_degree"]
            == "высокая"
            else "ок"
        )
    ]

    # Пункт 2
    df_main["02 - Участие в судебных делах"] = [
        (
            "отказ"
            if company_dict["info"]["riskFactor"]["head"]["litigation"][
                "total_criminal_count"
            ]
            > 0
            else "ок"
        )
    ]

    # Пункт 3
    profit_data = company_dict.get("profit", [])
    df_profit_raw = pd.DataFrame(profit_data)
    df_profit_pivot = df_profit_raw.pivot_table(
        index=None, columns="year", values="amount", aggfunc="sum"
    )
    df_profit_pivot.insert(0, "bin", biin)
    df_profit_pivot = df_profit_pivot.fillna(0).astype(int)

    # по всем слайдам , если доход 0, то отказ
    df_main["03 - Оценочная прибыль компании"] = [
        "отказ" if df_profit_pivot.iloc[0, 1:].sum() == 0 else "ок"
    ]

    # Пункт 4
    tax_data = company_dict.get("tax", {}).get("details", [])
    years = [str(y) for y in range(2012, 2026)]
    tax_dict = {year: 0 for year in years}
    for row in tax_data:
        year = str(row.get("year"))
        amount = row.get("amount", 0)
        if year in tax_dict:
            tax_dict[year] = amount
    tax_row = {"bin": biin}
    tax_row.update(tax_dict)
    df_tax_pivot = pd.DataFrame([tax_row])

    # Пункт 5
    kbk_data = company_dict.get("tax-deduction_kbk", {}).get("line", [])
    years_kbk = [str(y) for y in range(2012, 2026)]
    kbk_dict = {year: 0 for year in years_kbk}
    for row in kbk_data:
        year = str(row.get("year"))
        amount = row.get("amount", 0)
        if year in kbk_dict:
            kbk_dict[year] = amount
    kbk_row = {"bin": biin}
    kbk_row.update(kbk_dict)
    df_kbk = pd.DataFrame([kbk_row])

    # Пункт 6
    details = company_dict.get("tax-deduction_extended", {}).get("details", [])
    kpn_entries = []

    for row in details:
        bcc_name = row.get("bcc_name", "").lower()
        entry_name = row.get("entry_name", "").lower()
        pay_code = row.get("pay_code", "").lower()

        # Критерий: КПН, платеж, налог
        if (
            ("корпоративный подоходный налог" in bcc_name)
            and entry_name == "платеж"
            and pay_code == "tax"
        ):
            kpn_entries.append(row)

    # В сводку
    df_main["06 - Налоговые отчисления КБК по КПН"] = [
        "отказ" if not kpn_entries else "ок"
    ]

    # Лист со всеми строками по КПН
    if kpn_entries:
        df_kpn = pd.DataFrame(kpn_entries)
        df_kpn.insert(0, "bin", biin)
    else:
        df_kpn = pd.DataFrame([{"bin": biin}])  # пустая строка, если нет данных

    # Пункт 7
    tax_debt = company_dict["info"]["status"].get("tax_debt", 0)
    ban_leaving = company_dict["info"]["riskFactor"]["company"].get(
        "ban_leaving", False
    )
    enforcement_debt = company_dict["info"]["riskFactor"]["company"].get(
        "enforcement_debt", False
    )

    # Условие для отказа
    if tax_debt > 0 or ban_leaving or enforcement_debt:
        df_main["07 - Задолженности"] = ["отказ"]
    else:
        df_main["07 - Задолженности"] = ["ок"]

    # Отдельный лист
    df_debts = pd.DataFrame(
        [
            {
                "bin": biin,
                "налоговая задолженность": tax_debt,
                "запрет на выезд": "да" if ban_leaving else "нет",
                "исполнительное производство": "да" if enforcement_debt else "нет",
            }
        ]
    )

    # Пункт 8
    contract_data = company_dict.get("contract_status", {})
    total_count = contract_data.get("total_count", 0)
    total_sum = contract_data.get("total_sum", 0)

    # На главный лист
    if total_count > 0 or total_sum > 0:
        df_main["08 - Участие в закупках"] = ["есть"]
    else:
        df_main["08 - Участие в закупках"] = ["нет"]

    # Подробный лист по годам
    years_data = contract_data.get("years", [])
    years_range = [str(y) for y in range(2012, 2026)]
    purchase_by_year = {year: 0 for year in years_range}
    for item in years_data:
        year = str(item.get("year"))
        amount = item.get("sum", 0)
        if year in purchase_by_year:
            purchase_by_year[year] = amount

    purchase_row = {"bin": biin}
    purchase_row.update(purchase_by_year)
    df_contracts = pd.DataFrame([purchase_row])

    # Пункт 9
    rating_place = (
        company_dict.get("rating", {})
        .get("company", {})
        .get("actual", {})
        .get("place", "")
    )
    df_main["09 - Рейтинг компании (лучше, чем у 50% компаний)"] = [rating_place]

    # Пункт 10
    asset_info = company_dict.get("tax-deduction_dynamics", {})
    has_auto = asset_info.get("has_auto", False)
    no_land = asset_info.get("no_land", False)
    has_property = asset_info.get("has_property", False)

    # На главном листе: если хотя бы одно True — "есть", иначе — "отсутствует"
    if has_auto or has_property or no_land:
        df_main["10 - Рейтинг компании («нет имущества» - отказ)"] = ["есть"]
    else:
        df_main["10 - Рейтинг компании («нет имущества» - отказ)"] = ["отсутствует"]

    # Отдельный лист "Наличие активов"
    df_assets = pd.DataFrame(
        [
            {
                "bin": biin,
                "нет автотранспорта": "да" if not has_auto else "нет",
                "нет земельного участка": "да" if not no_land else "нет",
                "нет имущества": "да" if not has_property else "нет",
            }
        ]
    )

    # Пункт 11
    market_data = company_dict.get("market-dynamics", {})
    company_years = market_data.get("company", [])
    market_years = market_data.get("market", [])

    # Company динамика
    years_range = [str(y) for y in range(2012, 2026)]
    company_dict_dyn = {year: 0 for year in years_range}
    for row in company_years:
        year = str(row.get("year"))
        amount = row.get("amount", 0)
        if year in company_dict_dyn:
            company_dict_dyn[year] = amount
    df_market_company = pd.DataFrame([{"bin": biin, **company_dict_dyn}])

    # Market динамика
    market_dict_dyn = {year: 0 for year in years_range}
    for row in market_years:
        year = str(row.get("year"))
        amount = row.get("amount", 0)
        if year in market_dict_dyn:
            market_dict_dyn[year] = amount
    df_market_total = pd.DataFrame([{"bin": biin, **market_dict_dyn}])

    # Сохраняем в Excel
    with pd.ExcelWriter("company_report.xlsx", engine="xlsxwriter") as writer:
        df_main.to_excel(writer, sheet_name="Сводка", index=False)
        df_profit_pivot.to_excel(writer, sheet_name="Оценочная прибыль", index=False)
        df_tax_pivot.to_excel(writer, sheet_name="Налоговые отчисления", index=False)
        df_kbk.to_excel(writer, sheet_name="Налоговые отчисления по КБК", index=False)
        df_kpn.to_excel(writer, sheet_name="КБК по КПН", index=False)
        df_debts.to_excel(writer, sheet_name="Задолженности", index=False)
        df_contracts.to_excel(writer, sheet_name="Участие в закупках", index=False)
        df_assets.to_excel(writer, sheet_name="Наличие активов", index=False)
        df_market_company.to_excel(
            writer, sheet_name="Динамика рынка - company", index=False
        )
        df_market_total.to_excel(
            writer, sheet_name="Динамика рынка - market", index=False
        )
    print("Файл company_report.xlsx создан")


if __name__ == "__main__":
    create_excel_report()
