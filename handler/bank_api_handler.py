
def url_routing(bank: str, map_params: dict):
    if (bank.lower() == 'vietcombank'):
        date = map_params['date']
        return f"https://www.vietcombank.com.vn/api/exchangerates?date={date}" # e.g. 2024-06-07

    return ""