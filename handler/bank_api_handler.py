from datetime import datetime

def url_routing(bank: str, map_params: dict):
    if (bank.lower() == 'vietcombank'):
        date = map_params['date']
        return f"https://www.vietcombank.com.vn/api/exchangerates?date={date}" # e.g. 2024-06-07
    elif (bank.lower() == 'techcombank'):
        url = 'https://techcombank.com/content/techcombank/web/vn/vi/cong-cu-tien-ich/ty-gia/_jcr_content.exchange-rates.integration.json'

        if (map_params):
            date = map_params['date'] # e.g. 2022-12-15
            url = f'https://techcombank.com/content/techcombank/web/vn/vi/cong-cu-tien-ich/ty-gia/_jcr_content.exchange-rates.{date}.integration.json'
            
        return url
            
            
    return ""

if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d")
    print(url_routing('techcombank', dict(date = now)))