from datetime import datetime

def url_routing(bank: str, map_params: dict):
    if (bank.lower() == 'vietcombank'):
        date = map_params['date']
        return f"https://www.vietcombank.com.vn/api/exchangerates?date={date}" # e.g. 2024-06-07
    elif (bank.lower() == 'techcombank'):
        url = 'https://techcombank.com/content/techcombank/web/vn/vi/cong-cu-tien-ich/ty-gia/_jcr_content.exchange-rates.integration.json'

        if map_params:
            date = map_params['date'] # e.g. 2022-12-15
            date_param = convert_date_format(date)
            
            if not date_param:
                date_param = date
            
            url = f'https://techcombank.com/content/techcombank/web/vn/vi/cong-cu-tien-ich/ty-gia/_jcr_content.exchange-rates.{date_param}.integration.json'
            
        return url
            
            
    return ""

def convert_date_format(date_str):
    try:
        # Check if the date string matches the format 'yyyy-MM-dd HH:mm:ss'
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        
        # Convert the date to the format 'yyyy-MM-dd.HH-mm-ss'
        new_date_str = date_obj.strftime('%Y-%m-%d.%H-%M-%S')
        
        return new_date_str
    except ValueError:
        # If the date string does not match the format, return None or raise an error
        return None

if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d") + " " + "16:22:31"
    print(url_routing('techcombank', dict(date = now)))

    # print(convert_date_format('2024-06-27'))