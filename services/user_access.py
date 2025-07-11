import pandas as pd
import requests
import io
from config import ZONES_CSV_URL

def get_zones_df():
    # Скачиваем CSV-файл с Гугл Диска или другого URL
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    # Читаем CSV: подбери sep='\t' если табуляция или sep=',' если запятые
    df = pd.read_csv(io.StringIO(r.text), sep='\t')
    df.columns = df.columns.str.strip()  # убирает пробелы вокруг названий
    # print(df.columns.tolist())  # Для дебага, можно включить если нужно увидеть названия столбцов
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    user_row = df[df['Telegram ID'] == telegram_id]
    if user_row.empty:
        return None
    row = user_row.iloc[0]
    return {
        "zone": row['Видимость'],
        "filial": row['Филиал'],
        "res": row['РЭС'],
        "fio": row['ФИО'],
        "responsible": row['Ответственный'],
    }
