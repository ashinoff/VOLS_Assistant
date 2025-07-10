import pandas as pd
import requests
from config import ZONES_CSV_URL

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    df = pd.read_csv(pd.compat.StringIO(r.text))
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
