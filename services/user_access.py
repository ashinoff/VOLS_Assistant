import pandas as pd
import requests
import io
from config import ZONES_CSV_URL

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=None, engine='python')
    # Выведи все заголовки в лог, чтобы точно видеть их
    print("Заголовки в df.columns:", df.columns.tolist())
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    # Останови выполнение, чтобы увидеть заголовки (убери return None — только для диагностики!)
    if 'Telegram ID' not in df.columns:
        raise Exception(f"Нет столбца 'Telegram ID'. Заголовки: {df.columns.tolist()}")
    user_row = df[df['Telegram ID'].astype(str) == str(telegram_id)]
    if user_row.empty:
        return None
    row = user_row.iloc[0]
    # То же самое для всех остальных — если падает на другом столбце, сразу будет видно
    return {
        "zone": row['Видимость'],
        "filial": row['Филиал'],
        "res": row['РЭС'],
        "fio": row['ФИО'],
        "responsible": row['Ответственный'],
    }
