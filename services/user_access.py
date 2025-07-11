import pandas as pd
import requests
import io
from config import ZONES_CSV_URL

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    # попробуем оба варианта разделителя
    try:
        df = pd.read_csv(io.StringIO(r.text), sep='\t')
        if len(df.columns) == 1:  # не сработала табуляция
            df = pd.read_csv(io.StringIO(r.text), sep=',')
    except Exception:
        df = pd.read_csv(io.StringIO(r.text), sep=',')
    df.columns = df.columns.str.strip()
    print("Заголовки:", df.columns.tolist())  # убери после отладки
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    # Попробуем вывести все названия для ручного сопоставления
    if 'Telegram ID' not in df.columns:
        raise Exception(f"Нет столбца 'Telegram ID'. Заголовки: {df.columns.tolist()}")
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
