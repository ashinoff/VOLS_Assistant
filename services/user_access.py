import pandas as pd
import requests
import io
from config import ZONES_CSV_URL

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=',')
    # debug
    # print("Заголовки:", df.columns.tolist())
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    # Обеспечиваем гибкость к регистру и пробелам
    id_col = [c for c in df.columns if c.strip().lower() in ("telegram id", "telegram_id")][0]
    user_row = df[df[id_col].astype(str) == str(telegram_id)]
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
