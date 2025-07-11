import pandas as pd
import requests
import io
from config import ZONES_CSV_URL

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=None, engine='python')
    # Лог для отладки — чтобы видеть все заголовки при запуске
    print("Заголовки в df.columns:", df.columns.tolist())
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    # Проверка на наличие нужных столбцов
    required_cols = ['Telegram ID', 'Видимость', 'Филиал', 'РЭС', 'ФИО', 'Ответственный']
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Нет столбца '{col}'. Все заголовки: {df.columns.tolist()}")
    # Поиск пользователя
    user_row = df[df['Telegram ID'].astype(str) == str(telegram_id)]
    if user_row.empty:
        print(f"Пользователь с Telegram ID {telegram_id} не найден в таблице доступа.")
        return None
    row = user_row.iloc[0]
    # Сбор прав доступа с обработкой возможных NaN/None
    return {
        "zone": row['Видимость'] if 'Видимость' in row and pd.notna(row['Видимость']) else "",
        "filial": row['Филиал'] if 'Филиал' in row and pd.notna(row['Филиал']) else "",
        "res": row['РЭС'] if 'РЭС' in row and pd.notna(row['РЭС']) else "",
        "fio": row['ФИО'] if 'ФИО' in row and pd.notna(row['ФИО']) else "",
        "responsible": row['Ответственный'] if 'Ответственный' in row and pd.notna(row['Ответственный']) else "",
    }
