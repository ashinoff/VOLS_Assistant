import re

def normalize_tp_name(name: str) -> str:
    # Убирает все нецифровые и небуквенные символы, делает заглавные
    return re.sub(r'[^a-zA-Zа-яА-Я0-9]', '', name).upper()
