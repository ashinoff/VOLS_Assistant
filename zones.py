import os
import asyncio
import aiohttp
from io import BytesIO
import pandas as pd
import logging
from datetime import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def normalize_sheet_url(url):
    """Нормализует URL Google Sheets для экспорта в CSV, поддерживая /pub и /export."""
    if not url:
        return ""
    if "docs.google.com" in url:
        if "/pub?" in url and "output=csv" in url:
            return url
        if "/export?format=csv" not in url and "/pub?" not in url:
            return url + "/export?format=csv"
    return url

async def load_zones_cached(context, url=os.getenv("ZONES_CSV_URL", ""), ttl=3600):
    cache_key = "zones_data"
    if not url:
        logger.error("ZONES_CSV_URL не настроен")
        raise ValueError("URL зон не указан")
    if cache_key not in context.bot_data or context.bot_data[cache_key]["expires"] < time.time():
        try:
            normalized_url = await normalize_sheet_url(url)
            logger.info(f"Загружаю зоны с URL: {normalized_url}")
            async with aiohttp.ClientSession() as session:
                async with session.get(normalized_url, timeout=10) as response:
                    response.raise_for_status()
                    df = pd.read_csv(BytesIO(await response.read()))
            logger.info(f"Загружено строк: {len(df)}")
            
            # Маппинг возможных названий столбцов
            column_mapping = {
                "Telegram ID": ["ID", "Telegram ID"],
                "Видимость": ["Видимость", "RK", "UG"],
                "Филиал": ["Филиал", "ФИЛИАЛ"],
                "РЭС": ["РЭС"],
                "ФИО": ["ФИО"],
                "Ответственный": ["Ответственный"]
            }
            mapped_columns = {}
            for expected, alternatives in column_mapping.items():
                for alt in alternatives:
                    if alt in df.columns:
                        mapped_columns[expected] = alt
                        break
                if expected not in mapped_columns:
                    logger.error(f"Столбец {expected} не найден среди {df.columns.tolist()}")
                    raise ValueError(f"Отсутствует столбец: {expected}")

            vis_map = dict(zip(df[mapped_columns["Telegram ID"]], df[mapped_columns["Видимость"]]))
            raw_branch_map = dict(zip(df[mapped_columns["Telegram ID"]], df[mapped_columns["Филиал"]]))
            res_map = dict(zip(df[mapped_columns["Telegram ID"]], df[mapped_columns["РЭС"]]))
            names = dict(zip(df[mapped_columns["Telegram ID"]], df[mapped_columns["ФИО"]]))
            resp_map = dict(zip(df[mapped_columns["Telegram ID"]], df[mapped_columns["Ответственный"]]))
            logger.info(f"Создано словарей: vis_map={len(vis_map)}, raw_branch_map={len(raw_branch_map)}")
            context.bot_data[cache_key] = {
                "vis_map": vis_map,
                "raw_branch_map": raw_branch_map,
                "res_map": res_map,
                "names": names,
                "resp_map": resp_map,
                "expires": time.time() + ttl
            }
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка загрузки зон: {e}, url={normalized_url}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"CSV-файл зон пуст: {normalized_url}")
            raise
        except ValueError as e:
            logger.error(f"Ошибка обработки данных: {e}")
            raise
    return (
        context.bot_data[cache_key]["vis_map"],
        context.bot_data[cache_key]["raw_branch_map"],
        context.bot_data[cache_key]["res_map"],
        context.bot_data[cache_key]["names"],
        context.bot_data[cache_key]["resp_map"]
    )
