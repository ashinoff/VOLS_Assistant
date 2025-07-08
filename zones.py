import os
import asyncio
import aiohttp
from io import BytesIO
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def normalize_sheet_url(url):
    """Нормализует URL Google Sheets для экспорта в CSV."""
    if "docs.google.com" in url and not url.endswith("/export?format=csv"):
        return url + "/export?format=csv"
    return url

async def load_zones_cached(context, url=os.getenv("ZONES_CSV_URL", ""), ttl=3600):
    """Кэширует данные зон доступа с использованием aiohttp."""
    cache_key = "zones_data"
    if cache_key not in context.bot_data or context.bot_data[cache_key]["expires"] < time.time():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(await normalize_sheet_url(url), timeout=10) as response:
                    response.raise_for_status()
                    df = pd.read_csv(BytesIO(await response.read()))
            vis_map = dict(zip(df["Telegram ID"], df["Видимость"]))
            raw_branch_map = dict(zip(df["Telegram ID"], df["Филиал"]))
            res_map = dict(zip(df["Telegram ID"], df["РЭС"]))
            names = dict(zip(df["Telegram ID"], df["ФИО"]))
            resp_map = dict(zip(df["Telegram ID"], df["Ответственный"]))
            context.bot_data[cache_key] = {
                "vis_map": vis_map,
                "raw_branch_map": raw_branch_map,
                "res_map": res_map,
                "names": names,
                "resp_map": resp_map,
                "expires": time.time() + ttl
            }
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка загрузки зон доступа: {e}")
            raise
        except pd.errors.EmptyDataError:
            logger.error("CSV-файл зон пуст")
            raise
    return (
        context.bot_data[cache_key]["vis_map"],
        context.bot_data[cache_key]["raw_branch_map"],
        context.bot_data[cache_key]["res_map"],
        context.bot_data[cache_key]["names"],
        context.bot_data[cache_key]["resp_map"]
    )
