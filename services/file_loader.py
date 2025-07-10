import pandas as pd
import requests
from io import BytesIO

def load_csv(url):
    r = requests.get(url)
    r.raise_for_status()
    if url.endswith('.xlsx'):
        df = pd.read_excel(BytesIO(r.content))
    else:
        df = pd.read_csv(BytesIO(r.content))
    return df
