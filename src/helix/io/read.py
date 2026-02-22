import pandas as pd

def read_events_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)
