import requests
from requests.auth import HTTPBasicAuth
import polars as pl
from dataclasses import dataclass
import datetime

@dataclass
class auth_param:
    username: str
    password: str


def get_data(endpoint: str, airport: str, begin: int, end: int, auth: auth_param):

    url_request = f"https://opensky-network.org/api{endpoint}"
    params = {
        "airport": airport,
        "begin": begin,
        "end": end
    }
    response = requests.get(url=url_request, params=params, auth=HTTPBasicAuth(auth.username, auth.password))
    return response.json()
    

def clean_data(data: dict) -> pl.DataFrame:

    COLUMNS = ["icao24",
               "firstSeen",
               "lastSeen",
               "estDepartureAirport",
               "estArrivalAirport",
               "callsign"]
    df_data = pl.DataFrame(data).select(COLUMNS)
    df_data = df_data.with_columns(
        pl.from_epoch(pl.col("firstSeen")).alias("firstSeen"),
        pl.from_epoch(pl.col("lastSeen")).alias("lastSeen")
    )

    df_data = df_data.rename(
        {"firstSeen": "departure_time",
         "lastSeen": "arrival_time",
         "estDepartureAirport": "departure_airport",
         "estArrivalAirport": "arrival_airport",
         "callsign": "flight_number"}
    )

    return df_data

class DataLoader:
    def __init__(self, df_data: pl.DataFrame, path: str = "data/data.parquet"):
        self.df = df_data
        self.path = path


    def load_locally(self, path: str = None):
        # Use the provided path if given, otherwise default to self.path
        if path is None:
            path = self.path
            
        try:
            historic_data = pl.read_parquet(path)
            print("Data loaded successfully.")
        except FileNotFoundError:
            print("No saved data found. Creating new file.")
            historic_data = pl.DataFrame()
            
        all_data = pl.concat([historic_data, self.df]).unique()
        all_data.write_parquet(path)
        store_logs(all_data)

    def load_to_bigquery(self):
        # Placeholder for BigQuery loading logic
        pass


def store_logs(all_data: pl.DataFrame):
        try:
            logs = pl.read_csv("data/logs.csv")
        except:
            logs = pl.DataFrame()
        
        logs = pl.concat([
            logs,
            pl.DataFrame([{"rows": all_data.shape[0], "timestamp": datetime.date.today()}])
        ])
        logs.write_csv("data/logs.csv")