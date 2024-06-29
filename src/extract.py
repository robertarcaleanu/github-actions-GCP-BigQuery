import requests
import datetime
import io
import os
import polars as pl

from dataclasses import dataclass
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

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
        try:
            PROJECT_ID = os.getenv("PROJECT_ID")
            PRIVATE_KEY_ID = os.getenv("PRIVATE_KEY_ID")
            PRIVATE_KEY = os.getenv("PRIVATE_KEY")

            credentials_info = {
                "type": "service_account",
                "project_id": PROJECT_ID,
                "private_key_id": PRIVATE_KEY_ID,
                "private_key": PRIVATE_KEY,
                "client_email": f"demo-bigquery@{PROJECT_ID}.iam.gserviceaccount.com",
                "client_id": "117744405171076823965",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/demo-bigquery%40{PROJECT_ID}.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
                }
            print(f"Succeded getting credential: {credentials_info}")
        except:
            print(f"Failed getting credential: {credentials_info}")

        credentials = Credentials.from_service_account_info(credentials_info)
        print(f"Credentials: {credentials}")
        client = bigquery.Client(credentials=credentials)
        print(client)

        # Write DataFrame to stream as parquet file; does not hit disk
        with io.BytesIO() as stream:
            self.df.write_parquet(stream)
            # df.head(5).write_parquet(stream)
            stream.seek(0)
            job = client.load_table_from_file(
                stream,
                destination=f'{PROJECT_ID}.demobigquery.arrivals-bcn',
                project=PROJECT_ID,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    
                ),
            )
        job.result()  # Waits for the job to complete


def store_logs(all_data: pl.DataFrame):
        try:
            logs = pl.read_csv("data/logs.csv")
            logs = logs.with_columns(pl.col("timestamp").str.strptime(pl.Date))
        except:
            logs = pl.DataFrame()
        
        logs = pl.concat([
            logs,
            pl.DataFrame([{"rows": all_data.shape[0], "timestamp": datetime.date.today()}])
        ])
        logs.unique().write_csv("data/logs.csv")