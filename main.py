import datetime
import os

from src.extract import auth_param, get_data, clean_data, DataLoader
from dotenv import load_dotenv

def main():
    load_dotenv()
    ENDPOINT = "/flights/arrival"
    airport = "LEBL"

    auth = auth_param(
        username=os.getenv("OPEN_SKY_USERNAME"),
        password=os.getenv("OPEN_SKY_PASSWORD")
    )

    today = datetime.date.today()
    end_date = datetime.datetime(today.year, today.month, today.day)
    begin_date = end_date - datetime.timedelta(days=1)

    begin = int(begin_date.timestamp())
    end = int(end_date.timestamp())

    data = get_data(ENDPOINT, airport, begin, end, auth)
    data = clean_data(data)
    loader = DataLoader(data)
    loader.load_locally()


if __name__ == "__main__":
    main()