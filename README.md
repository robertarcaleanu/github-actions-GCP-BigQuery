# ETL with Github Actions and GCP
This repository consists of a project that extracts data automatically using github actions and loads it to BigQuery (GCP).

## Data Extraction

The data is retrieved from the [OpenSky REST API](https://openskynetwork.github.io/opensky-api/rest.html). In this particular case, we are using the `GET /flights/arrival` endpoint to obtain the daily arrivals to the Barcelona Airport (BCN, LEBL).

## Pipeline Automation

In order to make automate the pipeline and execute it daily, we are using Github Actions to automate the process. To do so, it is necessary to create this directory (`.github/workflows`) where the workflow is defined in a `.yml` format.

This project follows the steps indicated in [this](https://github.com/ShawhinT/data-pipeline-example?tab=readme-ov-file) repository. The main steps that have been followed are listed below:

1. Define workflow name
2. Define when to execute the workflow (manually or on schedule)
3. Define the job

    1. Define OS to run on
    2. Define the steps 

## Data Storage
In this section we provide the two options that have been chosen to store the data.

### Local Storage
Initially, the data is stored locally as it is easier to configure. The data can be found in `data/data.parquet` file. The data is a table with the following columns:

* icao24
* departure_time
* arrival_time
* departure_airport
* arrival_airport
* flight_number

In this case, the data is stored in a parquet file. Youn can read about its benefits [here](https://parquet.apache.org/).

### GCP
Usually, companies rely on Cloud Datawarehouses to store their data. There are different options in the market, such as Snowflake, AWS Redshift, Databricks, BigQuery, among others.

For the purpose of this project, we are using BigQuery. BigQuery is a serverless data warehouse provided by GCP.

**Placeholder: EXPLAIN HOW TO UPLOAD THE DATA**
