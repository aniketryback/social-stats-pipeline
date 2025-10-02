import requests
import json
import boto3
import pandas as pd
from io import StringIO
import logging
import os


def fetch_birth_rate(country_code = 'IND'):

    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.DYN.CBRT.IN?format=json&per_page=500"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data, status code {response.status_code}")
    data = response.json()

    records = []

    for entry in data[1]:
        records.append({
            
            "country" : entry["country"]["value"],
            "year" : int(entry["date"]),
            "birth_rate": entry["value"]
        })
        
    df = pd.DataFrame(records)
    logging.info(f"Fetched {len(df)} records for {country_code}")
    return df

def upload_to_s3(df, bucket, key, local_path = 'data/raw/raw_data.csv'):
    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False)
    s3.put_object(Bucket = bucket, Key = key, Body = csv_buffer.getvalue())
    logging.info(f"Uploaded {key} to s3://{bucket}")

    if local_path:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)  # make folder if not exists
        print("Saving file at:", os.path.abspath(local_path))
        df.to_csv(local_path, index=False)
        logging.info(f"Saved local copy at {local_path}")
        print(os.getcwd())


def save_data_locally(df, local_path: str):
    # Ensure folder exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Save DataFrame to CSV
    df.to_csv(local_path, index=False, encoding="utf-8")
    print(f"✅ File saved locally at {local_path}")


if __name__ == "__main__":
    # Fetch the data
    df = fetch_birth_rate("IND")

    # Upload to S3 and save locally
    save_data_locally(
    df,
    local_path="data/raw/india_birth_rate.csv"
)