import requests
import pandas as pd
import logging
from io import StringIO
import boto3
import os

def fetch_smoking_prevalence(country_code: str):
    url = "https://ghoapi.azureedge.net/api/M_Est_cig_curr"  # cigarette prevalence
    resp = requests.get(url)
    data = resp.json()

    records = []
    for rec in data["value"]:
        if rec.get("SpatialDim") == country_code and rec.get("TimeDim"):
            records.append({
                "country": rec.get("SpatialDim"),
                "year": int(rec.get("TimeDim")),   # safe now
                "value": float(rec.get("NumericValue")) if rec.get("NumericValue") else None,
                "sex": rec.get("Dim1"),
                "indicator": rec.get("IndicatorName")
            })

    return pd.DataFrame(records)

sex_map = {
    'SEX_BTSX' : "Both sexes",
    'SEX_MLE' : 'Male',
    'SEX_FMLE' : 'Female'
}

def clean_smoking_data(df) -> pd.DataFrame:
    df['sex'] = df['sex'].map(sex_map).fillna(df['sex'])

    current_year = pd.Timestamp.today().year
    df = df[df['year'] <= current_year]

    df = df.drop(columns = ['indicator'])
    df = df.sort_values(['year', 'sex'])
    return df

def save_data_locally(df, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    logging.info(f"Saved local smoking prevalence CSV to {filepath}")

# def upload_to_s3(df, bucket, key):
#     csv_buffer = StringIO()
#     df.to_csv(csv_buffer, index=False)
#     s3 = boto3.client("s3")
#     s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
#     logging.info(f"Uploaded smoking data to s3://{bucket}/{key}")

if __name__ == "__main__":
    
    df = fetch_smoking_prevalence("IND")
    df_clean = clean_smoking_data(df)
    save_data_locally(df_clean, "data/raw/smoking.csv")
    print("Data saved to data/raw locally")
