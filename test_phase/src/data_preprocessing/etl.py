from pyspark.sql import SparkSession
import os
import pandas as pd
import dask.dataframe as dd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


if __name__ == "__main__":
    load_dotenv()
    jdbc_url = os.environ['JDBC_URL']
    engine =  create_engine(os.environ['SQL_ENGINE'])

    chunk_size = 100_000
    chunk_data = pd.read_sql("SELECT * FROM merged_full_data", engine, chunksize=chunk_size)

    for i, chunk in enumerate(chunk_data):
        print(f"🔄 Processing batch {i+1}")
        print(chunk.head(1))
        
        # df["review_length"] = df["text"].apply(lambda x: len(x.split()))
        # df["yelping_since"] = pd.to_datetime(df["yelping_since"], errors="coerce")
        # df["account_age_days"] = (pd.to_datetime("today") - df["yelping_since"]).dt.days
        # df["elite_years"] = df["elite"].apply(lambda x: len(x.split(",")) if pd.notnull(x) and x!="" else 0)
        # df["friends_count"] = df["friends"].apply(lambda x: len(x.split(",")) if pd.notnull(x) and x!="" else 0)
        # df["rating_deviation"] = np.abs(df["review_stars"] - df["biz_stars"])