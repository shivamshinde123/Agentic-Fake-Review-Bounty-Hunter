import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ✅ Configure PostgreSQL connection with pooling
engine = create_engine(
    os.environ['SQL_ENGINE'] + "?keepalives=1&connect_timeout=30",
    isolation_level="AUTOCOMMIT",
    pool_size=5,
    max_overflow=10,
    pool_timeout=60,
    connect_args={"application_name": "ETL_Optimized"}
)

def create_sample_data(engine, rows=2000000):
    print("⚙️ Sampling and saving 2M rows directly from PostgreSQL...")
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS yelp_sample_data AS
            SELECT *
            FROM merged_full_data
            ORDER BY RANDOM()
            LIMIT {rows};
        """))
    print("✅ Sampled data saved to 'yelp_sample_data'")

def data_processing():
    print("🔍 Loading sampled data into pandas DataFrame...")
    df = pd.read_sql("SELECT * FROM yelp_sample_data", engine)

    df["review_length"] = df["text"].fillna('').apply(lambda x: len(x.strip().split()))
    df["yelping_since"] = pd.to_datetime(df["yelping_since"], errors="coerce")
    df["yelping_since_days"] = (pd.Timestamp.today() - df["yelping_since"]).dt.days

    df["elite"] = df["elite"].fillna('').astype(str)
    df["elite_years"] = df["elite"].apply(lambda x: len(x.split(',')) if x.strip() else 0)

    df["user_friends"] = df["user_friends"].fillna('').astype(str)
    df["friends_count"] = df["user_friends"].apply(lambda x: len(x.split(',')) if x.strip() else 0)

    df["review_stars"] = pd.to_numeric(df["review_stars"], errors="coerce")
    df["biz_stars"] = pd.to_numeric(df["biz_stars"], errors="coerce")
    df["rating_deviation"] = (df["review_stars"] - df["biz_stars"]).abs()

    print("💾 Saving processed sample data back to PostgreSQL...")
    df.to_sql("yelp_sample_data", engine, if_exists="replace", index=False)
    print("✅ Data processed and saved successfully.")

def csv_data(file_path):
    print("🔍 Loading processed sampled data into pandas DataFrame...")
    df = pd.read_sql("SELECT * FROM yelp_sample_data", engine)

    print("Saving DataFrame into CSV Format...")
    df.to_csv(file_path + "yelp_sample_data.csv", index=False, encoding="utf-8")
    print("✅ Data saved in CSV Format successfully.")

if __name__ == "__main__":
    create_sample_data(engine)
    data_processing()
    csv_data(file_path="test_phase/data/processed/")
