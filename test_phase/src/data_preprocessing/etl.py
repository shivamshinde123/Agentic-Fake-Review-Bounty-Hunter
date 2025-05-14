import os
import pandas as pd
import numpy as np
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

def create_sample_data(engine):
    print("⚙️ Sampling and saving rows directly from PostgreSQL...")
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS yelp_data AS
            SELECT *
            FROM merged_full_data
            ORDER BY RANDOM()
            LIMIT 10000;
        """))
    print("✅ Sampled data saved to 'yelp_data'")

def data_processing():
    print("🔍 Loading sampled data into pandas DataFrame...")
    df = pd.read_sql("SELECT * FROM yelp_data", engine)

    # df["review_length"] = df["text"].fillna('').apply(lambda x: len(x.strip().split()))
    # df["yelping_since"] = pd.to_datetime(df["yelping_since"], errors="coerce")
    # df["yelping_since_days"] = (pd.Timestamp.today() - df["yelping_since"]).dt.days

    # df["elite"] = df["elite"].fillna('').astype(str)
    # df["elite_years"] = df["elite"].apply(lambda x: len(x.split(',')) if x.strip() else 0)

    # df["user_friends"] = df["user_friends"].fillna('').astype(str)  
    # df["friends_count"] = df["user_friends"].apply(lambda x: len(x.split(',')) if x.strip() else 0)

    df["review_stars"] = pd.to_numeric(df["review_stars"], errors="coerce")
    df["biz_stars"] = pd.to_numeric(df["biz_stars"], errors="coerce")
    df["rating_deviation"] = (df["review_stars"] - df["biz_stars"]).abs()

    # df["user_age"] = np.random.randint(12, 71, size=len(df))

    print("💾 Saving processed sample data back to PostgreSQL...")
    df.to_sql("yelp_data", engine, if_exists="replace", index=False)
    print("✅ Data processed and saved successfully.")

def csv_data(engine):
    with engine.begin() as conn:
        print("🔍 Loading processed sampled data into pandas DataFrame...")
        reviews_df = pd.read_sql("SELECT review_id, user_id, business_id, review_stars, text, date, rating_deviation FROM yelp_data", engine)
        reviews_df.to_csv("test_phase/data/processed/reviews.csv", index=False, encoding="utf-8")

        users_df = pd.read_sql("SELECT DISTINCT ON (user_id) user_id, user_name, user_review_count, yelping_since, elite, friends, average_stars, user_age FROM yelp_data", engine)
        users_df.to_csv("test_phase/data/processed/users.csv", index=False, encoding="utf-8")

        business_df = pd.read_sql("SELECT DISTINCT ON (business_id) business_id, biz_name, biz_city, biz_state, biz_stars, biz_review_count, categories FROM yelp_data", engine)
        business_df.to_csv("test_phase/data/processed/business.csv", index=False, encoding="utf-8")

        # users_df = pd.read_sql("SELECT user_id, user_name, user_review_count, yelping_since, elite, friends, average_stars, user_age FROM users", engine)
        # users_df.to_csv("test_phase/data/processed/users.csv", index=False, encoding="utf-8")

        # business_df = pd.read_sql("SELECT business_id, biz_name, biz_city, biz_state, biz_stars, biz_review_count, categories FROM business", engine)
        # business_df.to_csv("test_phase/data/processed/business.csv", index=False, encoding="utf-8")

    print("✅ Data saved in CSV Format successfully.")

if __name__ == "__main__":
    create_sample_data(engine)
    data_processing()
    csv_data(engine)
