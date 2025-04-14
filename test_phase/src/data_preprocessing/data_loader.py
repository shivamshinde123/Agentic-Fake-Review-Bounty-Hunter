from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace
import shutil
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def create_table(engine):
    # Create Table for Reviews, USers and Business JSOn files
    with engine.connect() as conn:
        conn.execute(text(
        """CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY,
            user_id TEXT,
            business_id TEXT,
            stars REAL,
            useful INTEGER,
            funny INTEGER,
            cool INTEGER,
            text TEXT,
            date TIMESTAMP
        );"""))              
        conn.execute(text("""CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            review_count INTEGER,
            yelping_since TIMESTAMP,
            useful INTEGER,
            funny INTEGER,
            cool INTEGER,
            elite TEXT,
            friends TEXT,
            fans INTEGER,
            average_stars REAL,
            compliment_hot INTEGER,
            compliment_more INTEGER,
            compliment_profile INTEGER,
            compliment_cute INTEGER,
            compliment_list INTEGER,
            compliment_note INTEGER,
            compliment_plain INTEGER,
            compliment_cool INTEGER,
            compliment_funny INTEGER,
            compliment_writer INTEGER,
            compliment_photos INTEGER
        );"""))
        conn.execute(text("""CREATE TABLE IF NOT EXISTS business (
            business_id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            latitude REAL,
            longitude REAL,
            stars REAL,
            review_count INTEGER,
            is_open INTEGER,
            categories TEXT
        );
        """))

def df_to_table(df, table_name, jdbc_url):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def save_data(spark, jdbc_url):
    try:
        # Load Reviews data
        reviews_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_review.json")
        processed_reviews_df = reviews_df.select(
            col("review_id"),
            col("user_id"),
            col("business_id"),
            col("stars").cast("float"),
            col("useful"),
            col("funny"),
            col("cool"),
            regexp_replace(col("text"), r"\s+", " ").alias("text"),
            col("date")
        )

        df_to_table(processed_reviews_df, "reviews", jdbc_url)
        print("✅ Reviews.JSON Data successfully saved to PostgreSQL!")

        # Load Users data
        users_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_user.json")
        processed_users_df = users_df.select(
            col("user_id"),
            col("name"), 
            col("review_count"), 
            col("yelping_since"), 
            col("useful"),
            col("funny"), 
            col("cool"), 
            col("elite"), 
            col("friends"), 
            col("fans"),
            col("average_stars").cast("float"),
            col("compliment_hot"),
            col("compliment_more"),
            col("compliment_profile"),
            col("compliment_cute"),
            col("compliment_list"),
            col("compliment_note"),
            col("compliment_plain"),
            col("compliment_cool"),
            col("compliment_funny"),
            col("compliment_writer"),
            col("compliment_photos")
        )

        df_to_table(processed_users_df, "users", jdbc_url)
        print("✅ Users.JSON Data successfully saved to PostgreSQL!")

        # Load Business data
        business_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_business.json")
        processed_business_df = business_df.select(
            col("business_id"),
            col("name"),
            col("address"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("latitude"),
            col("longitude"),
            col("stars"),
            col("review_count"),
            col("is_open"),
            col("categories")
        )

        df_to_table(processed_business_df, "business", jdbc_url)
        print("✅ Business.JSON Data successfully saved to PostgreSQL!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        
def merge_data(engine):

    query1 = """
        CREATE TABLE IF NOT EXISTS merged_data AS
        SELECT
            r.review_id, r.user_id, r.business_id, r.stars AS review_stars, r.text,
            u.review_count AS user_review_count, u.yelping_since, u.elite, u.friends, u.fans
        FROM reviews r
        JOIN users u ON r.user_id = u.user_id
    """
    
    query2 = """
        CREATE TABLE IF NOT EXISTS merged_full_data AS
        SELECT
            m.review_id, m.user_id, m.business_id, m.review_stars, m.text,
            m.user_review_count, m.yelping_since, m.elite, m.friends, m.fans,
            b.city, b.state, b.stars AS biz_stars, b.review_count AS biz_review_count, b.categories
        FROM merged_data m
        JOIN business b ON m.business_id = b.business_id
    """

    print("Merging all the 3 DataFrames.....")
    
    with engine.begin() as conn:
        print(f"Merging Phase 1 in progress....")
        conn.execute(text(query1))
        print(f"Merging Phase 2 in progress....")
        conn.execute(text(query2))

    print("✅ Merged data completed!")

def data_processing(engine):
    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM merged_full_data", engine)
        print(df.head())

if __name__ == "__main__":
    load_dotenv()
    jdbc_url = os.environ['JDBC_URL']

    os.environ["HADOOP_HOME"] = "D:\\Hadoop"

    # Create Spark session with Windows-friendly settings
    spark = SparkSession.builder \
        .appName("YelpDataPreprocessing") \
        .master("local[*]") \
        .config("spark.jars", "C:\\drivers\\postgresql-42.6.2.jar") \
        .getOrCreate()
    
    # Build SQAlchemy Engine
    engine =  create_engine(
        os.environ['SQL_ENGINE'] + "?keepalives=1&keepalives_idle=30&keepalives_interval=10&connect_timeout=30"
        )

    print("Creating tables and preprocessing data using PostgreSQL(SQLAlchemy).....")
    # Create SQL Tables
    create_table(engine)

    # Saving JSON data in created SQL Database
    save_data(spark, jdbc_url)
    print("Saved data successfully into PostgreSQL")
    spark.stop()
    shutil.rmtree("C:/Users/Ankit/AppData/Local/Temp/spark-xxx", ignore_errors=True)

    print("Data Preprocessing......")
    merge_data(engine)
    