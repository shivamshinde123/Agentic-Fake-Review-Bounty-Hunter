from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, lower
from pyspark.sql.types import StringType
import shutil
import sqlite3
import os
import pandas as pd

def create_table(db_path):
    """Creates a table to store all JSON fields in SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create Table for Reviews, USers and Business JSOn files
    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS reviews (
        review_id TEXT PRIMARY KEY,
        user_id TEXT,
        business_id TEXT,
        stars INTEGER,
        useful INTEGER,
        funny INTEGER,
        cool INTEGER,
        text TEXT,
        date TEXT
    );               
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        name TEXT,
        review_count INTEGER,
        yelping_since TEXT,
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
    );
    CREATE TABLE IF NOT EXISTS business (
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
    """)

    conn.commit()
    conn.close()

def df_to_table(df, table_name, jdbc_url):
    df.coalesce(1).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", "org.sqlite.JDBC") \
        .mode("overwrite") \
        .save()

def preprocess_data(spark, jdbc_url):
    try:
        # Load Reviews data
        reviews_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_review.json")
        processed_reviews_df = reviews_df.select(
            col("review_id"),
            col("user_id"),
            col("business_id"),
            col("stars"),
            col("useful"),
            col("funny"),
            col("cool"),
            regexp_replace(col("text"), r"\s+", " ").alias("text"),
            col("date")
        )

        df_to_table(processed_reviews_df, "reviews", jdbc_url)
        print("✅ Reviews.JSON Data successfully saved to SQLite!")

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
            col("average_stars"),
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
        print("✅ Users.JSON Data successfully saved to SQLite!")

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
        print("✅ Business.JSON Data successfully saved to SQLite!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        
def data_processing(db_path):
    conn = sqlite3.connect(db_path)

    users_df = pd.read_sql_query("SELECT * from users", conn)
    business_df = pd.read_sql_query("SELECT * from business", conn)
    reviews_df = pd.read_sql_query("SELECT * from reviews", conn)

    # Checking the Dataframes if any null values exist
    print(users_df.isnull().sum())
    print(business_df.isnull().sum())
    print(reviews_df.isnull().sum())

    # Merge all the 3 dataframes for further NLP Feature Engineering
    merged_df = reviews_df.merge(users_df, on="user_id", how="left")
    df = merged_df.merge(business_df, on="business_id", how="left")

    df.head()


if __name__ == "__main__":
    db_path = 'D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_dataset.db'
    jdbc_url = "jdbc:sqlite:D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_dataset.db"

    if not os.path.exists(db_path):
        print("Database file not found. Creating tables and preprocessing data...")
        create_table(db_path)

        os.environ["HADOOP_HOME"] = "D:\\Hadoop"
        spark_temp_dir = "D:\\Projects\\Agentic Fake Review Bounty Hunter\\test_phase\\spark_temp"

        # Create Spark session with Windows-friendly settings
        spark = SparkSession.builder \
            .appName("YelpDataPreprocessing") \
            .master("local[*]") \
            .config("spark.driver.host", "localhost") \
            .config("spark.local.dir", spark_temp_dir) \
            .config("spark.sql.legacy.allowUntypedScalaUDF", "true") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=D:\\Projects\\Agentic Fake Review Bounty Hunter\\test_phase\\spark_temp") \
            .config("spark.jars", "D:\\Projects\\Agentic Fake Review Bounty Hunter\\test_phase\\data\\processed\\sqlite-jdbc.jar") \
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false") \
            .getOrCreate()

        preprocess_data(spark, jdbc_url)
        spark.stop()
    else:
        print("Database file exists. Skipping table creation and data preprocessing.")
        data_processing(db_path)