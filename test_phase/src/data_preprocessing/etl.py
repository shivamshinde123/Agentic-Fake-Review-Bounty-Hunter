from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, lower
from pyspark.sql.types import StringType
import shutil
import sqlite3
import os


def create_review_table():
    """Creates a table to store all JSON fields in SQLite."""
    conn = sqlite3.connect('D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db')
    cursor = conn.cursor()

    # Create Table for Reviews.JSON
    cursor.execute("""
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
    """)

    conn.commit()
    conn.close()

def create_user_table():
    conn = sqlite3.connect('D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db')
    cursor = conn.cursor()

    # Create Table for Users.JSON
    cursor.execute("""
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
    """)

    conn.commit()
    conn.close()

def create_business_table():
    conn = sqlite3.connect('D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db')
    cursor = conn.cursor()

    # Create Table for Business.JSON
    cursor.execute("""
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

def preprocess_review_data(spark):
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

        # Drop rows with null values
        processed_reviews_df = processed_reviews_df.filter(col('text').isNotNull())

        processed_reviews_df = processed_reviews_df.coalesce(1)
        jdbc_url = "jdbc:sqlite:D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db"

        processed_reviews_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "reviews") \
            .option("driver", "org.sqlite.JDBC") \
            .mode("overwrite") \
            .save()

        print("✅ Reviews.JSON Data successfully saved to SQLite!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

def preprocess_user_data(spark):
    try:
        # Load Users data
        users_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_review.json")
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

        processed_users_df = processed_users_df.coalesce(1)
        jdbc_url = "jdbc:sqlite:D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db"

        processed_users_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "users") \
            .option("driver", "org.sqlite.JDBC") \
            .mode("overwrite") \
            .save()

        print("✅ Users.JSON Data successfully saved to SQLite!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

def preprocess_business_data(spark):
    try:
        # Load Business data
        business_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_review.json")
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

        processed_business_df = processed_business_df.coalesce(1)
        jdbc_url = "jdbc:sqlite:D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db"

        processed_business_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "business") \
            .option("driver", "org.sqlite.JDBC") \
            .mode("overwrite") \
            .save()

        print("✅ Business.JSON Data successfully saved to SQLite!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        

if __name__ == "__main__":
    create_review_table()
    create_user_table()

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

    preprocess_review_data(spark)
    preprocess_user_data(spark)
    preprocess_business_data(spark)
    
    spark.stop()