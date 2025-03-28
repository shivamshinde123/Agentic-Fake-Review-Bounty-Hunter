from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace, lower
from pyspark.sql.types import StringType
import shutil
import sqlite3
import os


def create_table():
    """Creates a table to store all JSON fields in SQLite."""
    conn = sqlite3.connect('D:/Projects/Agentic Fake Review Bounty Hunter/test_phase/data/processed/yelp_reviews.db')
    cursor = conn.cursor()

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

def preprocess_data():
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
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=D:\\spark_temp") \
        .config("spark.jars", "D:\\Projects\\Agentic Fake Review Bounty Hunter\\test_phase\\data\\processed\\sqlite-jdbc.jar") \
        .getOrCreate()

    try:
        # Load data
        reviews_df = spark.read.json("data/raw/yelp_academic_dataset_review.json")
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

        print("✅ Data successfully saved to SQLite!")
        spark.stop()

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        

if __name__ == "__main__":
    create_table()
    preprocess_data()
    