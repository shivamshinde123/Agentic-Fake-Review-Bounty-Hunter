from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace
import shutil
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def create_table(engine):
    # Create Table for Reviews, Users and Business JSOn files
    with engine.connect() as conn:
        conn.execute(text(
            """CREATE TABLE IF NOT EXISTS users (
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
            );"""
        ))
        conn.execute(text(
            """CREATE TABLE IF NOT EXISTS business (
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
            );"""
        ))
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
                date TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (business_id) REFERENCES business(business_id)
            );"""
        ))

def save_filtered_data(spark, jdbc_url):
    try:
        # Load and Save business data
        business_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_business.json")
        business_df = business_df.select(
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

        df_to_table(business_df, "business", jdbc_url)
        print("✅ Business.JSON Data successfully saved to PostgreSQL!")
        business_ids_df = business_df.select("business_id")

        # Load and Save users data
        users_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_user.json")
        users_df = users_df.select(
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

        df_to_table(users_df, "users", jdbc_url)
        print("✅ Users.JSON Data successfully saved to PostgreSQL!")
        users_ids_df = users_df.select("user_id")

        # Load review data
        reviews_df = spark.read.json("test_phase/data/raw/yelp_academic_dataset_review.json")

        # Filter reviews only for existing business_id AND user_id
        filtered_reviews_df = reviews_df.join(business_ids_df, on="business_id", how="inner") \
                                        .join(users_ids_df, on="user_id", how="inner")

        selected_reviews_df = filtered_reviews_df.select(
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

        df_to_table(selected_reviews_df, "reviews", jdbc_url)
        print("✅ Filtered Reviews.JSON Data successfully saved to PostgreSQL!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")


def df_to_table(df, table_name, jdbc_url):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 10000) \
        .option("numPartitions", 8) \
        .option("isolationLevel", "NONE") \
        .mode("overwrite") \
        .save()
        
def merge_data(engine):
    print("Merging all the 3 DataFrames.....")
    
    with engine.begin() as conn:
        print("🔄 Creating merged_data (reviews + users)...")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS merged_data AS
            SELECT
                r.review_id, r.user_id, r.business_id, r.stars AS review_stars, r.text,
                u.review_count AS user_review_count, u.yelping_since, u.elite, u.friends, u.fans
            FROM reviews r
            JOIN users u ON r.user_id = u.user_id
        """))
        print("🔄 Creating merged_full_data (merged_data + business)...")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS merged_full_data AS
            SELECT
                m.review_id, m.user_id, m.business_id, m.review_stars, m.text,
                m.user_review_count, m.yelping_since, m.elite, m.friends as user_friends, m.fans,
                b.city as biz_city, b.state as biz_state, b.stars AS biz_stars, b.review_count AS biz_review_count, b.categories
            FROM merged_data m
            JOIN business b ON m.business_id = b.business_id
        """))

    print("✅ Merged data completed!")

def data_to_csv(engine):
    with engine.begin() as conn:
        reviews_df = pd.read_sql("SELECT * FROM reviews LIMIT 2000000", engine)
        reviews_df.to_csv("test_phase/data/processed/reviews.csv", index=False, encoding="utf-8")

        users_df = pd.read_sql("SELECT * FROM users", engine)
        users_df.to_csv("test_phase/data/processed/users.csv", index=False, encoding="utf-8")

        business_df = pd.read_sql("SELECT * FROM business", engine)
        business_df.to_csv("test_phase/data/processed/business.csv", index=False, encoding="utf-8")

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

    print("🧱 Creating tables...")
    create_table(engine)

    # Saving JSON data in created SQL Database
    print("🚚 Saving JSON to PostgreSQL...")
    #save_data(spark, jdbc_url)
    save_filtered_data(spark, jdbc_url)
    
    spark.stop()
    shutil.rmtree("C:/Users/Ankit/AppData/Local/Temp/spark-xxx", ignore_errors=True)

    print("🛠️ Merging and preprocessing...") 
    merge_data(engine)
    data_to_csv(engine)
    