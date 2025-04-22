from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from transformers import pipeline

dr_path = "D:/Projects/Agentic Fake Review Bounty Hunter/test_phase"

load_dotenv()

engine = create_engine(
    os.environ['SQL_ENGINE'] + "?keepalives=1&connect_timeout=30",
    isolation_level="AUTOCOMMIT",
    pool_size=5,
    max_overflow=10,
    pool_timeout=60,
    connect_args={"application_name": "FeatureExtractor"}
)

def load_data(source="sql", limit=10000):
    if source == "csv":
        return pd.read_csv("test_phase/data/processed/reviews.csv").head(limit)
    elif source == "sql":
        df = pd.read_sql(f"SELECT * FROM yelp_data LIMIT {limit}", engine)


def get_sentiment_label(text):
    try:
        return sentiment_analyzer(str(text)[:512])[0]["label"]
    except:
        return "NEUTRAL"
    
def get_sentiment_score(text):
    try:
        res = sentiment_analyzer(str(text)[:512])[0]
        return res["score"] if res["label"] == "POSITIVE" else -res["score"]
    except:
        return 0.0
    
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

df = pd.read_sql("SELECT * FROM yelp_data", engine)

df["sentiment"] = df["text"].apply(get_sentiment_label)
df["sentiment_score"] = df["text"].apply(get_sentiment_score)

df.to_sql("yelp_data", engine, if_exists="replace", index=False)
df.to_csv(f"{dr_path}/data/processed/sentiment-analysis.csv", index=False, encoding="utf-8")
print("✅ Feature extraction completed and saved.")