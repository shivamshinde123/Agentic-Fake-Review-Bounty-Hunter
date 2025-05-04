import pandas as pd
import numpy as np
from scipy.stats import entropy, zscore
from datetime import datetime
import re

# Load and preprocess the data
df = pd.read_csv("./test_phase/data/processed/sentiment-analysis.csv")

# Remove rows with invalid IDs (start with '=')
df = df[~df['user_id'].astype(str).str.startswith('=')]
df = df[~df['business_id'].astype(str).str.startswith('=')]

# Sample ~2500 rows randomly
sampled_df = df.sample(n=2500, random_state=42).reset_index(drop=True)

# Convert datetime column
sampled_df['date'] = pd.to_datetime(sampled_df['date'], errors='coerce')

# Metric 1: Rating Deviation (from user average)
sampled_df['user_avg_rating'] = sampled_df.groupby('user_id')['review_stars'].transform('mean')
sampled_df['rating_deviation'] = np.abs(sampled_df['review_stars'] - sampled_df['user_avg_rating'])

# Metric 2: Sentiment Entropy per User
def compute_entropy(sentiments):
    counts = sentiments.value_counts(normalize=True)
    return entropy(counts)

sentiment_entropy = sampled_df.groupby('user_id')['sentiment'].apply(compute_entropy).reset_index()
sentiment_entropy.columns = ['user_id', 'sentiment_entropy']
sampled_df = sampled_df.merge(sentiment_entropy, on='user_id', how='left')

# Metric 3: Average Sentiment Score vs Business Rating
sentiment_map = {'positive': 2, 'neutral': 1, 'negative': 0}
sampled_df['sentiment_score'] = sampled_df['sentiment'].map(sentiment_map)
avg_sentiment_business = sampled_df.groupby('business_id')['sentiment_score'].mean().reset_index()
avg_sentiment_business.columns = ['business_id', 'avg_sentiment_score']
sampled_df = sampled_df.merge(avg_sentiment_business, on='business_id', how='left')

# Metric 4: Sentiment Spike Detection (Temporal Outliers)
sampled_df['timestamp'] = sampled_df['date'].astype("int64") // 10**9  # convert to seconds
sampled_df['zscore_time'] = zscore(sampled_df['timestamp'].fillna(0))
sampled_df['temporal_outlier'] = sampled_df['zscore_time'].abs() > 2

# Metric 5: Sentiment-Text Length Anomaly
sampled_df['text_length'] = sampled_df['text'].astype(str).apply(lambda x: len(x))
sampled_df['zscore_length'] = zscore(sampled_df['text_length'])
sampled_df['zscore_sentiment'] = zscore(sampled_df['sentiment_score'].fillna(0))
sampled_df['sentiment_length_anomaly'] = (sampled_df['zscore_length'].abs() > 2) & (sampled_df['zscore_sentiment'].abs() > 2)

# Save the processed DataFrame
sampled_df.to_csv("./test_phase/data/processed/sampled_sentiment_data.csv", index=False)
print("✅ Sampled and metrics-evaluated data saved to sampled_sentiment_data.csv")
