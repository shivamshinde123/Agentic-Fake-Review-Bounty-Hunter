import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from ace_tools import display_dataframe_to_user

# Ensure VADER lexicon is available
nltk.download('vader_lexicon')

# Load reviews dataset
reviews = pd.read_csv('/mnt/data/reviews.csv')

# Initialize VADER sentiment analyzer
sia = SentimentIntensityAnalyzer()

# Compute compound sentiment score for each review
reviews['sentiment_compound'] = reviews['text'].apply(lambda txt: sia.polarity_scores(txt)['compound'])

# Flag mismatches
reviews['positive_mismatch'] = (reviews['sentiment_compound'] < 0.2) & (reviews['stars'] >= 4)
reviews['negative_mismatch'] = (reviews['sentiment_compound'] > 0.8) & (reviews['stars'] <= 2)
reviews['flagged'] = reviews['positive_mismatch'] | reviews['negative_mismatch']

# Extract flagged reviews for inspection
flagged = reviews.loc[reviews['flagged'], [
    'review_id', 'user_id', 'business_id',
    'stars', 'sentiment_compound',
    'positive_mismatch', 'negative_mismatch'
]]

# Display interactively
display_dataframe_to_user('Sentiment–Rating Mismatch Reviews', flagged)
