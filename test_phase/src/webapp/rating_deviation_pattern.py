import pandas as pd
from ace_tools import display_dataframe_to_user

# Load datasets
businesses = pd.read_csv('business.csv')
reviews = pd.read_csv('reviews.csv')

# Merge to get business average stars
df = reviews.merge(
    businesses[['business_id', 'stars']],
    on='business_id',
    how='left',
    suffixes=('', '_biz')
)

# Compute absolute deviation and flag
df['rating_deviation'] = (df['stars'] - df['stars_biz']).abs()
df['flagged'] = df['rating_deviation'] >= 2

# Prepare and display True/False output
output = df[['review_id', 'flagged']]
display_dataframe_to_user('Review Flagged (True/False)', output)
