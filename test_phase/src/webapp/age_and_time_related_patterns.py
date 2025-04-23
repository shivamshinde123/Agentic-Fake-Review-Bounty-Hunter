from collections import Counter
from datetime import datetime, timedelta
from neo4j_db_operations import Neo4jHandler
from sentiment_analysis import get_sentiment_label
from sentence_transformers import SentenceTransformer, util

class AgeTimeRelatedPatterns:

    def __init__(self):
        self.restricted_categories = [
            "Nightlife",
            "Bars",
            "Cocktail Bars",
            "Sushi Bars",
            "Sports Bars",
            "Wine Bars",
            "Juice Bars & Smoothies",
            "Dive Bars",
            "Tapas Bars",
            "Tobacco Shops",
            "Casinos",
            "Tattoo",
            "Karaoke",
            "Hookah Bars",
            "Whiskey Bars",
            "Tiki Bars",
            "Adult Entertainment",
            "Vape Shops",
            "Gay Bars",
            "Pool Halls",
            "Piano Bars",
            "Cannabis Clinics",
            "Cannabis Dispensaries",
            "Tattoo Removal",
            "Lingerie",
            "Cigar Bars",
            "Champagne Bars",
            "Strip Clubs",
            "Drive-Thru Bars",
            "nightclub",
        ]

        self.uri = "bolt://localhost:7687"   # Neo4j server URI
        self.user = "neo4j"                  # Username
        self.password = "password"  
        self.handler = Neo4jHandler(self.uri, self.user, self.password)

    def authenticate_appropriateness_for_children(self, review):

        model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

        labels = [
            f"This is a {category}, which is inappropriate for children under 18"
            for category in self.restricted_categories
        ]

        review_embedding = model.encode(review, convert_to_tensor=True)
        labels_embedding = model.encode(labels, convert_to_tensor=True)

        cosine_similarities = util.cos_sim(review_embedding, labels_embedding)

        threshold = 0.4

        is_inappropriate = any(sim > threshold for sim in cosine_similarities[0])

        return is_inappropriate
    
    def check_temporal_burst_with_sentiment(self, user_id, business_id):

        relationship_list = self.handler.fetch_relationships(user_id, business_id)

        reviews = list()
        dates = list()

        for rel in relationship_list:
            reviews.append(rel['text'])
            dates.append(datetime.strptime(rel['date'], "%Y-%m-%d %H:%M:%S"))

        recent_reviews = [review for review, date in zip(reviews, dates) if datetime.now() - date <= timedelta(hours=48)]
        
        count = len(recent_reviews)

        if count > 10:
            return True
        
        sentiments = recent_reviews.apply(lambda review: get_sentiment_label(review))

        sentiment_counts = Counter(sentiments)

        if len(sentiments) in sentiment_counts.values():
            return True
        
    def check_temporal_burst_without_sentiment(self, user_id, business_id, deviation_threshold=3.0, review_limit=5, time_window_hours=48):
        relationship_list = self.handler.fetch_relationships(user_id, business_id)

        if not relationship_list:
            return False

        flagged_reviews = 0
        now = datetime.now()

        for rel in relationship_list:
            try:
                review_time = datetime.strptime(rel['date'], "%Y-%m-%d %H:%M:%S")
                deviation = float(rel.get('rating_deviation', 0))

                if now - review_time <= timedelta(hours=time_window_hours) and deviation >= deviation_threshold:
                    flagged_reviews += 1
            except Exception as e:
                print(f"Error parsing review or deviation: {e}")

        return flagged_reviews >= review_limit


if __name__ == "__main__":

    review1 = "The toy store had a magical selection. My kids loved it!"  # appropriate
    review2 = "Had a wild night at this nightclub, the music and drinks were insane!"  # Inappropriate

    atp = AgeTimeRelatedPatterns()

    is_inappropriate = atp.authenticate_appropriateness_for_children(review2)

    print(is_inappropriate)
