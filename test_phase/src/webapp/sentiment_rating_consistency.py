from neo4j_db_operations import Neo4jHandler
from sentiment_analysis import get_sentiment_label

class SentimentRatingConsistencyPattern:

    def __init__(self):
        self.uri = "bolt://localhost:7687"
        self.user = "neo4j"
        self.password = "password"
        self.handler = Neo4jHandler(self.uri, self.user, self.password)

    def sentiment_matches_rating(self, sentiment, stars):
        """
        Check if the sentiment matches the given star rating.
        """
        if sentiment == "positive" and stars >= 4:
            return True
        elif sentiment == "neutral" and stars == 3:
            return True
        elif sentiment == "negative" and stars <= 2:
            return True
        return False

    def detect_inconsistent_sentiment_reviews(self, user_id, allowed_mismatches=2):
        """
        Detect if a user has inconsistent reviews.

        Args:
            user_id (str): The user ID.
            allowed_mismatches (int): Number of mismatches allowed before flagging.

        Returns:
            str: 'Fake' if suspicious, 'Real' if clean.
        """

        query = """
        MATCH (u:User {user_id: $user_id})-[r:REVIEWED]->(b:Business)
        RETURN r.text as text, r.review_stars as stars
        """

        mismatches = 0

        with self.handler.driver.session() as session:
            result = session.run(query, user_id=user_id)
            for record in result:
                text = record["text"]
                stars = record["stars"]
                sentiment = get_sentiment_label(text)

                if not self.sentiment_matches_rating(sentiment, stars):
                    mismatches += 1

                if mismatches > allowed_mismatches:
                    return "Fake"

        return "Real"

if __name__ == "__main__":

    srcp = SentimentRatingConsistencyPattern()

    user_id = "u12345"  # Example

    result = srcp.detect_inconsistent_sentiment_reviews(user_id)
    print(f"Sentiment-Rating Consistency Result: {result}")
