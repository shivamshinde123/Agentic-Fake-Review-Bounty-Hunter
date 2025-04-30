from neo4j_db_operations import Neo4jHandler
from datetime import datetime, timedelta

class RatingDeviationPatterns:

    def __init__(self):
        self.uri = "bolt://localhost:7687"  # Neo4j server URI
        self.user = "neo4j"  # Username
        self.password = "12345678"  
        self.handler = Neo4jHandler(self.uri, self.user, self.password)

    def detect_high_rating_deviation(self, user_id, deviation_threshold=2.5, min_reviews=3):
        """
        Detect if a user has posted multiple reviews with a high rating deviation.

        Args:
            user_id (str): The user_id of the user to check.
            deviation_threshold (float): The minimum deviation considered suspicious.
            min_reviews (int): Minimum number of high-deviation reviews to flag.

        Returns:
            bool: True if suspicious behavior detected, False otherwise.
        """

        query = """
        MATCH (u:User {user_id: $user_id})-[r:REVIEWED]->(b:Business)
        WHERE r.rating_deviation >= $deviation_threshold
        RETURN COUNT(r) as high_deviation_count
        """

        with self.handler.driver.session() as session:
            result = session.run(query, user_id=user_id, deviation_threshold=deviation_threshold)
            count = result.single()["high_deviation_count"]

        return count >= min_reviews

    def detect_recent_high_rating_deviation(self, user_id, deviation_threshold=2.5, min_reviews=2, time_window_hours=48):
        """
        Detect if a user recently posted multiple high deviation reviews within a specific time window.

        Args:
            user_id (str): The user_id of the user to check.
            deviation_threshold (float): The minimum deviation considered suspicious.
            min_reviews (int): Minimum number of high-deviation reviews to flag.
            time_window_hours (int): How many past hours to check.

        Returns:
            bool: True if suspicious behavior detected, False otherwise.
        """

        query = """
        MATCH (u:User {user_id: $user_id})-[r:REVIEWED]->(b:Business)
        WHERE r.rating_deviation >= $deviation_threshold
          AND datetime(r.date) >= datetime() - duration({hours: $time_window_hours})
        RETURN COUNT(r) as recent_high_deviation_count
        """

        with self.handler.driver.session() as session:
            result = session.run(
                query,
                user_id=user_id,
                deviation_threshold=deviation_threshold,
                time_window_hours=time_window_hours
            )
            count = result.single()["recent_high_deviation_count"]

        return count >= min_reviews

if __name__ == "__main__":

    rdp = RatingDeviationPatterns()
    
    user_id = "u12345"  # Example

    # Example use
    flagged = rdp.detect_high_rating_deviation(user_id)
    print(f"User flagged for high rating deviation: {flagged}")

    flagged_recent = rdp.detect_recent_high_rating_deviation(user_id)
    print(f"User flagged for recent high rating deviation: {flagged_recent}")
