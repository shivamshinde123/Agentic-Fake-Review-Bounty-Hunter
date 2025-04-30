from sentence_transformers import SentenceTransformer, util
from neo4j_db_operations import Neo4jHandler

class DuplicateReviewAcrossBusinesses:

    def __init__(self, similarity_threshold=0.85):
        self.similarity_threshold = similarity_threshold
        self.model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

        self.uri = "bolt://localhost:7687"   # Neo4j server URI
        self.user = "neo4j"                  # Username
        self.password = "12345678"
        self.handler = Neo4jHandler(self.uri, self.user, self.password)

    def check_duplicate_reviews(self, user_id):
        """
        Check if a user has posted similar reviews across multiple businesses.
        Returns True if any review similarity is above the defined threshold.
        """
        # Fetch user relationships with reviews and businesses
        relationship_list = self.handler.fetch_user_reviews(user_id)

        if not relationship_list or len(relationship_list) < 2:
            return False

        # Extract reviews and corresponding business IDs
        reviews = [rel['text'] for rel in relationship_list]
        businesses = [rel['business_id'] for rel in relationship_list]

        # Create list of tuples (review, business_id)
        review_business_pairs = list(zip(reviews, businesses))

        # Compare all unique pairs of reviews across different businesses
        for i in range(len(review_business_pairs)):
            for j in range(i + 1, len(review_business_pairs)):
                review_i, business_i = review_business_pairs[i]
                review_j, business_j = review_business_pairs[j]

                if business_i != business_j:  # Only compare reviews from different businesses
                    emb_i = self.model.encode(review_i, convert_to_tensor=True)
                    emb_j = self.model.encode(review_j, convert_to_tensor=True)

                    sim_score = util.cos_sim(emb_i, emb_j).item()

                    if sim_score >= self.similarity_threshold:
                        return True  # Flag as potential duplicate review

        return False


if __name__ == "__main__":
    user_id = "example_user_id"

    detector = DuplicateReviewAcrossBusinesses()

    result = detector.check_duplicate_reviews(user_id)

    print("Duplicate reviews across businesses detected?" , result)
