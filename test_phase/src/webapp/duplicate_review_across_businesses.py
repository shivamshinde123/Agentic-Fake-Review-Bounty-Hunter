from sentence_transformers import SentenceTransformer, util
from neo4j_db_operations import Neo4jHandler

class DuplicateReviewDetector:

    def __init__(self, similarity_threshold=0.85):
        self.similarity_threshold = similarity_threshold
        self.model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

        self.uri = "bolt://localhost:7687"   # Neo4j server URI
        self.user = "neo4j"                  # Username
        self.password = "12345678"
        self.handler = Neo4jHandler(self.uri, self.user, self.password)

    def check_all_users_for_duplicate_reviews(self):
        """Check all users and flag any with duplicated reviews across businesses."""
        users = self.handler.get_all_users()

        flagged_users = []

        for user_id in users:
            if self.check_user_for_duplicate_reviews(user_id):
                flagged_users.append(user_id)

        return flagged_users

    def check_user_for_duplicate_reviews(self, user_id):
        """Check if a specific user has posted highly similar reviews for different businesses."""
        relationship_list = self.handler.fetch_user_reviews(user_id)

        if not relationship_list or len(relationship_list) < 2:
            return False

        reviews = [rel['text'] for rel in relationship_list]
        businesses = [rel['business_id'] for rel in relationship_list]

        review_business_pairs = list(zip(reviews, businesses))

        for i in range(len(review_business_pairs)):
            for j in range(i + 1, len(review_business_pairs)):
                review_i, business_i = review_business_pairs[i]
                review_j, business_j = review_business_pairs[j]

                if business_i != business_j:
                    emb_i = self.model.encode(review_i, convert_to_tensor=True)
                    emb_j = self.model.encode(review_j, convert_to_tensor=True)
                    sim_score = util.cos_sim(emb_i, emb_j).item()

                    if sim_score >= self.similarity_threshold:
                        print(f"[FLAGGED] User {user_id} wrote similar reviews for different businesses")
                        print(f"Business A: {business_i}, Business B: {business_j}")
                        print(f"Review A: {review_i}")
                        print(f"Review B: {review_j}")
                        print(f"Similarity: {sim_score:.2f}\n")
                        return True  # Flagged

        return False


if __name__ == "__main__":
    detector = DuplicateReviewDetector()
    flagged = detector.check_all_users_for_duplicate_reviews()

    print("Flagged Users with Suspicious Review Patterns:")
    for user in flagged:
        print(user)
