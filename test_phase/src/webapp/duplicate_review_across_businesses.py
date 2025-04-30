from sentence_transformers import SentenceTransformer, util
from neo4j_db_operations import Neo4jHandler
from neo4j import GraphDatabase

class DuplicateReviewDetector:

    def __init__(self, similarity_threshold=0.85):
        self.similarity_threshold = similarity_threshold
        self.model = SentenceTransformer("paraphrase-MiniLM-L6-v2")
        self.uri = "bolt://localhost:7687"   # Neo4j server URI
        self.user = "neo4j"                  # Username
        self.password = "12345678"
        self.handler = Neo4jHandler(self.uri, self.user, self.password)
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    # def check_all_users_for_duplicate_reviews(self):
    #     """Check all users and flag any with duplicated reviews across businesses."""
    #     users = self.handler.get_all_users()

    #     flagged_users = []

    #     for user_id in users:
    #         if self.check_user_for_duplicate_reviews(user_id):
    #             flagged_users.append(user_id)

    #     return flagged_users

    def _execute_fetch_relationships(self, tx, query, **params):
        """
        Execute the relationship fetching query within a transaction.
        """
        result = tx.run(query, **params)
        relationships = [record["r"] for record in result]
        return relationships
    
    def fetch_relationships(self, user_id):
        query = """
        MATCH (u:User {user_id: $user_id})-[r:REVIEWED]->(b:Business)
        RETURN r
        """
        params = {"user_id": user_id}
        with self.driver.session() as session:
            return session.execute_write(lambda tx: self._execute_fetch_relationships(tx, query, **params))

    def check_user_for_duplicate_reviews(self, user_id, pct=0.9):
        """Check if a specific user has posted highly similar reviews for different businesses."""

        relationship_list = self.fetch_relationships(user_id)

        print(f"relationships:\n {relationship_list}")

        if not relationship_list or len(relationship_list) < 2:
            return False

        reviews = [rel['text'] for rel in relationship_list]
        print(reviews)
        businesses = [rel['business_id'] for rel in relationship_list]
        print(businesses)
        ## NEED THE BUSINESS ID OF ALL THE BUSINESSES --- FIX
        review_business_pairs = list(zip(reviews, businesses))
        counter = 0
        for i in range(len(review_business_pairs)):
            for j in range(i + 1, len(review_business_pairs)):
                review_i, business_i = review_business_pairs[i]
                review_j, business_j = review_business_pairs[j]

                if business_i != business_j:
                    emb_i = self.model.encode(review_i, convert_to_tensor=True)
                    emb_j = self.model.encode(review_j, convert_to_tensor=True)
                    sim_score = util.cos_sim(emb_i, emb_j).item()
                    
                    if sim_score >= self.similarity_threshold:
                        counter += 1

        print(counter / len(review_business_pairs))

        return counter / len(review_business_pairs) >= pct

if __name__ == "__main__":
    detector = DuplicateReviewDetector()
    user_id = "69d33a53a5e5d6d70cb163"
    is_spam = detector.check_user_for_duplicate_reviews(user_id, pct=0.9)
    print(is_spam)
