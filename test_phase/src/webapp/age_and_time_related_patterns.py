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

if __name__ == "__main__":

    review1 = "The toy store had a magical selection. My kids loved it!"  # appropriate
    review2 = "Had a wild night at this nightclub, the music and drinks were insane!"  # Inappropriate

    atp = AgeTimeRelatedPatterns()

    is_inappropriate = atp.authenticate_appropriateness_for_children(review2)

    print(is_inappropriate)
