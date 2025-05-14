from transformers import pipeline

sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

def get_sentiment_label(text):
    try:
        return sentiment_analyzer(str(text)[:512])[0]["label"]
    except:
        return "NEUTRAL"
    
def get_sentiment_score(text):
    try:
        res = sentiment_analyzer(str(text)[:512])[0]
        return res["score"] if res["label"] == "POSITIVE" else -res["score"]
    except:
        return 0.0
    

