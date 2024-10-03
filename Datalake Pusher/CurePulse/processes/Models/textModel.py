# - coding: utf-8 --
import numpy as np
from transformers import pipeline
import joblib

class TextClassifier():

    def __init__(self, model_path, labels_dict):
        self.sentiment_analysis = pipeline('sentiment-analysis', model=model_path)
        self.labels_dict = labels_dict  

    # # Define a function to preprocess and predict on a new string
    def predict_from_string(self, loaded_model, vectorizer_tfidf, input_string):
        input_tfIdf = vectorizer_tfidf.transform([input_string])
        predictions = loaded_model.predict(input_tfIdf)
        return predictions

    def predict_language_scores(self, model_path, tfidf_path, text):
        loaded_model = joblib.load(model_path)
        vectorizer_tfidf = joblib.load(tfidf_path)
        predicted_values = self.predict_from_string(loaded_model, vectorizer_tfidf,  text)[0]

        language_scores = {"Cohesion": predicted_values[0],
                            "Syntax": predicted_values[1],
                            "Vocabulary": predicted_values[2],
                            "Phraseology": predicted_values[3],
                            "Grammar": predicted_values[4],
                            "Conventions": predicted_values[5]}
        
        overall_language_score = language_scores['Cohesion']*0.2 + \
                                language_scores['Syntax']*0.1 + \
                                language_scores['Vocabulary']*0.2 + \
                                language_scores['Phraseology']*0.2 + \
                                language_scores['Grammar']*0.2 + \
                                language_scores['Conventions']*0.1
        
        language_score_percentage = (overall_language_score / 5) * 100

        return language_scores, overall_language_score, language_score_percentage
    
    def get_language_stars(self, score, thresholds):
        for stars, condition in thresholds.items():
            if eval(condition):
                return stars    
        return 3

    def create_sentiment_dict(self, sentiments_list):
        sentiments_dict = {0: [], 1: [], 2: []}
        for item in sentiments_list:
            key = item[0]['label']
            score = item[0]['score']
            key_idx = int(key.split("_")[-1])
            sentiments_dict[key_idx].append(score)
        sentiments_dict = {key: np.mean(values) if values else 0.0 for key, values in sentiments_dict.items()}
        emotions_dict = {
            "Positive": sentiments_dict[2],
            "Negative": sentiments_dict[0],
            "Neutral": sentiments_dict[1]
        }
        return emotions_dict

    def predict_sentiment(self, text_list):
        try:
            sentiments_list = []
            for sent in text_list:
                sentiments_list.append(self.sentiment_analysis(sent))
            emotions_dict = self.create_sentiment_dict(sentiments_list)
            return emotions_dict, sentiments_list 
        except:
            file_obj.textClientEmotionScore = {'Negative': 0, 'Neutral': 1, 'Positive': 0}
            file_obj.textclientstars = 3
            file_obj.text_sentiment_client = 'Neutral'
            file_obj.text_sentiments_list_client = []  
    
    def predict_stars_sentiment(self, sentiment_list, thresholds, mapping):
        LABELS = {'LABEL_0': 'Negative', 'LABEL_1': 'Neutral', 'LABEL_2':'Positive'}
        stars_sentiment_mapping = {5: "Positive", 4.5: "Positive", 4: "Positive", 3.5: "Neutral", 3: "Neutral", 2: "Negative", 1: "Negative"}
        label_counts = {label: 0 for label in LABELS.values()}
        for item in sentiment_list:
            key = item[0]['label']
            label = LABELS.get(key.upper(), None)
            if label:
                label_counts[label] += 1

        # Calculate the total length of list_of_dicts
        total_length = len(sentiment_list)
        scores = {'Negative': label_counts['Negative'] / total_length, 
                    'Neutral': label_counts['Neutral'] / total_length,
                    'Positive': label_counts['Positive'] / total_length}
        
        for star, condition in thresholds.items():
            if eval(condition):
                return star, mapping[star], label_counts, scores
        return 3, mapping[3], label_counts, scores