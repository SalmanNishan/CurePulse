import time
import numpy as np
import tensorflow as tf
import torch

# Process 3
def runSpeechModel(config, file_obj, call_fetcher):
    '''
    Predicts tone sentiment
    Input: Agent and Client files
    Output: Tone sentiment prediction stars
    '''
    print('Running SpeechModel for: ', file_obj.filename)
    from processes.Models.speech_model import SpeechModel
    model = config.speech_model_path
    speechModelclient = SpeechModel(model, config.labels, config.speech_model_class_dict)
    speechModelagent = SpeechModel(model, config.labels, config.speech_model_class_dict)

    # predicting the agent and client emotion
    # scores is the final average of all time-segment-sentiments
    # probailities is a list of sentiment probabilites for all segments
    # this is conceptual difference between scores and probabilities
    
    start_time = time.time()

    ## Predcit scores
    file_obj.scores_agent_emotion, file_obj.pred_probabilitites_agent = speechModelagent.predictSentiment(file_obj.agent_music_removed, timestep = 4)
    file_obj.scores_client_emotion, file_obj. pred_probabilitites_client = speechModelclient.predictSentiment(file_obj.client_music_removed, timestep = 4)
    file_obj.tone_sentiment_execution_time = time.time() - start_time

    file_obj.tone_sentiments_count_agent = speechModelagent.get_labels_count()
    file_obj.tone_sentiments_count_client = speechModelclient.get_labels_count()

    file_obj.emotion_agent = config.labels[np.argmax(file_obj.scores_agent_emotion)]
    file_obj.emotion_client = config.labels[np.argmax(file_obj.scores_client_emotion)]

    file_obj.agent_tone_stars, file_obj.emotion_agent = speechModelagent.predict_stars(file_obj.scores_agent_emotion, config.tone_thresholds, config.stars_sentiment_mapping)
    file_obj.client_tone_stars, file_obj.emotion_client = speechModelclient.predict_stars(file_obj.scores_client_emotion, config.tone_thresholds, config.stars_sentiment_mapping)

    print('SpeechModel completed for: ', file_obj.filename)

    return file_obj