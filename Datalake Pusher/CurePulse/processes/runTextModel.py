import time
import tensorflow as tf
from processes.Models.textModel import TextClassifier

# Process 4
def runTextModel(config, file_obj, call_fetcher):
    
    '''
    Predicts Text Sentiment on transcriptions
    Input: Transcriptions
    Output: Text sentiment score
    '''

    ## Check if termination packet in args instance
    filename = file_obj.filename

    print('Running TextModel for: ', file_obj.filename)

    ## Segregate Agent and Client transcriptions
    file_obj.AgentTextList = []
    file_obj.ClientTextList = []
    for dict in file_obj.transcript_without_names:
        if dict['Speaker'] == 'Agent':
            file_obj.AgentTextList.extend(dict['Text'].split('.'))
        else:
            file_obj.ClientTextList.extend(dict['Text'].split('.'))

    file_obj.AgentTextList = [item for item in file_obj.AgentTextList if item != ""]
    file_obj.ClientTextList = [item for item in file_obj.ClientTextList if item != ""]

    ## Make predictions on Agent and Client text
    textModelClient = TextClassifier(config.textModelPath, config.labels)
    textModelAgent = TextClassifier(config.textModelPath, config.labels)

    start_time = time.time()

    try:
        file_obj.textAgentEmotionScore, file_obj.text_sentiments_list_agent = textModelAgent.predict_sentiment(file_obj.AgentTextList)
        file_obj.textagentstars, file_obj.text_sentiment_agent, file_obj.text_sentiments_count_agent, file_obj.textAgentEmotionScore = textModelAgent.predict_stars_sentiment(file_obj.text_sentiments_list_agent,
                                                                                                                                                                config.text_thresholds, config.stars_sentiment_mapping)
    except:
        file_obj.textAgentEmotionScore = {'Negative': 0, 'Neutral': 1, 'Positive': 0}
        file_obj.textagentstars = 3
        file_obj.text_sentiment_agent = 'Neutral'
        file_obj.text_sentiments_list_agent = []
        file_obj.text_sentiments_count_agent = {'Negative': 0, 'Neutral': len(file_obj.AgentTextList), 'Positive': 0}

    try:
        file_obj.textClientEmotionScore, file_obj.text_sentiments_list_client = textModelClient.predict_sentiment(file_obj.ClientTextList)
        file_obj.textclientstars, file_obj.text_sentiment_client, file_obj.text_sentiments_count_client, file_obj.textClientEmotionScore = textModelClient.predict_stars_sentiment(file_obj.text_sentiments_list_client,
                                                                                                                                                                config.text_thresholds, config.stars_sentiment_mapping)
    except:
        file_obj.textClientEmotionScore = {'Negative': 0, 'Neutral': 1, 'Positive': 0}
        file_obj.textclientstars = 3
        file_obj.text_sentiment_client = 'Neutral'
        file_obj.text_sentiments_list_client = []
        file_obj.text_sentiments_count_client = {'Negative': 0, 'Neutral': len(file_obj.ClientTextList), 'Positive': 0}

    print("Text Model Completed for: ", file_obj.filename)
    file_obj.text_sentiment_execution_time = time.time() - start_time
    
    start_time = time.time()

    ## Check Grammar
    agent_transcription = " ".join(file_obj.AgentTextList)
    file_obj.language_scores, file_obj.overall_language_score, file_obj.language_score_percentage = textModelAgent.predict_language_scores(config.LanguageModel, 
                                                                                                        config.LanguageVectorizer, agent_transcription)
    file_obj.language_stars = textModelAgent.get_language_stars(file_obj.language_score_percentage, config.language_thresholds)

    file_obj.grammar_execution_time = time.time() - start_time

    print('Language Model completed for: ', file_obj.filename)

    return file_obj