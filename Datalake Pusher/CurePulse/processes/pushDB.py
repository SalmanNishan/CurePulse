import os
import time
import soundfile as sf
from pymongo import MongoClient

from utils.supporter_functions import *

# Process 9
def pushDB(file_obj, storage_manager, config):
    '''
    Creates a record and pushed that record into MongodB
    Input: All Scores, execution times, meta data etc
    Output: Document record, data pushed to MongodB
    '''
    
    print('Pushing Data to dB for: ', file_obj.filename)

    ## creating the table representing the sentiments
    table = createTable(file_obj.emotion_agent, file_obj.emotion_client, file_obj.text_sentiment_agent, file_obj.text_sentiment_client, file_obj.agent_inference_score, file_obj.client_inference_score)
    
    ## originally time is in minutes so firstly converting it to seconds
    call_duration = time.strftime('%H:%M:%S', time.gmtime(file_obj.callDuration*60))

    reasons = getReasons(file_obj.agent_text_reason, file_obj.client_text_reason, file_obj.agent_tone_reason, file_obj.client_tone_reason)

    execution_time = time.time() - file_obj.start_time

    ## creating the document that will be sent ot MongoDB database
    document = createDocument(file_obj.filename, file_obj.required_date, file_obj.holding_time, file_obj.hold_time_stars, call_duration, file_obj.callDuration, execution_time, table, 
                file_obj.scores_agent_emotion, file_obj.agent_tone_stars, file_obj.scores_client_emotion, file_obj.client_tone_stars, file_obj.textagentstars, file_obj.textclientstars, file_obj.agent_accent_type, 
                file_obj.agent_inference_score, file_obj.client_inference_score, file_obj.textAgentEmotionScore, file_obj.textClientEmotionScore,
                file_obj.agent_accent_score, file_obj.agent_accent_stars, file_obj.agent_accent_language, 
                file_obj.language_stars, file_obj.language_scores, file_obj.overall_language_score, file_obj.language_score_percentage,
                file_obj.music_execution_time, file_obj.vad_execution_time, file_obj.transcription_execution_time, file_obj.tone_sentiment_execution_time, file_obj.text_sentiment_execution_time, 
                file_obj.grammar_execution_time, file_obj.tone_explaination_execution_time, file_obj.accent_execution_time, 
                file_obj.inference_execution_time, reasons, file_obj.diarized_agent_path, file_obj.diarized_client_path, file_obj.agent_reason_path, file_obj.client_reason_path, 
                file_obj.transcriptions, file_obj.cs_corpus, file_obj.engg_corpus, file_obj.engg_corpus_teams, file_obj.engg_teams_dict, file_obj.agent_duration, file_obj.client_duration, 
                file_obj.agent_name, file_obj.client_id, file_obj.client_name, file_obj.call_timestamp, file_obj.call_type, 
                file_obj.text_sentiments_list_agent, file_obj.text_sentiments_list_client, file_obj.text_sentiments_count_agent, file_obj.text_sentiments_count_client,
                file_obj.music_hold_time, file_obj.paragraph_agent_transcription, file_obj.paragraph_client_transcription)
    
    # Inserting document values in MongodB
    if file_obj.total_agent_duration < 30 or file_obj.total_client_duration < 30 or file_obj.total_duration < 60 or (file_obj.zero_time==1): 
        client = MongoClient(config.mongo_url)
        db = client[config.db_name]                          
        collection = db["CurePulse_Processed_Exception_Calls"]
        collection.insert_one(document)
    else:
        storage_manager.InsertRecord(document)
    
    client = MongoClient(config.mongo_url)
    db = client[config.db_name]                          
    collection = db["CurePulse_All_Calls"]
    collection.insert_one(document)

    sf.write(os.path.join('/ext01/CurePulse_Audio_Data/Original', file_obj.filename), file_obj.audio_music_removed, samplerate=16000, format='WAV')

    print("PushDB completd for: ", file_obj.filename)
    document["Zero_time"] = file_obj.zero_time
    document["total_agent_duration"] = file_obj.total_agent_duration
    document["total_client_duration"] = file_obj.total_client_duration
    document["total_duration"] = file_obj.total_duration
    return document