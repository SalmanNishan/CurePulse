from processes.Models.speech_model import SpeechModel
from processes.Models.Inference import Inference
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from Config import Configuration

from processes.runAccentDetection import predict_stars
from processes.Models.textModel import TextClassifier

import os
import numpy as np
import json
from tqdm import tqdm

config_filepath = '/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini'
config = Configuration(config_filepath)
config.loadConfiguration()

conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
engine = create_engine(conn_string)
postgresconn = engine.connect()

client = MongoClient("mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1")
db = client['CurePulse'] 
collection = db['CurePulse_Processed_Calls']

def convert_to_username(full_name):
    first_name, last_name = full_name.split()
    username = f"{first_name.lower()}.{last_name.lower()}"
    return username

def get_managers(agent):
    username = convert_to_username(agent)

    client_facing_file_path = '/home/cmdadmin/Datalake Pusher/config/client_facing_agents_hierarchy.json' 
    with open(client_facing_file_path, 'r') as json_file:
        client_facing_data = json.load(json_file)

    non_client_facing_file_path = '/home/cmdadmin/Datalake Pusher/config/non_client_facing_agents_hierarchy.json' 
    with open(non_client_facing_file_path, 'r') as json_file:
        non_client_facing_data = json.load(json_file)

    if username in client_facing_data:
        return ', '.join(client_facing_data[username])
    elif username in non_client_facing_data:
        return ', '.join(non_client_facing_data[username])
    else:
        return 'No Manager'

text_classifier = TextClassifier("/home/cmdadmin/Datalake Pusher/CurePulse/models/text_model_bert_obaid", {0: 'Negative', 1: 'Neutral', 2:'Positive'})
speech = SpeechModel("/home/cmdadmin/Datalake Pusher/CurePulse/models/tone_model_bert_obaid", None, None)
def update_speech_stars(speaker, result):
    sentiment = result[f"{speaker}_Tone_Sentiment"]
    average_preds = result[f"{speaker}_Tone_Scores"]
    # print(average_preds)
    stars, sentiment = speech.predict_stars(sentiment, average_preds)
    return stars, average_preds

def update_accent_stars(result):
    text_lab = result["Agent_Accent_Language"]
    score = result["Agent_Accent_Score"]

    stars, _ = predict_stars(text_lab, score, config.accent_thresholds)
    return stars, score, text_lab

def update_language_stars(result):
    # scores = speech.predict_language_scores("/home/cmdadmin/Datalake Pusher/CurePulse/models/language_model.joblib", "/home/cmdadmin/Datalake Pusher/CurePulse/models/vectorizer_tfidf.joblib", result["Agent_Transcription"])
    stars = text_classifier.get_language_stars(result["Agent_Langauge_Score_Percentage"], {5: [75], 4.5: [72, 75], 4: [67, 72], 3.5: [62, 67], 3: [55, 62], 2: [50, 55], 1: [50]})
    return stars, result["Agent_Langauge_Score_Percentage"]

def update_text_stars(speaker, result):
    stars, _, _, emotion_scores= text_classifier.predict_stars_sentiment(result[f"{speaker}_Text_Sentiments"], {5: [0.5], 4.5: [0.35, 0.5], 4: [0.2, 0.35], 3.5: [0.2], 2: [0.1, 0.4], 1: [0.4]})
    return stars, emotion_scores

def update_scores(speaker, result, agent, filename):
    if 'goto_' in filename:
            audio_dir = config.goto_base
    else:
        audio_dir = os.path.join(config.base, '2023-10-10')
    # file_path = os.path.join(audio_dir, f"{speaker.lower()}_" + filename)

    # speech_stars = int(np.floor(update_speech_stars(speaker, result)))
    text_classifier = TextClassifier("/home/cmdadmin/Datalake Pusher/CurePulse/models/text_model_bert_obaid", {0: 'Negative', 1: 'Neutral', 2:'Positive'})
    textAgentEmotionScore, text_sentiments_list_agent = text_classifier.predict_sentiment(result[f"{speaker}_Transcription"].split('.'))
    textagentstars, text_sentiment_agent, text_sentiments_count_agent, textAgentEmotionScore = text_classifier.predict_stars_sentiment(text_sentiments_list_agent, config.text_thresholds)
    text_stars = int(np.floor(textagentstars))

    collection.update_one(
        {"Filename": result["Filename"]},
        {"$set": {f"{speaker}_Text_Sentiment": text_sentiment_agent,
                  f"{speaker}_Text_Scores": textAgentEmotionScore,
                  f"{speaker}_Text_Stars": text_stars,
                  f"{speaker}_Text_Sentiments": text_sentiments_list_agent,
                  f"{speaker}_Text_Sentiments_Count": text_sentiments_count_agent}}
    )

    # infer = Inference(speech_stars, result[f"{speaker}_Tone_Scores"], text_stars, file_path, result["Music_Hold_Time"], agent = agent)

    # if agent:
    #     accent_stars = int(np.floor(update_accent_stars(result)))
    #     total_score, _, _ = infer.InferenceEngine(config, result[f"{speaker}_Text_Sentiments_Count"], result["Holding_Time"], 
    #                                               result["Agent_Grammar_Mistakes"], accent_stars, result["Client_Infer_Scores"]/100)
    # else:
    #     total_score = infer.InferenceEngine(config, result[f"{speaker}_Text_Sentiments_Count"])

    # final_inference = extractInferenceSentiment(total_score)
    # infer_scores = round(total_score*100)
    # final_stars = finalStars(total_score)

    # collection.update_one(
    #     {"Filename": filename},
    #     {"$set": {f"{speaker}_Text_Scores": text_emotion}}
    # )

    # collection.update_one(
    #     {"Filename": filename},
    #     {"$set": {f"{speaker}_Final_Inference": final_inference, f"{speaker}_Infer_Scores": infer_scores, f"{speaker}_Infer_Stars": final_stars, f"{speaker}_Text_Scores": text_emotion,
    #               f"{speaker}_Text_Stars": text_stars}}
    # update_query = text(
    #     f"""
    #     UPDATE public."CurePulseData"
    #     SET "{speaker}_Infer_Score" = :infer_score
    #     WHERE "File_Name" = :filename
    #     """
    # )
    # postgresconn.execute(update_query, infer_score=round(total_score*100), filename=filename)

def update_managers(result, filename):
    managers = get_managers(result["agent_name"])
    print(managers)
    collection.update_one(
        {"Filename": filename},
        {"$set": {"Managers": managers}}
    )
    update_query = text(
        f"""
        UPDATE public."CurePulseData"
        SET "Managers" = :managers
        WHERE "File_Name" = :filename
        """
    )
    postgresconn.execute(update_query, managers=managers, filename=filename)
    
def extractInferenceSentiment(total_score):
    '''
    States sentiment based on score
    '''
    if total_score >= 0.70:
        return "Positive"
    elif total_score < 0.50:
        return "Negative"
    else:
        return "Neutral"
    
def finalStars(score):
    '''
    Scale inference upto stars
    '''
    return round(score / 0.2)

# calls_path = os.path.join(config.base, '2023-09-14')

# old_files = [x for x in os.listdir(calls_path) if "agent_" not in x and "client_" not in x]

result_files = collection.find({"Date": '2023-10-17'})

# for filename in old_files:
#     print(filename)
#     try:
#         result = collection.find_one({"Filename": filename})
#         if result is not None:
#             # update_speech_stars("Agent", result, filename)
#             # update_speech_stars("Client", result, filename)

#             update_scores("Agent", result, True, filename)
#             update_scores("Client", result, False, filename)

#             # update_managers(result, filename)
#     except:
#         pass

# stars_dict_language = {1: 0, 2: 0, 3: 0, 3.5: 0, 4: 0, 4.5: 0, 5: 0}
# stars_dict_text = {1: 0, 2: 0, 3: 0, 3.5: 0, 4: 0, 4.5: 0, 5: 0}
# stars_dict_tone = {1: 0, 2: 0, 3: 0, 3.5: 0, 4: 0, 4.5: 0, 5: 0}
# stars_dict_accent = {1: 0, 2: 0, 3: 0, 3.5: 0, 4: 0, 4.5: 0, 5: 0}
# data_list = []
# for res in tqdm(result_files):
#     # try:
#     result = collection.find_one({"Filename": res["Filename"]})
#     if result is not None:
#         # stars = update_speech_stars("Agent", result)
#         # try:
#             languagestars, languagescore = update_language_stars(result)
#             textstars, textscores = update_text_stars("Agent", result)
#             tonestars, tonescores = update_speech_stars("Agent", result)
#             accentstars, accentscore, accentlang = update_accent_stars(result)
#             stars_dict_language[languagestars] += 1
#             stars_dict_text[textstars] += 1
#             stars_dict_tone[tonestars] += 1
#             stars_dict_accent[accentstars] += 1 
#             data_list.append({
#                 "Filename": res["Filename"],
#                 "Language Scores": languagescore,
#                 "Language Stars": languagestars,
#                 "Text Scores": textscores,
#                 "Text Stars": textstars,
#                 "Tone Scores": tonescores,
#                 "Tone Stars": tonestars,
#                 "Accent Stars": accentstars,
#                 "Accent Score": accentscore,
#                 "Accent Language": accentlang
#             })
#             # print(stars)
#         # except:
#         #     pass
#             # update_speech_stars("Client", result, res["Filename"])
#         # try:
#             # update_scores("Agent", result, True, res["Filename"])
#         # except:
#         #     pass
#         # update_scores("Client", result, False, res["Filename"])

#             # update_managers(result, filename)
#     # except:
#     #     pass

# import pandas as pd
# df = pd.DataFrame(data_list)
# df.to_csv("metrics_results.csv", index=False)
# print(stars_dict_language)
# print(stars_dict_text)
# print(stars_dict_tone)
# print(stars_dict_accent)


# if __name__ == "__main__":
#     def get_team_name(agent):
#         # username = convert_to_username(agent)
#         teams_data_file = '/home/cmdadmin/Datalake Pusher/config/CS_Teams_Data.json' 
#         with open(teams_data_file, 'r') as json_file:
#             teams_data = json.load(json_file)
        
#         for key, value in teams_data.items():
#             if agent in teams_data[key]:
#                 return key
#         return "None"
#     conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
#     db = create_engine(conn_string)
#     postgresconn = db.connect()


#     result = postgresconn.execute('SELECT * FROM public."CurePulseData"')
#     agents_list = []
#     for row in result:
#         agent_username = row[11]
#         agents_list.append(agent_username)

#     agents_list = list(set(agents_list))

#     for agent in agents_list:
#         team_name = get_team_name(agent)
#         query = text(
#             f"""
#             UPDATE public."CurePulseData"
#             SET "team_name" = :team_name
#             WHERE "Username" = :agent_username
#             """
#         )
#         postgresconn.execute(query, team_name=team_name, agent_username=agent)


# if __name__ == "__main__":
#     config_filepath = '/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini'
#     config = Configuration(config_filepath)
#     config.loadConfiguration()
#     from processes.Models.music_detector import MusicDetector
#     import soundfile as sf
#     from processes.Models.speech_model import SpeechModel
#     model = config.speech_model_path
#     # speechModelclient = SpeechModel(model, config.labels, config.speech_model_class_dict)
#     # speechModelagent = SpeechModel(model, config.labels, config.speech_model_class_dict)
#     music_detector = MusicDetector(config.ivr_model_path)
#     result_files = collection.find({"Date": '2023-10-17'})
#     client = MongoClient("mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1")
#     db = client['CurePulse'] 
#     collection = db['CurePulse_Processed_Calls']
#     filenames = []
#     for res in tqdm(result_files[100:]):
#         filenames.append((res["Filename"], res["Call_Duration"]))
#     # new_list = filenames

#     new_list = sorted(filenames, key=lambda x: x[1], reverse=False)
#     # print(new_list)
#     filenames = [filename for filename, duration in new_list]
#     for filename in tqdm(filenames):
#         res = collection.find_one({"Filename": filename})
#         if ('goto_' not in res["Filename"]) and (res["agent_name"] not in ["Kris Reed", "Tom Bennett", "Anthony Clark"]):
#             try:
#                 print(res["Filename"])
#                 audio_dir = os.path.join(config.base, "2023-10-17") ## audio files directory
#                 file_path = os.path.join(audio_dir, res["Filename"])   ## path to file
#                 agent_file_path = os.path.join(audio_dir, "agent_" + res["Filename"])   ## path to agent file
#                 client_file_path = os.path.join(audio_dir, "client_" + res["Filename"])

#                 audio_music_removed, speech_segments, audio_length = music_detector.remove_music(file_path)
#                 holding_time = (music_detector.get_holding_time(speech_segments.tolist(), audio_length))/60
#                 print(holding_time)
#                 hold_time_stars = 5
#                 total_hold_time = 0

#                 duration = res["Call_Duration"]
#                 print(duration)
#                 # 00:03:48
#                 thresholds = config.holdtime_thresholds
#                 if duration > "00:03:00":
#                     total_hold_time = float(holding_time)
#                     if total_hold_time <= thresholds[5][0]:
#                         hold_time_stars = 5
#                     elif thresholds[4][0] < total_hold_time <= thresholds[4][1]:
#                         hold_time_stars = 4
#                     elif thresholds[3][0] < total_hold_time <= thresholds[3][1]:
#                         hold_time_stars = 3
#                     elif thresholds[2][0] < total_hold_time < thresholds[2][1]:
#                         hold_time_stars = 2
#                     elif total_hold_time > thresholds[1][0]:
#                         hold_time_stars = 1
                # agent_music_removed, _, _ = music_detector.remove_music(agent_file_path)
                # client_music_removed, _, _ = music_detector.remove_music(client_file_path)

                # diarized_agent_path = "agent_" + res["Filename"]
                # diarized_client_path = "client_" + res["Filename"]
                # sf.write(os.path.join('/ext01/CurePulse_Audio_Data/Original', res["Filename"]), audio_music_removed, samplerate=16000, format='WAV')
                # sf.write(os.path.join(config.dest_dir, diarized_agent_path), agent_music_removed, samplerate=16000, format='WAV')
                # sf.write(os.path.join(config.dest_dir, diarized_client_path), client_music_removed, samplerate=16000, format='WAV')

                # print("Running Speech")
                # scores_agent_emotion, pred_probabilitites_agent = speechModelagent.predictSentiment(agent_music_removed, timestep = 4)
                # scores_client_emotion,  pred_probabilitites_client = speechModelclient.predictSentiment(client_music_removed, timestep = 4)

                # tone_sentiments_count_agent = speechModelagent.get_labels_count()
                # tone_sentiments_count_client = speechModelclient.get_labels_count()

                # emotion_agent = config.labels[np.argmax(scores_agent_emotion)]
                # emotion_client = config.labels[np.argmax(scores_client_emotion)]

                # agent_tone_stars, emotion_agent = speechModelagent.predict_stars(emotion_agent, scores_agent_emotion)
                # client_tone_stars, emotion_client = speechModelclient.predict_stars(emotion_client, scores_client_emotion)

                # toneAgentEmotionScore = scores_agent_emotion.tolist()
                # toneClientEmotionScore = scores_client_emotion.tolist()

                # collection.update_one(
                # {"Filename": res["Filename"]},
                # {"$set": {"Client_Tone_Sentiment": emotion_client, "Client_Tone_Scores": toneClientEmotionScore, "Client_Tone_Stars": int(np.floor(client_tone_stars)), 
                #         "Agent_Tone_Sentiment": emotion_agent, "Agent_Tone_Scores": toneAgentEmotionScore, "Agent_Tone_Stars": int(np.floor(agent_tone_stars))
                #         }})
                
            #     collection.update_one(
            #     {"Filename": res["Filename"]},
            #     {"$set": {"Holding_Time": total_hold_time, "Holding_Time_Stars": hold_time_stars}})
                
            #     conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
            #     db = create_engine(conn_string)
            #     postgresconn = db.connect()

            #     query = text(
            #         f"""
            #         UPDATE public."CurePulseData"
            #         SET "Hold_Time" = :hold_time_stars
            #         WHERE "File_Name" = :filename
            #         """
            #     )
            #     postgresconn.execute(query, hold_time_stars=hold_time_stars, filename=res["Filename"])
            # except:
            #     pass



# if __name__ == "__main__":
#     result_files = collection.find({"Date": '2023-10-02'})
#     conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
#     db = create_engine(conn_string)
#     postgresconn = db.connect()
#     for res in tqdm(result_files):
#         team = res["Team Name"]

#         query = text(
#             f"""
#             UPDATE public."CurePulseData"
#             SET "team_name" = :team
#             WHERE "File_Name" = :filename
#             """
#         )
#         postgresconn.execute(query, team=team, filename=res["Filename"])
