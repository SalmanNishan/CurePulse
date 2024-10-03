### Import Dependencies

import numpy as np
import ast, os
import pandas as pd
from datetime import date
import subprocess
from pathlib import Path
import json

# Path of CSVs
DF_PATH = '/home/cmdadmin/Data Ambient Intelligence/CSV Database/'

#___________________________document creation functons________________________________________#

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

def createDocument(filename, required_date, holding_time, holding_stars, call_duration, callDuration, execution_time, table, 
                scores_agent_emotion, agent_tone_stars, scores_client_emotion, client_tone_stars, textagentstars, textclientstars, agent_accent_type, 
                agent_inference_score, client_inference_score, textAgentEmotionScore, textClientEmotionScore, 
                agent_accent_score, agent_accent_stars, agent_accent_language, 
                languageStars, languageScores, langaugeOverallScore, langaugeScorePercentage,
                music_execution_time, vad_execution_time, transcription_execution_time, tone_sentiment_execution_time, 
                text_sentiment_execution_time, grammar_execution_time, tone_explaination_execution_time, 
                accent_execution_time, inference_execution_time, reasons, diarized_agent_path, diarized_client_path, agent_reason_path, 
                client_reason_path, transcriptions, cs_corpus, engg_corpus, engg_corpus_teams, engg_teams_dict, agent_duration, client_duration, 
                agent_name, client_id, client_name, call_timestamp, call_type, text_sentiments_list_agent, text_sentiments_list_client,
                text_sentiments_count_agent, text_sentiments_count_client, music_hold_time, paragraph_agent_transcription, paragraph_client_transcription):
                # textAgentEmotionScoreParagraph, textClientEmotionScoreParagraph
    '''
    Creates a document to be sent through MongodB
    '''
    textAgentEmotionScore = [float(num) for num in list(textAgentEmotionScore.values())]
    textClientEmotionScore = [float(num) for num in list(textClientEmotionScore.values())]

    toneAgentEmotionScore = scores_agent_emotion.tolist()
    toneClientEmotionScore = scores_client_emotion.tolist()

    agent_text_stars = textagentstars 
    client_text_stars = textclientstars


    document = {
        'Filename' : filename,
        'Date'     : required_date,
        'call_type': call_type,
        'Call_Duration' : call_duration,
        
        'Client_Tone_Sentiment': table['client_tone_sentiment'],
        'Client_Tone_Scores' : toneClientEmotionScore,
        'Client_Tone_Stars': int(np.floor(client_tone_stars)),
        'Agent_Tone_Sentiment': table['agent_tone_sentiment'],
        'Agent_Tone_Scores' : toneAgentEmotionScore,
        'Agent_Tone_Stars' : int(np.floor(agent_tone_stars)),

        'Client_Text_Sentiment' : table['client_text_sentiment'],
        'Client_Text_Scores' : textClientEmotionScore,
        'Client_Text_Stars': int(np.floor(client_text_stars)),

        'Agent_Text_Sentiment' : table['agent_text_sentiment'],
        'Agent_Text_Scores' : textAgentEmotionScore,
        'Agent_Text_Stars' : int(np.floor(agent_text_stars)),

        'Client_Infer_Scores' : round(client_inference_score*100),
        'Client_Infer_Stars' : finalStars(client_inference_score),
        'Agent_Infer_Scores' : round(agent_inference_score*100),
        'Agent_Infer_Stars' : finalStars(agent_inference_score),

        'Agent_Accent_Score' : agent_accent_score,
        'Agent_Accent_Stars' : int(np.floor(agent_accent_stars)),
        'Agent_Accent_Type' : agent_accent_type,
        'Agent_Accent_Language' : agent_accent_language,

        'Holding_Time' : holding_time,
        'Holding_Time_Stars': holding_stars,

        'Agent_Language_Stars' : int(np.floor(languageStars)),
        'Agent_Language_Scores' : languageScores,
        'Agent_Langauge_Overall_Score' : langaugeOverallScore,
        'Agent_Langauge_Score_Percentage' : langaugeScorePercentage,
        'Agent_Language_Performance' :  grammarPerformance(languageStars),

        'Agent_Duration' : agent_duration,
        'Client_Duration' : client_duration,

        'Execution Time' : execution_time,
        'Client_Final_Inference' : table['client_final_pred'],
        'Agent_Final_Inference' : table['agent_final_pred'],

        'Music_Execution_Time' : music_execution_time, 
        'VAD_Execution_Time'   : vad_execution_time,
        'Diarization_Execution_Time' : 'NULL',
        'Transcription_Execution_Time' : transcription_execution_time,
        'Tone_Execution_Time' : tone_sentiment_execution_time,
        'Text_Execution_Time' : text_sentiment_execution_time,
        'Punctuation_Execution_Time' : 0.00,
        'Grammar_Execution_Time' : grammar_execution_time,
        'Explaination_Execution_Time' : tone_explaination_execution_time, 
        'Accent_Execution_Time' : accent_execution_time,
        'Inference_Execution_Time' : inference_execution_time,

        'Client_Tone_Reason' : reasons['client_reason_tone'],
        'Client_Text_Reason' : reasons['client_reason_text'],
        'Agent_Tone_Reason' : reasons['agent_reason_tone'],
        'Agent_Text_Reason' : reasons['agent_reason_text'],

        'Agent_Diarized_Audio' : diarized_agent_path,
        'Client_Diarized_Audio' : diarized_client_path,
        'Agent_Reason_Audio' : agent_reason_path,
        'Client_Reason_Audio' : client_reason_path,

        'Transcription' : transcriptions, 
        'CS_Corpus' : cs_corpus,
        'Engineering_Corpus': engg_corpus,
        'Engineering_Corpus_Teams': engg_corpus_teams,
        'Engineering Specific Teams Corpus': engg_teams_dict,
        
        'client_id': client_id,
        'client_name': client_name,
        'agent_name': agent_name,
        'Managers': get_managers(agent_name),
        'Team_Type': get_team_type(agent_name),
        'Team Name': get_team_name(agent_name),
        'call_time_stamp': call_timestamp,
        
        'Agent_Text_Sentiments': text_sentiments_list_agent, 
        'Client_Text_Sentiments': text_sentiments_list_client,
        'Agent_Text_Sentiments_Count': text_sentiments_count_agent, 
        'Client_Text_Sentiments_Count': text_sentiments_count_client,
        'Music_Hold_Time': music_hold_time,

        'Client_Transcription' : paragraph_client_transcription,
        'Agent_Transcription' : paragraph_agent_transcription
    }

    return document

# Function to convert a full name to a username
def convert_to_username(full_name):
    first_name, last_name = full_name.split()
    username = f"{first_name.lower()}.{last_name.lower()}"
    return username

def get_managers(agent):
    username = convert_to_username(agent)

    hierarchy_file_path = '/home/cmdadmin/Datalake Pusher/config/agents_hierarchy.json' 
    with open(hierarchy_file_path, 'r') as json_file:
        hierarchy_data = json.load(json_file)

    if username in hierarchy_data:
        return ', '.join(hierarchy_data[username])
    else:
        return 'No Manager'

# Function to check which sheet the username belongs to
def get_team_type(agent):
    username = convert_to_username(agent)

    with open('/home/cmdadmin/Datalake Pusher/config/CS_Teams_Data_2.json', 'r') as f:
        data = json.load(f)

    for _, details in data.items():
        if username in details["members"]:
            return details["team_type"]
    return 'None'

def get_team_name(agent):
    username = convert_to_username(agent)

    with open('/home/cmdadmin/Datalake Pusher/config/CS_Teams_Data_2.json', 'r') as f:
        data = json.load(f)

    for department, details in data.items():
        if username in details["members"]:
            return department
    return 'None'

def finalStars(score):
    '''
    Scale inference upto stars
    '''
    return round(score / 0.2)

def grammarPerformance(stars):
    '''
    Rates grammar based on stars
    '''
    if stars > 3:
        return "Competent"
    elif stars == 3:
        return "Novice"
    else:
        return "Unacceptable"

def holdingTimeStars(holding_time, duration):
    '''
    Calculates holding time stars based on duration
    '''
    ratio = holding_time / duration
    if ratio > 0.8:
        return 1
    elif ratio > 0.6:
        return 2
    elif ratio > 0.4:
        return 3
    elif (holding_time <= 60):
        num_stars = 5
    elif holding_time > 60 and holding_time <= 120: 
        num_stars = 4
    elif holding_time > 120 and holding_time <= 180:
        num_stars = 3
    elif holding_time > 180 and holding_time <= 240: 
        num_stars = 2
    else:
        num_stars = 1

    return num_stars

def createTable(emotion_agent_tone, emotion_client_tone, text_sentiment_agent, text_sentiment_client, agent_inference_score, client_inference_score):
    '''
    Creates a dict like table with keys and values
    '''
    table = {}

    # storing the sentiment "Positive" or "Negative"
    table['agent_text_sentiment'] = text_sentiment_agent
    table['client_text_sentiment'] = text_sentiment_client

    # storing the sentiment "Positive", "Neutral" or "Negative"
    table['agent_tone_sentiment'] = emotion_agent_tone
    table['client_tone_sentiment'] = emotion_client_tone

    table['agent_final_pred'] = extractInferenceSentiment(agent_inference_score)
    table['client_final_pred'] = extractInferenceSentiment(client_inference_score)

    return table

def getReasons(agent_text_reason, client_text_reason, agent_tone_reason, client_tone_reason):
    '''
    Acquire explanibility from agent and client transciption txt files
    '''
    reasons = {}
        
    reasons['agent_reason_tone'] = agent_tone_reason
    reasons['agent_reason_text'] = agent_text_reason

    reasons['client_reason_tone'] = client_tone_reason
    reasons['client_reason_text'] = client_text_reason
    return reasons

def getScores(scores_agent_emotion, scores_client_emotion, textAgentEmotionScore, textClientEmotionScore, agent_inference_score, client_inference_score):
    '''
    Get tone, text and infer scores
    '''
    # assigning the max probability from predicted probability distribution
    tone_scores_agent = max(scores_agent_emotion)
    tone_scores_client = max(scores_client_emotion)
    tone_scores = {"Client" : tone_scores_client , "Agent" : tone_scores_agent}

    text_scores_agent = max(textAgentEmotionScore.values())
    text_scores_client = max(textClientEmotionScore.values())
    text_scores = { "Client" : text_scores_client , "Agent" : text_scores_agent}

    infer_scores = {"Client" : client_inference_score,
                    "Agent" : agent_inference_score}

    return (tone_scores, text_scores, infer_scores)


#___________________________dataframe creation functons________________________________________#

def day_time_func(col1, col2):
    '''
    Combines day and time columns to give daytime column
    '''
    col3 = col1 + ' ' + col2
    return col3

def username_generator(name):
    '''
    Converts Full Name into Usernaem 
    '''
    name_sep = name.split(' ')
    username = name_sep[0].lower() + '.' + name_sep[1].lower()
    return username

def id_generator(name, names_list):
    '''
    Generates IDs for CS Team
    '''
    if name == 'error.error':
        return 666

    x = names_list.index(name)
    return x+12

def id_generator2(name, names_list):
    '''
    Generates IDs for RCM
    '''
    x = names_list.index(name)
    return x+132

def id_generator3(name, names_list):
    '''
    Generates IDs for Finance Team
    '''
    x = names_list.index(name)
    return x+161

def userid_gen(username):
    '''
    Generates IDs according to full name 
    '''
    team = ''

    try:
        with open(DF_PATH + 'finance_agent_names.txt') as file:
                lines = file.readlines()
                agent_list = [line.rstrip() for line in lines]
        
        user_id = id_generator3(username, agent_list)
        team = 'Finance'

    except: 
        try:
            with open(DF_PATH + 'rcm_agent_names.txt') as file:
                    lines = file.readlines()
                    agent_list = [line.rstrip() for line in lines]
            
            user_id = id_generator2(username, agent_list)
            team = 'RCM'

        except:
            try:
                with open(DF_PATH + 'sales_agent_names.txt') as file:
                    lines = file.readlines()
                    agent_list = [line.rstrip() for line in lines]
                
                user_id = id_generator(username, agent_list)
                team = 'Sales'

            except:
                try:
                    with open(DF_PATH + 'agent_names.txt') as file:
                            lines = file.readlines()
                            agent_list = [line.rstrip() for line in lines]
                         
                    user_id = id_generator(username, agent_list)
                    team = 'CS'
                except:
                    with open(DF_PATH + 'indian_agent_names.txt') as file:
                        lines = file.readlines()
                        agent_list = [line.rstrip() for line in lines]

                        if username not in agent_list:
                            username = 'error.error'
                            return None, None
                                            
                    user_id = id_generator(username, agent_list)
                    team = 'India'

        
    return user_id, team

def transcription_fix(formatted_transcript):
    '''
    Merges same person dialougues
    '''
    new_transcripts = []
    formatted_transcript_list = formatted_transcript.split('<br>')
    
    new_transcripts.append(formatted_transcript_list[0])

    for line in formatted_transcript_list[1:]:
        
        if len(line) < 5:
            continue

        if line[0] == new_transcripts[-1][0]:
            new_transcripts[-1] = new_transcripts[-1] + ' ' + line.split(':')[1][1:]
        else:
            new_transcripts.append(line)

    return '<br>'.join(new_transcripts) + '<br>'

def transcript_conv(transcript):
    '''
    Cleans transcriptions
    '''
    new_transcript = ''
    for single_transcript in transcript:
        try:
            if len(single_transcript['Text']) < 2:
                continue
            new_transcript = new_transcript + '<b><span style="font-size: larger;">' + single_transcript['Speaker'] + '</span></b>' + ': ' + single_transcript['Text'] + '<br>'
        except:
            continue
    return new_transcript

def client_data(client_numbers):
    '''
    Extract and Fomat Client Numbers from CurePulse
    '''
    new_client_numbers = []
    for original_number in client_numbers:

        number = str(original_number)

        if len(number) == 10:
            new_client_numbers.append('+1' + number)
        elif len(number) == 11:
            new_client_numbers.append('+' + number)
        elif len(number) == 12:
            new_client_numbers.append(number)
        elif len(number) == 15:
            new_client_numbers.append(number[4:])
        else:
            new_client_numbers.append(number)
    
    return new_client_numbers

def dict_maker(numbers, names):
    '''
   Creates mapping dict
    '''
    mydict = {}
    for i in range(len(numbers)):
        mydict[numbers[i]] = names[i]
    return mydict

def client_number_to_name_mapper(original_number):
    '''
    Maps client numbers to names
    '''
    df = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/client_mappings_curepulse.csv')

    number = str(original_number)

    if len(number) == 10:
        new_number = '+1' + number
    elif len(number) == 11:
        new_number = '+' + number
    elif len(number) == 12:
        new_number = number
    elif len(number) == 15:
        new_number = number[4:]
    else:
        new_number = number
    
    client_numbers = client_data(df['Client Numbers'].values)
    client_names = df['Client Names'].values

    client_mappings = dict_maker(client_numbers, client_names)

    if new_number in client_mappings:
        return client_mappings[new_number]
    else:
        return 'Unknown'


def data_extractor(doc, day_of_week):
    '''
    Extract data from document
    '''
    
    username_single = username_generator(doc['agent_name'])

    with open('time_cal.txt', 'a') as f:
                f.write(doc['Call_Duration'])
                f.write('\n')

    user_id, team_name = userid_gen(username_single)
    ext_df = pd.read_excel("/home/cmdadmin/Datalake Pusher/config/CS_Names_Extensions.xlsx")
    ext_df["Name"] = ext_df["Name"].str.replace("  ", " ")
    ext_df["Name"] = ext_df["Name"].str.lower()
    ext_df["Name"] = ext_df["Name"].str.strip()
    try:
        ext = ext_df["Ext"][ext_df["Name"] == doc['agent_name'].lower().strip()].values[0]
    except:
        ext = 0

    if team_name == 'CS':

        document = {
            'Date': doc['Date'] + ' 00:00:01',
            'Time': day_time_func(doc['Date'], doc['call_time_stamp']),
            'Day_of_Week' : day_of_week,
            'File_Name': doc['Filename'],
            
            'Client_Name' : doc['client_name'],
            'Client_Infer_Score' : doc['Client_Infer_Scores'],
            'Client_Score': doc['Client_Infer_Stars'],
            'Client_Tone_Score': int(np.floor(doc['Client_Tone_Stars'])),
            'Client_Text_Score': int(np.floor(doc['Client_Text_Stars'])),
            'Client_Dur': doc['Client_Duration'],

            'Agent_Name': doc['agent_name'],
            'Username' : username_single,
            'UserID' : user_id,
            'Agent_Infer_Score': doc['Agent_Infer_Scores'],
            'Agent_Score': doc['Agent_Infer_Stars'],
            'Agent_Tone_Score': int(np.floor(doc['Agent_Tone_Stars'])),
            'Agent_Text_Score': int(np.floor(doc['Agent_Text_Stars'])),
            'Agent_Lang_Score': int(np.floor(doc['Agent_Language_Stars'])),
            'Agent_Acc_Score': int(np.floor(doc['Agent_Accent_Stars'])),
            'Agent_Dur': doc['Agent_Duration'],
            'Call_Dur' : doc['Call_Duration'],
            'Hold_Time': doc['Holding_Time_Stars'],

            'Call Type': doc['call_type'],
            'Team': team_name,
            'Transcript': transcript_conv(doc['Transcription']),
            'CS Corpus' : doc["CS_Corpus"],
            'Engineering Corpus' : doc["Engineering_Corpus"],
            'Engineering Corpus Teams' : ', '.join(doc["Engineering_Corpus_Teams"]),
            'Client_IDs': doc['client_id'],
            'Managers': doc['Managers'],
            'Team_Type': doc['Team_Type'],
            'team_name': doc["Team Name"],
            'Ext': ext
        }
    elif team_name == 'Sales':

        document = {
            'Date': doc['Date'] + ' 00:00:01',
            'Time': day_time_func(doc['Date'], doc['call_time_stamp']),
            'Day_of_Week' : day_of_week,
            'File_Name': doc['Filename'],
            
            'Client_Name' : doc['client_name'],
            'Client_Infer_Score' : doc['Client_Infer_Scores'],
            'Client_Score': doc['Client_Infer_Stars'],
            'Client_Tone_Score': int(np.floor(doc['Client_Tone_Stars'])),
            'Client_Text_Score': int(np.floor(doc['Client_Text_Stars'])),
            'Client_Dur': doc['Client_Duration'],

            'Agent_Name': doc['agent_name'],
            'Username' : username_single,
            'UserID' : user_id,
            'Agent_Infer_Score': doc['Agent_Infer_Scores'],
            'Agent_Score': doc['Agent_Infer_Stars'],
            'Agent_Tone_Score': int(np.floor(doc['Agent_Tone_Stars'])),
            'Agent_Text_Score': int(np.floor(doc['Agent_Text_Stars'])),
            'Agent_Lang_Score': int(np.floor(doc['Agent_Language_Stars'])),
            'Agent_Acc_Score': int(np.floor(doc['Agent_Accent_Stars'])),
            'Agent_Dur': doc['Agent_Duration'],
            'Call_Dur' : doc['Call_Duration'],
            'Hold_Time': doc['Holding_Time_Stars'],

            'Call Type': doc['call_type'],
            'Team': team_name,
            'Transcript': transcript_conv(doc['Transcription']),
            'Client_IDs': doc['client_id']
        }

    elif team_name == 'India':

        document = {
            'Date': doc['Date'] + ' 00:00:01',
            'Time': day_time_func(doc['Date'], doc['call_time_stamp']),
            'Day_of_Week' : day_of_week,
            'File_Name': doc['Filename'],
            
            'Client_Name' : doc['client_name'],
            'Client_Infer_Score' : doc['Client_Infer_Scores'],
            'Client_Score': doc['Client_Infer_Stars'],
            'Client_Tone_Score': int(np.floor(doc['Client_Tone_Stars'])),
            'Client_Text_Score': int(np.floor(doc['Client_Text_Stars'])),
            'Client_Dur': doc['Client_Duration'],

            'Agent_Name': doc['agent_name'],
            'Username' : username_single,
            'UserID' : user_id,
            'Agent_Infer_Score': doc['Agent_Infer_Scores'],
            'Agent_Score': doc['Agent_Infer_Stars'],
            'Agent_Tone_Score': int(np.floor(doc['Agent_Tone_Stars'])),
            'Agent_Text_Score': int(np.floor(doc['Agent_Text_Stars'])),
            'Agent_Lang_Score': int(np.floor(doc['Agent_Language_Stars'])),
            'Agent_Acc_Score': int(np.floor(doc['Agent_Accent_Stars'])),
            'Agent_Dur': doc['Agent_Duration'],
            'Call_Dur' : doc['Call_Duration'],
            'Hold_Time': doc['Holding_Time_Stars'],

            'Call Type': doc['call_type'],
            'Team': team_name,
            'Transcript': transcript_conv(doc['Transcription']),
            'Corpus_Alert' : doc["CS_Corpus"],
            'Client_IDs': doc['client_id']
        }
    
    else:

        document = {
            'Date': doc['Date'] + ' 00:00:01',
            'Time': day_time_func(doc['Date'], doc['call_time_stamp']),
            'Day_of_Week' : day_of_week,
            'File_Name': doc['Filename'],
            
            'Client_Name' : doc['client_id'],
            'Client_Infer_Score' : doc['Client_Infer_Scores'],
            'Client_Score': doc['Client_Infer_Stars'],
            'Client_Tone_Score': doc['Client_Tone_Stars'],
            'Client_Text_Score': doc['Client_Text_Stars'],
            'Client_Dur': doc['Client_Duration'],

            'Agent_Name': doc['agent_name'],
            'Username' : username_single,
            'UserID' : user_id,
            'Agent_Infer_Score': doc['Agent_Infer_Scores'],
            'Agent_Score': doc['Agent_Infer_Stars'],
            'Agent_Tone_Score': int(np.floor(doc['Agent_Tone_Stars'])),
            'Agent_Text_Score': int(np.floor(doc['Agent_Text_Stars'])),
            'Agent_Lang_Score': int(np.floor(doc['Agent_Language_Stars'])),
            'Agent_Acc_Score': int(np.floor(doc['Agent_Accent_Stars'])),
            'Agent_Dur': doc['Agent_Duration'],
            'Call_Dur' : doc['Call_Duration'],
            'Hold_Time': doc['Holding_Time_Stars'],

            'Call Type': doc['call_type'],
            'Team': team_name,
            'Transcript': transcript_conv(doc['Transcription'])
        }       

    return document

def df_cleaner(df):
    '''
    Removes any duplicare values in dataframe
    '''
    columns_to_remove = []
    for header in df.columns:
        if 'Unnamed' in header:
            columns_to_remove.append(header)
    df = df.drop_duplicates(subset = 'File_Name', keep='last')
    df = df.drop(columns_to_remove, axis = 1)
    return df

def df_cleaner2(df):
    # Find the columns that have "Unnamed" in the header
    unnamed_columns = [col for col in df.columns if 'Unnamed' in col]
    # Drop the unnamed columns from the DataFrame
    df = df.drop(columns=unnamed_columns)
    return df

def date_appender(some_date, n):
    date_list = some_date.split('-')
    yy, mm, dd = date_list[0], date_list[1], int(date_list[2]) + n
    return yy + '-' + mm + '-' + str(dd)
    
def engg_teams_dict_to_df(doc):

    document = {
            'Date': doc['Date'] + ' 00:00:01',
            'Time': day_time_func(doc['Date'], doc['call_time_stamp']),
            'File_Name': doc['Filename'],

            'Engineering Corpus' : doc["Engineering_Corpus"],
            'Engineering Corpus Teams' : ', '.join(doc["Engineering_Corpus_Teams"]),
        }
    for item in doc['Engineering Specific Teams Corpus']:
        document[item["Team"]] = item["Pattern"]
    
    return document

def teams_count(df):
    # sort dataframe by 'date' column in descending order
    df = df.sort_values('Date', ascending=False)

    # get latest date
    latest_date = df.iloc[0]['Date']

    # filter rows for latest date
    df = df[df['Date'] == latest_date]

    # create new dataframe with separated team names
    teams_count_df = pd.DataFrame()  # create empty dataframe
    teams_count_df['Date'] = df['Date'].copy()  # copy date column
    teams_count_df['Time'] = df['Time'].copy()  # copy time column
    teams_count_df['File_Name'] = df['File_Name'].copy()  # copy File_Name column
    teams_df = df['Engineering Corpus Teams'].str.split(', ', expand=True)  # split teams column into separate columns
    teams_count_df['Engineering Corpus Teams'] = teams_df.values.tolist()  # convert teams dataframe to list and assign to 'team' column
    teams_count_df = teams_count_df.explode('Engineering Corpus Teams').reset_index(drop=True)  # split rows with multiple teams into separate rows
    teams_count_df = teams_count_df[teams_count_df['Engineering Corpus Teams'].notnull()]

    try:
        columns_to_remove = []
        for header in teams_count_df.columns:
            if 'Unnamed' in header:
                columns_to_remove.append(header)
        teams_count_df = teams_count_df.drop(columns_to_remove, axis = 1)
    except:
        pass
    return teams_count_df
