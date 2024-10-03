from sqlalchemy import create_engine, text
import pandas as pd
from CallsFetcher import CallFetcher
from Config import Configuration
from utils.supporter_functions import *

from pymongo import MongoClient
import json
import time
import math
from tqdm import tqdm

def change_managers():
    with open("/home/cmdadmin/Datalake Pusher/config/pmo_cs_heirarchy.json") as f:
        cs_data = json.load(f)
    config_filepath = '/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini'
    config = Configuration(config_filepath)
    config.loadConfiguration()

    call_fetcher = CallFetcher(config)
    # call_fetcher.required_date = '2023-10-27'

    conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
    db = create_engine(conn_string)
    postgresconn = db.connect()

    query_db = f'SELECT "Agent_Name" FROM public."PMOData"'
    agent_names = []
    result = postgresconn.execute(query_db)
    for res in result:
        agent_names.append(res['Agent_Name'])
    agent_names = list(set(agent_names))
    for a_name in agent_names:
        manager = None
        team = None
        for teams, team_data in cs_data.items():
            for managers, members_list in team_data.items():
                # print(members_list)
                if a_name in members_list[0]:
                    manager = managers
                    team = teams
                    break
        query = text(
            f"""
            UPDATE public."PMOData"
            SET "Lead" = :lead,
            "team_name" = :team
            WHERE "Agent_Name" = :a_name
            """
        )
        postgresconn.execute(query, lead=manager, team=team, a_name=a_name)

def assign_manager(row):
    with open("/home/cmdadmin/Datalake Pusher/config/pmo_cs_heirarchy.json") as f:
        cs_data = json.load(f)
    manager = None
    team = None
    for teams, team_data in cs_data.items():
        for managers, members_list in team_data.items():
            # print(members_list)
            if row["Agent_Name"] in members_list[0]:
                manager = managers
                team = teams
                break
    return pd.Series({'Lead': manager, 'team_name':team})

def main():
    config_filepath = '/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini'
    config = Configuration(config_filepath)
    config.loadConfiguration()

    call_fetcher = CallFetcher(config)
    # call_fetcher.required_date = '2023-11-21'

    conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
    db = create_engine(conn_string)
    postgresconn = db.connect()


    query_db1 = f"""SELECT * FROM public."CurePulseData" WHERE "Date" = '{call_fetcher.required_date} 00:00:01'"""
    query_db2 = f"""SELECT * FROM public."CurePulseDataExceptions" WHERE "Date" = '{call_fetcher.required_date} 00:00:01'"""

    df_db1 = pd.read_sql(query_db1, con=db)
    df_db2 = pd.read_sql(query_db2, con=db)

    df_db1['Processed'] = "Processed"
    df_db2['Processed'] = "Unprocessed"

    # Combine the data into a single DataFrame
    combined_df = pd.concat([df_db1, df_db2], ignore_index=True)

    combined_df['Call_Dur_Sec'] = pd.to_timedelta(combined_df['Call_Dur']).dt.total_seconds()

    combined_df[['Lead', 'team_name']] = combined_df.apply(assign_manager, axis=1)

    combined_df["Incoming_Dur_Sec"] = combined_df['Call_Dur_Sec'].where(combined_df['Call Type'] == 'incoming', 0)
    combined_df["Outgoing_Dur_Sec"] = combined_df['Call_Dur_Sec'].where(combined_df['Call Type'] == 'outgoing', 0)

    combined_df['Talk_Time_Score'] = combined_df.apply(lambda row: ((row['Call_Dur_Sec']/60)*1.41) if (row['Call_Dur_Sec']/60) < 60 else (85 if 60 < (row['Call_Dur_Sec']/60) <= 90 
                                                                    else (90 if 90 < (row['Call_Dur_Sec']/60) <= 120 else (95 if 120 < (row['Call_Dur_Sec']/60) <= 150 else 100))), axis=1)

    combined_df["Performance_Score"] = combined_df.apply(lambda row: math.sqrt(row['Talk_Time_Score'] * row['Agent_Infer_Score']), axis=1)

    #Get Data from VOIP For Exception Calls
    call_type_flags = ["incoming", "outgoing"]
    outgoing = []
    incoming = []
    for call_type_flag in call_type_flags:
        with open(f'{config.voip_data_path}/{call_type_flag}_{call_fetcher.required_date}.json') as file:
            response = json.load(file)
        if call_type_flag == 'incoming':
            for i in range(len(response['data'])) :
                incoming.append(response['data'][i])
        else:
            for i in range(len(response['data'])) :
                outgoing.append(response['data'][i])
    try:
        url = "mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1"
        db = 'CurePulse'
        collection = 'Exception_Calls'

        client = MongoClient(url)
        mydb = client[db]
        mycollection = mydb[collection]

        results = mycollection.find({"Call Date": call_fetcher.required_date})
        all_voip_data = []
        for res in results:
            client_id, agent_name, call_timestamp, call_type = call_fetcher.fetch_voip_data(res["Filename"])
            if 'goto' not in res["Filename"] and agent_name not in ["Tom Bennet", 'Kris Reed', "Anthony Clark"]:
                id_client_name = client_number_to_name_mapper(client_id)
                try:
                    username = username_generator(agent_name)
                except:
                    username = ''
                try:
                    ext_df = pd.read_excel("/home/cmdadmin/Datalake Pusher/config/CS_Names_Extensions.xlsx")
                    ext_df["Name"] = ext_df["Name"].str.replace("  ", " ")
                    ext_df["Name"] = ext_df["Name"].str.lower()
                    ext_df["Name"] = ext_df["Name"].str.strip()
                    ext = ext_df["Ext"][ext_df["Name"] == agent_name.lower().strip()].values[0]
                except:
                    ext = 0

                for item in outgoing:
                    if item['call_id'] == res["Filename"].split('.wa')[0]:
                        duration = item['talk_time']
                        call_type = 'outgoing'
                for item in incoming:
                    if item['call_id'] == res["Filename"].split('.wa')[0]:
                        duration = item['talk_time']
                        call_type = 'incoming'
                try:
                    timestamp = day_time_func(call_fetcher.required_date, call_timestamp)
                except:
                    timestamp = call_fetcher.required_date + " 00:00:00"

                try:
                    managers = get_managers(agent_name)
                    team_type = get_team_type(agent_name),
                    team_name = get_team_name(agent_name),
                except:
                    managers = 'No Manager'
                    team_type = "None"
                    team_name = "None"

                all_voip_data.append({
                    'Date': call_fetcher.required_date + ' 00:00:01',
                    'Time': timestamp,
                    'Day_of_Week': call_fetcher.day_of_week,
                    'File_Name': res["Filename"],
                    'Client_Name': id_client_name,
                    'Client_Score': 3,
                    'Client_Infer_Score': 60,
                    'Client_Tone_Score': 3,
                    'Client_Text_Score': 3,
                    'Client_Dur': duration,
                    'Agent_Name': agent_name,
                    'Username': username,
                    'UserID': 666,
                    'Agent_Infer_Score': 60,
                    'Agent_Score': 3,
                    'Agent_Tone_Score': 3,
                    'Agent_Text_Score': 3,
                    'Agent_Lang_Score': 3,
                    'Agent_Acc_Score': 3,
                    'Agent_Dur': duration,
                    'Hold_Time': 5,
                    'Call_Dur': time.strftime('%H:%M:%S', time.gmtime(int(duration))),
                    'Call Type': call_type,
                    'Transcript': '',
                    'Client_IDs': client_id,
                    'CS Corpus': '',
                    'Engineering Corpus': '',
                    'Engineering Corpus Teams': '',
                    'Managers': managers,
                    'Team_Type': team_type,
                    'team_name': team_name,
                    'Ext': ext,
                    'Processed': 'Unprocessed',
                })

        voip_df = pd.DataFrame(all_voip_data) 

        voip_df['Call_Dur_Sec'] = pd.to_timedelta(voip_df['Call_Dur']).dt.total_seconds()

        voip_df[['Lead', 'team_name']] = voip_df.apply(assign_manager, axis=1)

        voip_df["Incoming_Dur_Sec"] = voip_df['Call_Dur_Sec'].where(voip_df['Call Type'] == 'incoming', 0)
        voip_df["Outgoing_Dur_Sec"] = voip_df['Call_Dur_Sec'].where(voip_df['Call Type'] == 'outgoing', 0)

        voip_df['Talk_Time_Score'] = voip_df.apply(lambda row: ((row['Call_Dur_Sec']/60)*1.41) if (row['Call_Dur_Sec']/60) < 60 else (85 if 60 < (row['Call_Dur_Sec']/60) <= 90 
                                                                    else (90 if 90 < (row['Call_Dur_Sec']/60) <= 120 else (95 if 120 < (row['Call_Dur_Sec']/60) <= 150 else 100))), axis=1)
        voip_df["Performance_Score"] = voip_df.apply(lambda row: math.sqrt(row['Talk_Time_Score'] * row['Agent_Infer_Score']), axis=1)
    except:
        voip_df = pd.DataFrame([])

    for index, row in voip_df.iterrows():
        exists = postgresconn.execute(
                """SELECT 1 FROM "PMOData" WHERE "File_Name" = %s and "Lead" = %s LIMIT 1""",
                (row['File_Name'], row["Lead"])
            ).fetchone() is not None
        if not exists:
            row.to_frame().transpose().to_sql('PMOData', postgresconn, if_exists = 'append', index = False)
    for index, row in combined_df.iterrows():
        exists = postgresconn.execute(
                """SELECT 1 FROM "PMOData" WHERE "File_Name" = %s and "Lead" = %s LIMIT 1""",
                (row['File_Name'], row["Lead"])
            ).fetchone() is not None
        if not exists:
            row.to_frame().transpose().to_sql('PMOData', postgresconn, if_exists = 'append', index = False)