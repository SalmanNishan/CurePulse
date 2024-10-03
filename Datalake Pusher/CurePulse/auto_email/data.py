from pymongo import MongoClient
import pandas as pd
from datetime import date, timedelta
import numpy as np
import os
from sqlalchemy import create_engine
import json
import copy

class CurePulseData:
    def __init__(self, url, db, collection, dept):
        self.threshold = 60
        client = MongoClient(url)
        self.mydb = client[db]
        self.mycollection = self.mydb[collection]
        self.dept = dept
        self.date = str(date.today() - timedelta(days=1))
        # self.date = '2024-03-14'
        datapoints= list(self.mycollection.find({ "Date": self.date}))
        self.df = pd.DataFrame(datapoints)
        conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
        db = create_engine(conn_string)
        self.conn = db.connect()
        agent_files_path = '/home/cmdadmin/Data Ambient Intelligence/CSV Database'
        self.voip_data_path = "/home/cmdadmin/Datalake Pusher/CurePulse/VOIP_data"
        if dept == 'CS':
            self.inc = "$in"
            self.db = 'public."CurePulseData"'
            with open(os.path.join(agent_files_path, 'agent_names.txt'), 'r') as f:
                usernames = [name.replace('\n', '') for name in f.readlines()]
            self.agents = self.__get_agent_names(usernames)
        elif dept == "Sales":
            self.inc = "$in"
            self.db = 'public."SalesData"'
            with open(os.path.join(agent_files_path, 'sales_agent_names.txt'), 'r') as f:
                usernames = [name.replace('\n', '') for name in f.readlines()]
            self.agents = self.__get_agent_names(usernames)
        elif dept == "India":
            self.inc = "$in"
            self.db = 'public."IndianData"'
            with open(os.path.join(agent_files_path, 'indian_agent_names.txt'), 'r') as f:
                usernames = [name.replace('\n', '') for name in f.readlines()]
            self.agents = self.__get_agent_names(usernames)
        self.outgoing_dataframe = self.original_data('outgoing')
        self.incoming_dataframe = self.original_data('incoming')
        self.original_dataframe = pd.concat([self.outgoing_dataframe, self.incoming_dataframe], axis=0, ignore_index=True)

    def get_date(self):
        return self.date

    def get_all_data(self):
        self.all_data = self.mycollection.aggregate([
                                {
                                    "$match": {
                                    "Date": self.date,
                                    "agent_name": { self.inc: self.agents }
                                    }
                                },
                                {
                                    "$project": {
                                    "agent_name": 1, "Filename": 1, "Date": 1, "Holding_Time": 1, "Holding_Time_Stars": 1,
                                    "Call_Duration": 1, "Execution Time": 1, "Client_Tone_Sentiment": 1, "Client_Text_Sentiment": 1, "Client_Final_Inference": 1, 
                                    "Agent_Tone_Sentiment": 1, "Agent_Text_Sentiment": 1, "Agent_Final_Inference": 1, "Agent_Accent_Type": 1, 
                                    "Client_Tone_Stars": 1, "Client_Text_Stars": 1, "Client_Infer_Scores": 1, "Client_Infer_Stars": 1,
                                    "Agent_Tone_Stars": 1, "Agent_Text_Stars": 1, "Agent_Infer_Scores": 1, "Agent_Infer_Stars": 1,
                                    "Agent_Language_Stars": 1, "Agent_Language_Performance": 1, "Agent_Accent_Stars": 1, "Transcription": 1, "Agent_Duration": 1,
                                    "client_id": 1, "client_name":1, "call_type": 1}
                                }
                                ])
        
        
        self.all_data = pd.DataFrame(list(self.all_data))
        try:
            columns = self.all_data.columns.tolist()
            # Move the specified column to the first position
            columns = ['agent_name'] + [col for col in columns if col != 'agent_name']
            # Reorder the DataFrame columns
            self.all_data = self.all_data[columns]
            self.all_data = self.all_data.sort_values(by='agent_name', ascending=True)
        except:
            pass

        if len(self.all_data) > 0:
            for i in range(len(self.all_data)):
                self.all_data["Transcription"][i] = " ".join([d["Text"] for d in self.all_data["Transcription"][i]])
            len_data = len(self.all_data)
        else:
            self.all_data = ["Filename","Date","Holding_Time","Holding_Time_Stars","Call_Duration","Execution Time",
                        "Client_Tone_Sentiment","Client_Text_Sentiment","Client_Final_Inference",
                        "Agent_Tone_Sentiment","Agent_Text_Sentiment","Agent_Final_Inference","Agent_Accent_Type",
                        "Client_Tone_Stars","Client_Text_Stars","Client_Infer_Scores","Client_Infer_Stars",
                        "Agent_Tone_Stars","Agent_Text_Stars","Agent_Infer_Scores","Agent_Infer_Stars","Agent_Language",
                        "Agent_Language_Performance","Agent_Accent_Stars","Transcription","client_id","agent_name",
                        "call_type"]
            len_data = 0
        return self.all_data, len_data
    
    def get_total_calls(self):
        _, _len = self.get_all_data()
        total_calls = _len
        return total_calls
    
    def get_calls_made(self):
        client = MongoClient("mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1")
        db = client['CurePulse']                          
        collection = db['Calls_Count']

        result = collection.find_one({"Date": self.date})

        # Check if result is None to determine if data exists or not
        if result is not None:
            if self.dept == "CS":
                return result["CS Calls Count"]
            elif self.dept == "Sales":
                return result["Sales Calls Count"]
            elif self.dept == "India":
                if "Indian Calls Count" not in result.keys():
                    return 0
                return result["Indian Calls Count"]

        else: return 0
    
    def get_incoming_calls(self):
        incoming_calls =  self.mycollection.count_documents({"call_type": "incoming", "Date":self.date,
                                                             "agent_name": { self.inc: self.agents}})
        return incoming_calls
    
    def get_outgoing_calls(self):
        outgoing_calls =  self.mycollection.count_documents({"call_type": "outgoing", "Date":self.date,
                                                             "agent_name": { self.inc: self.agents}})
        return outgoing_calls
    
    def get_agent_score(self):
        agent_score = list(self.mycollection.find({ "Date": self.date, "agent_name": { self.inc: self.agents} }, 
                                                  { "Agent_Infer_Scores": 1, "_id": 0}))
        agent_score = [d["Agent_Infer_Scores"] for d in agent_score]
        agent_score = np.mean(agent_score)
        return f"{agent_score:.2f}%"
    
    def get_client_score(self):
        client_score = list(self.mycollection.find({ "Date": self.date, "agent_name": { self.inc: self.agents} }, 
                                                   { "Client_Infer_Scores": 1, "_id": 0}))
        client_score = [d["Client_Infer_Scores"] for d in client_score]
        client_score = np.mean(client_score)
        return f"{client_score:.2f}%"
    
    def get_top_5_agents(self):
        try:
            # Execute the SQL query
            query = f"""
                SELECT "Agent_Name", AVG("Agent_Infer_Score") AS avg_score
                FROM {self.db}
                WHERE "Date" = '{self.date + ' 00:00:01'}' AND "Agent_Dur" > 20
                GROUP BY "Agent_Name"
                HAVING AVG("Agent_Infer_Score") >= {self.threshold}
                ORDER BY avg_score DESC
                LIMIT 5
            """

            # Fetch all the results
            results = self.conn.execute(query)
            results = results.fetchall()
            top_5_agents = [t for t in results]

            # Sort the filtered results by average score
            top_5_agents = sorted(top_5_agents, key=lambda x: x[1], reverse=True)
            top_5_agents = [{'Agent Name': item[0], 'Score': float(item[1])} for item in top_5_agents]

        except:
            top_5_agents = None
        return top_5_agents
    
    def get_bottom_5_agents(self):
        try:
            # Execute the SQL query
            query = f"""
                SELECT "Agent_Name", AVG("Agent_Infer_Score") AS avg_score
                FROM {self.db}
                WHERE "Date" = '{self.date + ' 00:00:01'}' AND "Agent_Dur" > 20
                GROUP BY "Agent_Name"
                HAVING AVG("Agent_Infer_Score") < {self.threshold}
                ORDER BY avg_score ASC
                LIMIT 5
            """

            # Fetch all the results
            results = self.conn.execute(query)
            results = results.fetchall()
            bottom_5_agents = [t for t in results]

            # Sort the filtered results by average score
            bottom_5_agents = sorted(bottom_5_agents, key=lambda x: x[1])
            bottom_5_agents = [{'Agent Name': item[0], 'Score': float(item[1])} for item in bottom_5_agents]

        except:
            bottom_5_agents = None
        return bottom_5_agents
    
    def get_top_5_clients(self):
        try:
            # Execute the SQL query
            query = f"""
                SELECT "Client_Name", AVG("Client_Infer_Score") AS avg_score
                FROM {self.db}
                WHERE "Date" = '{self.date + ' 00:00:01'}' AND "Client_Dur" > 20
                GROUP BY "Client_Name"
                HAVING AVG("Client_Infer_Score") >= {self.threshold}
                ORDER BY avg_score DESC
                LIMIT 5
            """

            # Fetch all the results
            results = self.conn.execute(query)
            results = results.fetchall()
            top_5_clients = [t for t in results]

            # Sort the filtered results by average score
            top_5_clients = sorted(top_5_clients, key=lambda x: x[1], reverse=True)
            top_5_clients = [{'Client Name': item[0], 'Score': float(item[1])} for item in top_5_clients]
        except:
            top_5_clients = None
        return top_5_clients
    
    def get_bottom_5_clients(self):
        try:
            # Execute the SQL query
            query = f"""
                SELECT "Client_Name", AVG("Client_Infer_Score") AS avg_score
                FROM {self.db}
                WHERE "Date" = '{self.date + ' 00:00:01'}' AND "Client_Dur" > 20
                GROUP BY "Client_Name"
                HAVING AVG("Client_Infer_Score") < {self.threshold}
                ORDER BY avg_score ASC
                LIMIT 5
            """

            # Fetch all the results
            results = self.conn.execute(query)
            results = results.fetchall()
            bottom_5_clients = [t for t in results]

            # Sort the filtered results by average score
            bottom_5_clients = sorted(bottom_5_clients, key=lambda x: x[1])
            bottom_5_clients = [{'Client Name': item[0], 'Score': float(item[1])} for item in bottom_5_clients]

        except:
            bottom_5_clients = None
        return bottom_5_clients
    
    def total_calls_time(self):
        total_calls_time = list(self.mycollection.find({ "Date": self.date, "agent_name": { self.inc: self.agents} }, 
                                                       { "Execution Time": 1, "_id": 0}))
        total_calls_time = sum(d["Execution Time"] for d in total_calls_time)

        def _convertMillis(seconds):
            seconds = seconds % (24 * 3600)
            hour = seconds // 3600
            seconds %= 3600
            minutes = seconds // 60
            seconds %= 60
            
            return "%d:%02d:%02d" % (hour, minutes, seconds)
        
        total_calls_time = _convertMillis(total_calls_time)
        return total_calls_time
    
    def agent_positive_count(self):
        count = self.mycollection.count_documents({"Agent_Final_Inference": "Positive", "Date":self.date, 
                                                   "agent_name": { self.inc: self.agents}})
        return count
    
    def agent_negative_count(self):
        count = self.mycollection.count_documents({"Agent_Final_Inference": "Negative", "Date":self.date,
                                                   "agent_name": { self.inc: self.agents}})
        return count
    
    def agent_neutral_count(self):
        count = self.mycollection.count_documents({"Agent_Final_Inference": "Neutral", "Date":self.date,
                                                   "agent_name": { self.inc: self.agents}})
        return count
    
    def client_positive_count(self):
        count = self.mycollection.count_documents({"Client_Final_Inference": "Positive", "Date":self.date,
                                                   "agent_name": { self.inc: self.agents}})
        return count
    
    def client_negative_count(self):
        count = self.mycollection.count_documents({"Client_Final_Inference": "Negative", "Date":self.date,
                                                   "agent_name": { self.inc: self.agents}})
        return count
    
    def client_neutral_count(self):
        count = self.mycollection.count_documents({"Client_Final_Inference": "Neutral", "Date":self.date,
                                                   "agent_name": { self.inc: self.agents}})
        return count
    
    def get_unsatisfactory_clients(self):
        unsatisfactory_clients = self.mycollection.aggregate([
                        { "$match": {"$and": [{ "Date": self.date, "agent_name": { self.inc: self.agents}}, 
                                              {"Client_Duration":{ '$gt': 20}}, {"Client_Infer_Scores":{ '$lt': self.threshold}},]}},
                        { "$group": {
                            "_id": "$client_id",
                            "average_score": { "$avg": "$Client_Infer_Scores" }
                        }},
                        { "$sort": { "average_score": 1 } },
                        { "$limit": 5 },
                        { "$project": {
                            "_id": 0,
                            "k": "$_id",
                            "v": "$average_score"
                        }},
                        { "$group": {
                            "_id": "null",
                            "result": { "$push": { "k": "$k", "v": "$v" } }
                        }},
                        { "$project": {
                            "_id": 0,
                            "result": { "$arrayToObject": "$result" }
                        }}
                    ])
        try:
            unsatisfactory_clients = list(list(unsatisfactory_clients)[0].values())
        except:
            unsatisfactory_clients = None
        return unsatisfactory_clients
    
    def below_60_clients(self):
        try:
            # Execute the SQL query
            query = f"""
                SELECT "Client_Name", round(AVG("Client_Infer_Score"), 2) AS avg_score
                FROM {self.db}
                WHERE "Date" = '{self.date + ' 00:00:01'}' AND "Client_Dur" > 20
                GROUP BY "Client_Name"
                ORDER BY avg_score ASC
            """
        
            # Fetch all the results
            results = self.conn.execute(query)

            results = results.fetchall()

            # Filter the results with average score less than 60
            below_60_clients = [t for t in results if t[1] < self.threshold]

            # Sort the filtered results by average score
            below_60_clients = sorted(below_60_clients, key=lambda x: x[1])
            below_60_clients = [{'Client Name': item[0], 'Score': float(item[1])} for item in below_60_clients]

        except:
            below_60_clients = None
        return below_60_clients
    
    def below_60_agents(self, performance_table):
        try:
            below_60_agents = performance_table[(performance_table['Quality Score'] < 60) | (performance_table['Talk Time Score'] < 60)]
            below_60_agents = below_60_agents.drop(columns=['Talk Time (mins)'])
        except:
            below_60_agents = pd.DataFrame(None, columns=['Agent Name', 'Talk Time Score', 'Quality Score', 'Performance Score'])
        return below_60_agents

    
    def __get_agent_names(self, usernames):
        names = []
        for user in usernames:
            first_name, last_name = user.split('.')
            first_name = first_name.capitalize()
            last_name = last_name.capitalize()
            names.append(first_name + ' ' + last_name)
        return names
    
    def get_total_voip_calls_count(self, df):
        try:
            df = df[df["Agent_Name"].isin(self.agents)]

            calls_1_min = len(df[df["Call_Duration"] < 60])
            calls_1_2_min = len(df[(df["Call_Duration"] >= 60) & (df["Call_Duration"] < 120)])
            calls_2_3_min = len(df[(df["Call_Duration"] >= 120) & (df["Call_Duration"] < 180)])
            calls_3_4_min = len(df[(df["Call_Duration"] >= 180) & (df["Call_Duration"] < 240)])
            calls_4_5_min = len(df[(df["Call_Duration"] >= 240) & (df["Call_Duration"] < 300)])
            calls_5_10_min = len(df[(df["Call_Duration"] >= 300) & (df["Call_Duration"] < 600)])
            calls_10_15_min = len(df[(df["Call_Duration"] >= 600) & (df["Call_Duration"] < 900)])
            calls_15_min = len(df[df["Call_Duration"] > 900])

            return calls_1_min, calls_1_2_min, calls_2_3_min, calls_3_4_min, calls_4_5_min, calls_5_10_min, calls_10_15_min, calls_15_min
        except:
            return 0, 0, 0, 0, 0, 0, 0, 0
    
    def get_processed_calls_count(self, call_flag):
        try:
            cursor = self.mycollection.find({"Date": self.date, "call_type": call_flag, "agent_name": { self.inc: self.agents }}, {"Filename": 1, "Call_Duration": 1, "_id": 0})
            data_list = list(cursor)
            df_processed = pd.DataFrame(data_list)
            df_processed["Call_Duration"] = df_processed["Call_Duration"].apply(self.__time_to_seconds).astype(int)

            calls_1_min = len(df_processed[df_processed["Call_Duration"] < 60])
            calls_1_2_min = len(df_processed[(df_processed["Call_Duration"] >= 60) & (df_processed["Call_Duration"] < 120)])
            calls_2_3_min = len(df_processed[(df_processed["Call_Duration"] >= 120) & (df_processed["Call_Duration"] < 180)])
            calls_3_4_min = len(df_processed[(df_processed["Call_Duration"] >= 180) & (df_processed["Call_Duration"] < 240)])
            calls_4_5_min = len(df_processed[(df_processed["Call_Duration"] >= 240) & (df_processed["Call_Duration"] < 300)])
            calls_5_10_min = len(df_processed[(df_processed["Call_Duration"] >= 300) & (df_processed["Call_Duration"] < 600)])
            calls_10_15_min = len(df_processed[(df_processed["Call_Duration"] >= 600) & (df_processed["Call_Duration"] < 900)])
            calls_15_min = len(df_processed[df_processed["Call_Duration"] > 900])
            return calls_1_min, calls_1_2_min, calls_2_3_min, calls_3_4_min, calls_4_5_min, calls_5_10_min, calls_10_15_min, calls_15_min
        except:
            return 0, 0, 0, 0, 0, 0, 0, 0
    
    def __time_to_seconds(self, time_str):
        hours, minutes, seconds = map(int, time_str.split(":"))
        total_seconds = ((hours * 60 + minutes) * 60 + seconds)
        return total_seconds
    
    def __time_to_hr_min(self, sec):
        hours = int(sec // 3600)
        minutes = int((sec % 3600) // 60)
        seconds = int(sec % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def original_data(self, call_flag):
        try:
            if self.dept is not "India":
                call_list = []
                file_name = f"{self.voip_data_path}/{call_flag}_{self.date}.json"
                with open(file_name, 'r') as file:
                    data = json.load(file)
                for call in data['data']:
                    call_id = call["call_id"]
                    call_duration = call["talk_time"]
                    agent_name = call["agent_name"]

                    call_list.append({
                        "Filename": call_id + '.wav',
                        "Call_Duration": int(call_duration),
                        "Agent_Name": agent_name
                    })
                original_data = pd.DataFrame(call_list)
                original_data = original_data[original_data["Agent_Name"].isin(self.agents)]
            else:
                if call_flag == 'outgoing':
                    call_flag = "OUTBOUND"
                    collection = self.mydb["GotoAPI"]
                    cursor = collection.find({"startTime": {"$regex": self.date}, "direction": call_flag}, {'recordingIds': 1, "duration": 1, 'caller': 1,"_id": 0})
                    data_list = list(cursor)
                    original_data = pd.DataFrame(data_list)
                    original_data = original_data.rename(columns={"recordingIds": "Filename", "duration": "Call_Duration", "caller": "Agent_Name"})
                else:
                    call_flag = "INBOUND"
                    collection = self.mydb["GotoAPI"]
                    cursor = collection.find({"startTime": {"$regex": self.date}, "direction": call_flag}, {'recordingIds': 1, "duration": 1, 'callee': 1, "_id": 0})
                    data_list = list(cursor)
                    original_data = pd.DataFrame(data_list)
                    original_data = original_data.rename(columns={"recordingIds": "Filename", "duration": "Call_Duration", "callee": "Agent_Name"})

                original_data["Filename"] = original_data["Filename"].str[0]
                original_data["Call_Duration"] = original_data["Call_Duration"] / 1000
                original_data["Agent_Name"] = original_data["Agent_Name"].str["name"]
        except:
            original_data = pd.DataFrame([])
        return original_data
    
    def get_total_agents(self):
        try:
            return len(list(set(self.original_dataframe["Agent_Name"].tolist())))
        except:
            return 0
    
    def get_average_calls_per_agent(self, total_agents, total_calls):
        try:
            return int(total_calls/total_agents)
        except:
            return 0
    
    def average_talktime_per_agent(self, total_agents):
        try:
            average_duration_secs = (self.original_dataframe['Call_Duration'].sum())/total_agents
            return self.__time_to_hr_min(average_duration_secs)
        except:
            return "00:00:00"
        
    def get_agents_average_scores(self, filename):
        try:
            # Group by 'agent_name' and calculate the mean of 'scores'
            new_data = copy.deepcopy(self.all_data)
            
            average_scores = self.all_data.groupby('agent_name').agg({'Agent_Duration':'sum', 'Agent_Infer_Scores':'mean'}).reset_index()
            # Create a new DataFrame with 'agent_name' and 'average_scores'
            performance_df = average_scores.rename(columns={'Agent_Infer_Scores': 'Quality Score', 'agent_name': "Agent Name", 'Agent_Duration': 'Talk Time (mins)'})
            performance_df['Talk Time (mins)'] /= 60
            performance_df['Talk Time Score'] = performance_df.apply(lambda row: (row['Talk Time (mins)']*1.41) if row['Talk Time (mins)'] < 60 else 
                                                                     (85 if 60 < row['Talk Time (mins)'] <= 90 else (90 if 90 < row['Talk Time (mins)'] <= 120 else 
                                                                    (95 if 120 < row['Talk Time (mins)'] <= 150 else 100))), axis=1) 
            performance_df["Performance Score"] = np.sqrt(performance_df['Talk Time Score'] * performance_df['Quality Score'])

            numerical_cols = performance_df.select_dtypes(include=[float, int]).columns
            performance_df[numerical_cols] = performance_df[numerical_cols].apply(lambda x: round(x, 0)).astype(int)
            performance_df = performance_df.sort_values(by='Performance Score', ascending=False).reset_index(drop=True)
            performance_df = performance_df[['Agent Name', 'Talk Time (mins)', 'Talk Time Score', 'Quality Score', 'Performance Score']]
        except:
            performance_df = pd.DataFrame([])
        with pd.ExcelWriter(filename, mode='a', engine='openpyxl') as writer:
            performance_df.to_excel(writer, sheet_name='Performance Scores', index=False)

        return performance_df
            
    
if __name__ == '__main__':
    url = "mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1"
    db = 'CurePulse'
    collection = 'CurePulse_Processed_Calls'
    CURE_PULSE_DATA = CurePulseData(url, db, collection, 'CS')
    CURE_PULSE_DATA.get_all_data()
    CURE_PULSE_DATA.get_agents_average_scores("")