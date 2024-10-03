import os
import json
from VOIP import voip_data
from pymongo import MongoClient
from datetime import datetime, date, timedelta

class CallFetcher:
    def __init__(self, config):
        self.config = config
        self.mongo_conn()
        self.required_date_selector()
        # self.required_date = "2024-08-15"
        self.dow_gen()
        self.response_list = []

    def mongo_conn(self):
        '''
        Establish connection with MongodB
        Pass credentials through MongoClient
        Select the Verified 5 collection inside Sentiment Analysis database
        '''
        self.connection = MongoClient(self.config.mongo_url)
        self.mycollection = self.connection[self.config.db_name][self.config.collection_name]

    def required_date_selector(self):
        '''
        Selects date depending on system time clock.
        If before 5:30 PM (PKT), it outputs yesterday's date as calls were processed yesterday
        If after 5:30 pm (PKT), it outputs today's date as calls have been or are currently being processed
        '''
        current_time = datetime.now()
        cutoff_time = current_time.replace(hour=17, minute=30, second=0, microsecond=0)
        if current_time < cutoff_time:
            adjusted_date = current_time - timedelta(days=1)
        else:
            adjusted_date = current_time
        self.required_date = adjusted_date.strftime('%Y-%m-%d')

    def dow_gen(self):
        '''
        Takes a given date and finds the day of week for that date
        '''
        date_list = self.required_date.split('-')
        dow_int = date(year = int(date_list[0]), month = int(date_list[1]), day = int(date_list[2]))
        dow = dow_int.strftime("%A")
        self.day_of_week = dow
    
    def url_fixer(self, url):
        '''
        Converts URL to useable format in Linux Server
        '''
        link = url.split('/')[3]
        new_url = self.config.base_url + link
        return new_url
    
    def process_downloaded_files(self):
        base_path = self.config.base
        calls_path = base_path + "/" + self.required_date
        old_files = [x for x in os.listdir(calls_path) if "agent_" not in x and "client_" not in x]

        call_type_flags = ["incoming", "outgoing"]

        for call_type_flag in call_type_flags:
            with open(f'{self.config.voip_data_path}/{call_type_flag}_{self.required_date}.json') as file:
                response = json.load(file)
            self.response_list.append(response)

        filtered_files = []
        for data_dict in self.response_list:
            for data in data_dict["data"]:
                if (data["call_id"]+'.wav') in old_files:
                    filtered_files.append(data["call_id"]+'.wav')

        return filtered_files

    def fetch_voip_data(self, filename):
        voip = voip_data(self.config)
        voip_data_list = voip.get_data_from_voip(self.response_list)
        voip.tags_finder(voip_data_list, filename)
        return voip.client_name, voip.agent_name, voip.call_timestamp, voip.call_type
    
    def goto_mongo_data(self, filename):
        client = MongoClient(self.config.mongo_url)
        db = client['CurePulse']                          
        collection = db['GotoAPI']
        filename = filename.split('.wav')[0]

        # Perform the aggregation pipeline
        pipeline = [
            {
                '$match': {
                    'recordingIds.0': filename.split("goto_")[-1]
                }},
            {
                '$project': {
                    'direction': '$direction',
                    'callerName': '$caller.name',
                    'callerId': '$caller.number',
                    'calleeName': '$callee.name',
                    'calleeId': '$callee.number',
                    'startTime': '$startTime'
                }}]

        result = list(collection.aggregate(pipeline))
        # Extract the values into a list
        call_type = result[0]['direction']
        if call_type == 'OUTBOUND':
            call_type = 'outgoing'
            agent_name = result[0]['callerName']
            agent_id = result[0]['callerId']
            client_name = result[0]['calleeName']
            client_id = result[0]['calleeId']
        else:
            call_type = 'incoming'
            agent_name = result[0]['calleeName']
            agent_id = result[0]['calleeId']
            client_name = result[0]['callerName']
            client_id = result[0]['callerId']
        call_timestamp = result[0]['startTime'].split('T')[-1].split('.')[0]

        return client_name, client_id, agent_name, agent_id, call_timestamp, call_type

    
    def process_goto_files(self):
        client = MongoClient(self.config.mongo_url)
        db = client['CurePulse']                          
        collection = db['GotoAPI']

        pipeline = [
            {
                '$match': {
                    'startTime':{
                        '$regex': '^' + self.required_date
                    }}},
            {
                '$project': {
                    'recordingIds':{
                        '$arrayElemAt': ['$recordingIds', 0]
                    }}}]
        result = list(collection.aggregate(pipeline))
        filenames = []
        for item in result:
            try:
                filenames.append(f"goto_{item['recordingIds']}.wav")
            except:
                pass
        return filenames