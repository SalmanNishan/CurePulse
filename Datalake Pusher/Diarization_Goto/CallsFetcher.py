from pymongo import MongoClient
from datetime import datetime, date, timedelta
#new
import os, json
from VOIP import voip_data

class CallFetcher:
    def __init__(self, config, date):
        self.config = config
        # self.required_date_selector()
        self.required_date = date
        self.dow_gen()
        self.response_list = []

    def required_date_selector(self):
        '''
        Selects date depending on system time clock.
        If before 6:30 PM (PKT), it outputs yesterday's date as calls were processed yesterday
        If after 6:30 pm (PKT), it outputs today's date as calls have been or are currently being processed
        '''

        start_time_today = 64800
        end_time_today = 86400

        start_time_tomorrow = 0
        end_time_tomorrow = 64440

        current_time = datetime.now().strftime('%H-%M-%S').split('-')
        current_time_conv = int(current_time[0])*3600 + int(current_time[1])*60 + int(current_time[2])

        if start_time_today < current_time_conv < end_time_today:
            required_date = (date.today()).strftime('%Y-%m-%d')
        elif start_time_tomorrow < current_time_conv < end_time_tomorrow:
            required_date = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

        self.required_date = required_date

    def dow_gen(self):
        '''
        Takes a given date and finds the day of week for that date
        '''
        date_list = self.required_date.split('-')
        dow_int = date(year = int(date_list[0]), month = int(date_list[1]), day = int(date_list[2]))
        dow = dow_int.strftime("%A")
        self.day_of_week = dow

    
    def goto_mongo_data(self, filename):
        client = MongoClient(self.config.mongo_url)
        db = client['CurePulse']                          
        collection = db['GotoAPI']

        # Perform the aggregation pipeline
        pipeline = [
            {
                '$match': {
                    'recordingIds.0': filename.split("goto_")[-1]
                }
            },
            {
                '$project': {
                    'direction': '$direction',
                    'caller': {
                        '$cond': [
                            {'$eq': ['$direction', 'OUTBOUND']},
                            '$caller.name',
                            '$caller.number'
                        ]
                    },
                    'callee': {
                        '$cond': [
                            {'$eq': ['$direction', 'OUTBOUND']},
                            '$callee.number',
                            '$callee.name'
                        ]
                    },
                    'startTime': '$startTime'
                }
            }
        ]

        result = list(collection.aggregate(pipeline))

        # Extract the values into a list
        call_type = result[0]['direction']
        if call_type == 'OUTBOUND':
            call_type = 'outgoing'
            agent_name = result[0]['caller']
            client_name = result[0]['callee']
        else:
            call_type = 'incoming'
            agent_name = result[0]['callee']
            client_name = result[0]['caller']
        call_timestamp = result[0]['startTime'].split('T')[-1].split('.')[0]

        return client_name, agent_name, call_timestamp, call_type

    
    def process_goto_files(self):
        client = MongoClient(self.config.mongo_url)
        db = client['CurePulse']                          
        collection = db['GotoAPI']

        pipeline = [
            {
                '$match': {
                    'startTime':{
                        '$regex': '^' + self.required_date
                    }
                }
            },
            {
                '$project': {
                    'recordingIds':{
                        '$arrayElemAt': ['$recordingIds', 0]
                    }
                }
            }
        ]
        result = list(collection.aggregate(pipeline))
        filenames = []
        for item in result:
            try:
                filenames.append(f"goto_{item['recordingIds']}")
            except:
                pass
        return filenames
    

    #new
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