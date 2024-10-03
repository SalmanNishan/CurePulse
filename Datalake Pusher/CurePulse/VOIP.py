import json
import requests

class voip_data:
    def __init__(self, config):
        self.config = config
        self.client_name = None
        self.agent_name = None
        self.call_timestamp = None
        self.call_type = None

    def get_data_from_voip(self, response_list):
        refined_records = []

        for response in response_list:

            list_records = response['data'] # get list of all the records

            for record in list_records:
                refined_records.append([record['call_id'], record['src'], record['agent_name'], record['call_to'], record['call_date'], record['recording_file']])

        return refined_records

    def unique_id_fixer(self, data):
        return data[:-4]

    def time_parser(self, day_time):
        return day_time.split(' ')[1]

    def is_outbound(self, record_file):
        record_file_prefix = record_file.split('-')[0]
        if record_file_prefix == 'out':
            return True
        else:
            return False

    def tags_finder(self, voip_data_list, audio_file_name):

        data_dict = {} 
        
        recording_name = self.unique_id_fixer(audio_file_name)

        data_dict[recording_name] = {}
        
        for data in voip_data_list:
            if data[0] in data_dict:
                data[4]
                if self.is_outbound(data[5]):
                    data[4]
                    data_dict[data[0]]['call_type'] = 'outgoing'
                    data_dict[data[0]]['client_id'] = data[3]
                    data_dict[data[0]]['agent_name'] = data[2]
                    data_dict[data[0]]['call_time_stamp'] = self.time_parser(data[4])
                else:
                    data_dict[data[0]]['call_type'] = 'incoming'
                    data_dict[data[0]]['client_id'] = data[1]
                    data_dict[data[0]]['agent_name'] = data[2]
                    data_dict[data[0]]['call_time_stamp'] = self.time_parser(data[4])
        
                self.client_name = data_dict[recording_name]['client_id']
                self.agent_name = data_dict[recording_name]['agent_name']
                self.call_timestamp = data_dict[recording_name]['call_time_stamp']
                self.call_type = data_dict[recording_name]['call_type']

