import os
from pymongo import MongoClient

class StorageManager():
    def __init__(self, config):
        self.config = config
        self.client = MongoClient(self.config.mongo_url)
        self.db = self.client[self.config.db_name]                          
        self.collection = self.db[self.config.collection_name]

    def CheckRecordExists(self, filename, date = None):
        '''
        Checks if the record with given filename already exists in the database.
        If date is also passed, checks if the record with given filename exists for given date. 
        Returns True if the record exists
        '''
        if date == None:
            if self.collection.count_documents({'Filename' : filename}, limit = 1):
                return True
        else:
            if self.collection.count_documents({'Filename' : filename, 'Date' : date}, limit = 1):
                return True
        return False


    def CheckRecordExistByDate(self, date):
        '''
        Checks if any record for given date exists. 
        Returns True if any record exists for given date. 
        '''

        if self.collection.count_documents({'Date' : date}, limit = 1):
            return True
        return False

    def ShowRecord(self, call_file):
        try:
            return self.collection.find( { 'Filename': { '$eq': call_file} } )[0]
        except IndexError:
            return None

    def InsertRecord(self, report):
        if self.CheckRecordExists(report['Filename'], report['Date']) == False:
            self.collection.insert_one(report)

    def getRecordsByDate(self, date = None):
        #if no date is passed, find all records
        #return all columns except id
        if date == None:
            DateRecords = self.collection.find( { } , {'_id': 0}).sort('Client_Infer_Scores' , 1)  
            
        else:
            DateRecords = self.collection.find( { 'Date': { '$eq': date} } , 
                                                {'_id': 0,
                                                 'Filename':1,
                                                 'Date' : 1,
                                                 'Client_Infer_Scores':1,
                                                 'Agent_Infer_Scores':1
                                                 }).sort('Client_Infer_Scores' , 1)
        #convert all records into a list of dictionaries
        Records = []
        for record in DateRecords:
            Records.append(record)
        #if any record found, get the column names
        if len(Records) > 0:
            columns = list(Records[0].keys())
        else:
            columns = [] #No records, so no columns
        return columns, Records
        

    def getFieldMean(self, field, date = None):
        if date == None:
            cursor = self.collection.find( {}, {field : 1, '_id' : 0})
        else:
            cursor = self.collection.find( { 'Date': { '$eq': date} }, {field : 1, '_id' : 0})
        values_list = []
        for value in cursor:
            values_list.append(value[field])
        if len(values_list) > 0:
            mean = sum(values_list) / len(values_list)
            return mean
        return 0

    def getField(self, field, date=None):
        if date == None:
            cursor = self.collection.find( {}, {field : 1, '_id' : 0})
        else:
            cursor = self.collection.find( { 'Date': { '$eq': date} }, {field : 1, '_id' : 0})
        values_list = []
        print(date)
        for value in cursor:
            values_list.append(value[field])
        return values_list 

    def getFieldCounts(self, field, date=None):
        field_list = self.getField(field, date)
        keys = list(dict.fromkeys(field_list))
        count_dict = {}

        for key in keys:
            count_dict[key] = field_list.count(key)
        return count_dict

    def files_cleaner(self):
        '''
        Removes additional files created by overall system during file processing
        '''

        ## Specify Paths
        path1 = self.config.dev_base
        path2 = path1 + 'uploads/'
        path3 = path1 + 'Audio Segments/'

        ## Initialize phrase dicts
        phrase_1 = {}
        phrase_2 = {}
        
        ## Store phrases corresponding to files in specific paths
        ## Each file will have a counterpart, hence two phrases
        phrase_1[path1] = '_Agent_textmodel.txt'
        phrase_1[path2] = '_agent_'
        phrase_1[path2] = 'music_'
        phrase_1[path3] = 'audio_segment'

        phrase_2[path1] = '_Client_textmodel.txt'
        phrase_2[path2] = '_client_'
        phrase_2[path2] = 'speech_'

        path_list = [path1, path2, path3]

        ## For files in a path, if a filename in a given path corresponds 
        ## to a phrase keyed in the dict below, it will be deleted

        for path in path_list:
            filenames_list = os.listdir(path)
            for filename in filenames_list:
                if phrase_1[path] in filename or phrase_2[path] in filename:
                    os.remove(path + filename)
        
        print('Files Cleaned')
