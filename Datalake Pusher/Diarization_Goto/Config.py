import configparser
import ast

class Configuration():
    def __init__(self, filepath):
        #Read the configuration filepath
        self.config = configparser.ConfigParser()
        self.config.read(filepath)

    def loadConfiguration(self):
        #Read all the paths  and parameters from paths section
        #Make these as the attributes of config object
        paths = self.config.sections()[0]
        self.upload_folder = self.config[paths]['Upload Folder']
        self.configuration_path = self.config[paths]['Configuration']
        self.music_model_path = self.config[paths]['Music Model']
        self.ASRModel = self.config[paths]['ASR Model']
        self.LanguageModel = self.config[paths]['Language Model']
        self.dest_dir = self.config[paths]['Dest Dir']
        self.base = self.config[paths]['Base']
        self.dev_base = self.config[paths]['Dev Base']
        self.ivr_model_path = self.config[paths]['IVR Model Path']

        #new
        self.voip_data_path = self.config[paths]['VOIP Data Path']

        parameters = self.config.sections()[1]
        self.No_of_speakers = int(self.config[parameters]['No of speakers'])
        self.time_window = int(self.config[parameters]['Time window'])
        self.No_of_windows = int(self.config[parameters]['No of windows'])
        self.Window_size = int(self.config[parameters]['Window size'])

        mongo = self.config.sections()[2]
        self.mongo_url = self.config[mongo]['Mongo URL']
        self.db_name = self.config[mongo]['DB Name']
        self.collection_name = self.config[mongo]['Collection Name']