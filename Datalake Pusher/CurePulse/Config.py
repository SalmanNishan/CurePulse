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
        self.speech_model_path = self.config[paths]['Speech Model']
        self.configuration_path = self.config[paths]['Configuration']
        self.music_model_path = self.config[paths]['Music Model']
        self.textModelPath = self.config[paths]['Text Model']
        self.LanguageModel = self.config[paths]['Language Model']
        self.LanguageVectorizer = self.config[paths]['Language Vectorizer']
        self.accent_model_path = self.config[paths]['Accent Model']
        self.transcriptions_dir = self.config[paths]['Transcriptions']
        self.agent_corpus = self.config[paths]['Agent Corpus']
        self.client_corpus = self.config[paths]['Client Corpus']
        self.dest_dir = self.config[paths]['Dest Dir']
        self.directory = self.config[paths]['Directory']
        self.base = self.config[paths]['Base']
        self.dev_base = self.config[paths]['Dev Base']
        self.goto_base = self.config[paths]['goto Base']
        self.cs_corpus_filepath = self.config[paths]['CS Corpus Filepath']
        self.teng_corpus_filepath = self.config[paths]['10g Corpus Filepath']
        self.callDataToMongoPhp_filepath = self.config[paths]['callDataToMongo PHP Filepath']
        self.diarization_main_filepath = self.config[paths]['diarization Main Filepath']
        self.speech_model_class_dict = self.config[paths]['speech model class dict']
        self.ivr_model_path = self.config[paths]['IVR Model Path']
        self.voip_data_path = self.config[paths]['VOIP Data Path']

        parameters = self.config.sections()[1]
        self.No_of_speakers = int(self.config[parameters]['No of speakers'])
        self.time_window = int(self.config[parameters]['Time window'])
        self.No_of_windows = int(self.config[parameters]['No of windows'])
        self.Window_size = int(self.config[parameters]['Window size'])
        self.labels = ast.literal_eval(self.config[parameters]['Labels'])
        self.accent_features = ast.literal_eval(self.config[parameters]["Accent Features"])
        self.interviews_penalties = ast.literal_eval(self.config[parameters]['interview_penalty_bins'])

        self.score_diff = float(self.config[parameters]["score_diff"])
        self.scoring_levels = int(self.config[parameters]["scoring_levels"])
        self.penalty_slope = float(self.config[parameters]["penalty_slope"])
        self.penalty_intercept = float(self.config[parameters]["penalty_intercept"])
        self.store_transcriptions = int(self.config[parameters]['Store Transcriptions'])

        agent_weights = self.config.sections()[2]
        self.Speech_weight_agent = float(self.config[agent_weights]['Speech Model Weight'])
        self.Text_weight_agent = float(self.config[agent_weights]['Text Model Weight'])
        self.Grammar_Weight_agent = float(self.config[agent_weights]['Grammar Model Weight'])
        self.Accent_Weight_agent = float(self.config[agent_weights]['Accent Model Weight'])
        self.Client_Weight_agent = float(self.config[agent_weights]['Client Descion Weight'])
        self.holding_weight_agent = float(self.config[agent_weights]['Music Average Weight'])
        self.Duration_weight_agent = float(self.config[agent_weights]['Music Duration Weight'])
        
        self.interview_tone_weight = float(self.config[agent_weights]['Interview Tone Weight'])
        self.interview_text_weight = float(self.config[agent_weights]['Interview Text Weight'])
        self.interview_accent_weight = float(self.config[agent_weights]['Interview Accent Weight'])
        self.interview_language_weight = float(self.config[agent_weights]['Interview Language Weight'])

        client_weights = self.config.sections()[3]
        self.Speech_weight_client = float(self.config[client_weights]['Speech Model Weight'])
        self.Text_weight_client = float(self.config[client_weights]['Text Model Weight'])
        self.holding_weight_client = float(self.config[client_weights]['Music Average Weight'])
        self.Duration_weight_client = float(self.config[client_weights]['Music Duration Weight'])

        voip_url = self.config.sections()[4]
        self.base_url = self.config[voip_url]['Base URL']
        self.url = self.config[voip_url]['URL']
        self.incoming = self.config[voip_url]['Incoming']
        self.outgoing = self.config[voip_url]['Outgoing']

        mongo = self.config.sections()[5]
        self.mongo_url = self.config[mongo]['Mongo URL']
        self.db_name = self.config[mongo]['DB Name']
        self.collection_name = self.config[mongo]['Collection Name']

        ms_teams = self.config.sections()[6]
        self.token_url = self.config[ms_teams]['Token URL']
        self.tokenHeader = eval(self.config[ms_teams]['Token Header'])
        self.msg_url = self.config[ms_teams]['Msg URL']

        scoring_thresholds = self.config.sections()[7]
        self.language_thresholds = eval(self.config[scoring_thresholds]['Language'])
        self.text_thresholds = eval(self.config[scoring_thresholds]['Text'])
        self.tone_thresholds = eval(self.config[scoring_thresholds]['Tone'])
        self.accent_thresholds = eval(self.config[scoring_thresholds]['Accent'])
        self.holdtime_thresholds = eval(self.config[scoring_thresholds]['Hold Time'])
        self.stars_sentiment_mapping = eval(self.config[scoring_thresholds]['Sentiment Stars Mapping'])
        self.accent_stars_mapping = eval(self.config[scoring_thresholds]['Accent Stars Mapping'])