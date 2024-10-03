from processes.Models.corpus_check import CourpusChecker
from utils.supporter_functions import transcript_conv
from utils.supporter_functions import client_number_to_name_mapper, username_generator

def runCorpusChecker(config, file_obj, call_fetcher):
    '''
    Seperates audio into two parts, one corresponding to each speaker. 
    Also transcribes audio for each speaker.
    Input: Audio file
    Output: Agent and Client files, Transcriptions
    '''

    filename = file_obj.filename

    print('Running Corpus Checker for: ', filename)

    agent_text_stars = file_obj.textagentstars
    client_text_stars = file_obj.textclientstars

    try:
        courpuschecker = CourpusChecker(config.cs_corpus_filepath, config.teng_corpus_filepath)

        if 'goto_' in file_obj.filename:
            if file_obj.client_name == None:
                id_client_name = client_number_to_name_mapper(file_obj.client_id)
                if id_client_name == "Unknown":
                    id_client_name = 'Client'
            else:
                id_client_name = file_obj.client_name    
        else:
            id_client_name = client_number_to_name_mapper(file_obj.client_id)
            if id_client_name == "Unknown":
                id_client_name = 'Client'

        username = username_generator(file_obj.agent_name)

        email_ = False
        with open("/home/cmdadmin/Data Ambient Intelligence/CSV Database/agent_names.txt", 'r') as f:
            cs_agents = f.read()

        if username in cs_agents: email_ = True


        conv_transcription = transcript_conv(file_obj.transcriptions)

        if agent_text_stars < 3 or client_text_stars < 3:
            neg = courpuschecker.check_cs_corpus_sentiment(conv_transcription, 
                                                    filename, file_obj.required_date, file_obj.call_timestamp, file_obj.agent_name, id_client_name, email=email_)
            engg_corpus, engg_corpus_teams, teams_corpus_dict = courpuschecker.check_engineering_corpus(conv_transcription)
        else:
            neg = ''
            engg_corpus = ''
            engg_corpus_teams = []
            teams_corpus_dict = {}

        file_obj.cs_corpus = neg
        file_obj.engg_corpus = engg_corpus
        file_obj.engg_corpus_teams = engg_corpus_teams
        file_obj.engg_teams_dict = teams_corpus_dict

    except:
        file_obj.cs_corpus = ''
        file_obj.engg_corpus = ''
        file_obj.engg_corpus_teams = ''
        file_obj.engg_teams_dict = {}

    print('Corpus Checker completed for: ', filename)
    
    return file_obj