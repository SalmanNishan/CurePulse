import os
import time
from processes.Models.music_detector import MusicDetector
import tensorflow as tf
from pymongo import MongoClient

# Process 1    
def runMusicDectectron(config, file_obj, filename, required_date, storage_manager):
    '''
    Detects the time stamps for Music and performs music-speech seperation
    Processes one file at a time
    Input: Audio file
    Output: Speech file, Music file
    '''
    client = MongoClient(config.mongo_url)
    db = client[config.db_name]                          
    collection = db["CurePulse_Processed_Exception_Calls"]
    exception_filename_exists = False
    if collection.count_documents({'Filename' : filename}, limit = 1):
        exception_filename_exists =  True
    
    if (not storage_manager.CheckRecordExists(filename)) and (not exception_filename_exists): ## Only runs if record does not exist in MongodB 
        
        file_obj.required_date = required_date
        file_obj.filename = filename 
        file_obj.start_time = time.time()
        file_obj.holding_time = 0    ## Variable stores value for holding time  

        print('Running Music Detection for: ', filename)

        if 'goto_' in filename:
            audio_dir = config.goto_base
        else:
            audio_dir = os.path.join(config.base, required_date) ## audio files directory
        file_path = os.path.join(audio_dir, filename)   ## path to file
        agent_file_path = os.path.join(audio_dir, "agent_" + filename)   ## path to agent file
        client_file_path = os.path.join(audio_dir, "client_" + filename)   ## path to client file

        file_obj.start_time = time.time()

        music_detector = MusicDetector(config.ivr_model_path)

        file_obj.audio_music_removed, speech_segments, audio_length = music_detector.remove_music(file_path, config.music_model_path)
        file_obj.agent_music_removed, _, file_obj.total_agent_duration = music_detector.remove_music(agent_file_path, config.music_model_path, agent=True)
        file_obj.client_music_removed, _, file_obj.total_client_duration = music_detector.remove_music(client_file_path, config.music_model_path)

        file_obj.holding_time = music_detector.get_holding_time(speech_segments.tolist(), audio_length)

        file_obj.music_execution_time = time.time() - file_obj.start_time

        file_obj.audiofile_mono_path = file_path
        file_obj.agent_audiofile_mono_path = agent_file_path
        file_obj.client_audiofile_mono_path = client_file_path
        
        file_obj.vad_execution_time = 0

        print('Music Detection completed for: ', filename)
    
        return file_obj