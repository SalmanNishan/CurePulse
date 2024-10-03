import os
import time
import shutil
from pydub.silence import split_on_silence
from pydub import AudioSegment
from scipy.io.wavfile import read, write
import soundfile as sf
from processes.Models.transcription import Transcription
from utils.supporter_functions import client_number_to_name_mapper

# Process 2
def runTranscription(config, file_obj, call_fetcher):
    '''
    Seperates audio into two parts, one corresponding to each speaker. 
    Also transcribes audio for each speaker.
    Input: Audio file
    Output: Agent and Client files, Transcriptions
    '''

    filename = file_obj.filename

    print('Running Transcription for: ', file_obj.filename)

    ## Get VOIP data for file

    if 'goto_' in filename:
        file_obj.client_name, file_obj.client_id, file_obj.agent_name, file_obj.agent_id, file_obj.call_timestamp, file_obj.call_type = call_fetcher.goto_mongo_data(file_obj.filename)
        if file_obj.client_name == None:
            id_client_name = client_number_to_name_mapper(file_obj.client_id)
            file_obj.client_name = id_client_name
            if id_client_name == "Unknown":
                id_client_name = 'Client'
        else:
            id_client_name = file_obj.client_name   
    else:
        file_obj.client_id, file_obj.agent_name, file_obj.call_timestamp, file_obj.call_type = call_fetcher.fetch_voip_data(file_obj.filename)
        id_client_name = client_number_to_name_mapper(file_obj.client_id)
        file_obj.client_name = id_client_name
        if id_client_name == "Unknown":
            id_client_name = 'Client'

    if 'goto_' in filename:
        files_dir = config.goto_base 
    else:
        files_dir = os.path.join(config.base, file_obj.required_date) 

    ## Initialize Transcription instance
    Transcript = Transcription("medium.en")
    start_time = time.time()

    ## identifying the agent and client file
    file_obj.agent_file =  os.path.join(files_dir, 'agent' + '_' + filename)
    file_obj.client_file = os.path.join(files_dir, 'client' + '_' + filename)

    ## Transcribe audio
    file_obj.transcriptions, file_obj.transcript_without_names, file_obj.paragraph_agent_transcription, file_obj.paragraph_client_transcription = Transcript.get_transcriptions(file_obj.agent_name, id_client_name, file_obj.audio_music_removed, file_obj.agent_audiofile_mono_path, file_obj.client_audiofile_mono_path)
    file_obj.agent_times, file_obj.client_times =Transcript.get_times()
    
    file_obj.transcription_execution_time = time.time() - start_time

    file_obj.zero_time = 0
    if (len(file_obj.agent_times) == 0) or (len(file_obj.client_times) == 0): 
        file_obj.zero_time = 1 
    
    ## Write two files, one for agent and one for client
    if ".wav" in filename:
        filename = filename.replace(".wav", "")

    file_obj.diarized_agent_path = "agent_" + filename + '.wav'
    file_obj.diarized_client_path = "client_" + filename + '.wav'

    sf.write(os.path.join(config.dest_dir, file_obj.diarized_agent_path), file_obj.agent_music_removed, samplerate=16000, format='WAV')
    sf.write(os.path.join(config.dest_dir, file_obj.diarized_client_path), file_obj.client_music_removed, samplerate=16000, format='WAV')

    file_obj.agent_duration = AudioSegment.from_file(file_obj.agent_file).duration_seconds
    file_obj.client_duration = AudioSegment.from_file(file_obj.client_file).duration_seconds

    print('Transcription completed for: ', file_obj.filename)
    
    return file_obj