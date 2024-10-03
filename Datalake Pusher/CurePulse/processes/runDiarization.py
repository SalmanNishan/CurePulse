import os
import time
import shutil

from pydub import AudioSegment
from processes.Models.Speaker_Diarization import SpeakerDiarization
from processes.Models.transcription import Transcription

# Process 2
def runDiarization(config, file_obj, call_fetcher):
    '''
    Seperates audio into two parts, one corresponding to each speaker. 
    Also transcribes audio for each speaker.
    Input: Audio file
    Output: Agent and Client files, Transcriptions
    '''

    filename = file_obj.filename

    ## Get VOIP data for file

    file_obj.client_name, file_obj.agent_name, file_obj.call_timestamp, file_obj.call_type = call_fetcher.fetch_voip_data(file_obj.filename)

    print('Running Diarization for: ', filename)

    ## Initialize Speaker Diarization instance
    speakerDiarization = SpeakerDiarization(file_obj.speech_filepath, 
                                            config.configuration_path, 
                                            os.getcwd(),
                                            config.time_window, 
                                            config.No_of_speakers)

    start_time = time.time()

    ## Output time stamps of each speaker
    speakers_timestamps = speakerDiarization.perfromDiarization()
    
    file_obj.diarization_execution_time = time.time() - start_time

    ## Remove duplicate timestamp values 
    filtered_speakers_timestamps=speakerDiarization.dict_cleaner(speakers_timestamps)

    ## Writing diarized files separately
    files_dir = speakerDiarization.seperate_files_writer2(filename, filtered_speakers_timestamps)

    ## getting the filenames of saved files
    speaker_0_path = os.path.join(files_dir, 'speaker_0' + '_' + filename)
    speaker_1_path = os.path.join(files_dir, 'speaker_1' + '_' + filename)
    speaker_0_times = filtered_speakers_timestamps['speaker_0']
    speaker_1_times = filtered_speakers_timestamps['speaker_1']

    ## Initialize Transcription instance
    Transcript = Transcription()
    start_time = time.time()
    asr_model, language_model = Transcript.load_model(config.ASRModel, config.LanguageModel)  ## Load language and ASR models
    
    ## Transcribe audio
    transcriptions = Transcript.getTranscription2(speaker_0_times, speaker_1_times, config.upload_folder, 
                                                        file_obj.speech_filepath, asr_model, language_model, file_obj.silenceTimeStamps)
    file_obj.transcription_execution_time = time.time() - start_time

    ## identifying the agent and client file
    file_obj.agent_file, file_obj.client_file, file_obj.agent_times, file_obj.client_times, file_obj.transcriptions = speakerDiarization.speakerIdentification(filename, files_dir, speaker_0_path, speaker_1_path, 
                                                                                                                        transcriptions, filtered_speakers_timestamps,
                                                                                                                        file_obj.call_type, False)
    
    ## Write two files, one for agent and one for client
    if ".wav" in filename:
        filename = filename.replace(".wav", "")

    file_obj.diarized_agent_path = filename + '_Agent.wav'
    file_obj.diarized_client_path = filename + '_Client.wav'

    shutil.copy(file_obj.agent_file, os.path.join(config.dest_dir, file_obj.diarized_agent_path) )
    shutil.copy(file_obj.client_file, os.path.join(config.dest_dir, file_obj.diarized_client_path) )

    file_obj.agent_duration = AudioSegment.from_file(file_obj.agent_file).duration_seconds
    file_obj.client_duration = AudioSegment.from_file(file_obj.client_file).duration_seconds
    print(file_obj.agent_file, file_obj.client_file)

    print('Diarization completed for: ', filename)
    
    return file_obj