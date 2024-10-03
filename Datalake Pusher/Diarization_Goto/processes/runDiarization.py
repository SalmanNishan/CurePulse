import os
import time

from processes.Models.Speaker_Diarization import SpeakerDiarization
from processes.Models.transcription import Transcription
import soundfile as sf
import librosa
import numpy as np
from pymongo import MongoClient

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

    # file_obj.client_name, file_obj.agent_name, file_obj.call_timestamp, file_obj.call_type = call_fetcher.goto_mongo_data(file_obj.filename)
    file_obj.client_id, file_obj.agent_name, file_obj.call_timestamp, file_obj.call_type = call_fetcher.fetch_voip_data(file_obj.filename)

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
    # asr_model, language_model = Transcript.load_model(config.ASRModel, config.LanguageModel)  ## Load language and ASR models
    
    ## Transcribe audio
    transcriptions = Transcript.getTranscription2(speaker_0_times, speaker_1_times, config.upload_folder, 
                                                        file_obj.speech_filepath, file_obj.silenceTimeStamps)
    file_obj.transcription_execution_time = time.time() - start_time

    ## identifying the agent and client file
    file_obj.agent_file, file_obj.client_file, file_obj.agent_times, file_obj.client_times, file_obj.transcriptions = speakerDiarization.speakerIdentification(filename, files_dir, speaker_0_path, speaker_1_path, 
                                                                                                                        transcriptions, filtered_speakers_timestamps,
                                                                                                                        file_obj.call_type, False)
    

    audio_file = os.path.join(os.path.join(config.base, call_fetcher.required_date), filename)
    audio, sr = librosa.load(audio_file, sr=None)
    # Create a dictionary to store speaker audio segments
    speaker_segments = {}

    # Iterate through each dictionary in the list
    for speaker_dict in file_obj.transcriptions:
        # Extract the speaker name and their corresponding times
        speaker = speaker_dict["Speaker"]
        times = speaker_dict["Time"]

        # Initialize the speaker's audio segment
        if speaker not in speaker_segments:
            speaker_segments[speaker] = np.zeros(int(len(audio)))

        # Extract the start and end times from the time string
        start_min, start_sec = map(int, times[0].split(':'))
        end_min, end_sec = map(int, times[1].split(':'))

        # Convert start and end times to milliseconds
        start_time_sec = start_min * 60 + start_sec
        end_time_sec = end_min * 60 + end_sec

        # Update the speaker's audio segment with the corresponding segment from the original audio
        start_frame = int(start_time_sec * sr)
        end_frame = int(end_time_sec * sr)

        speaker_segments[speaker][start_frame:end_frame] = audio[start_frame:end_frame]

    ## Write two files, one for agent and one for client
    if ".wav" in filename:
        filename = filename.replace(".wav", "")

    file_obj.diarized_agent_path = 'agent_' + filename + '.wav'
    file_obj.diarized_client_path = 'client_' + filename + '.wav'

    # Export speaker audio segments to separate WAV files
    for speaker, segment in speaker_segments.items():
        if speaker == "Agent":
            # Export the speaker's audio segment to a WAV file
            file_path = os.path.join(os.path.join(config.base, call_fetcher.required_date), file_obj.diarized_agent_path)
            sf.write(file_path, segment, 8000, 'PCM_16')

        else:
            file_path = os.path.join(os.path.join(config.base, call_fetcher.required_date), file_obj.diarized_client_path)
            sf.write(file_path, segment, 8000, 'PCM_16')

    client = MongoClient(config.mongo_url)
    mydb = client['CurePulse']
    mycollection = mydb['Diarized_Calls_GotoAPI']
    data = {
        "date": call_fetcher.required_date,
        "filename": filename+'.wav',

    }
    mycollection.insert_one(data)
    client.close()
    
    return file_obj