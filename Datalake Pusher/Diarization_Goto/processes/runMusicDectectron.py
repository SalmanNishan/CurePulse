import os
import time
import librosa
import soundfile

from processes.Models.music_detector import MusicDetectron
# from processes.Models.utils import Utils
import tensorflow as tf

# Process 1    
def runMusicDectectron(config, file_obj, filename, required_date, storage_manager):
    '''
    Detects the time stamps for Music and performs music-speech seperation
    Processes one file at a time
    Input: Audio file
    Output: Speech file, Music file
    '''
    if not storage_manager.CheckRecordExists(filename): ## Only runs if record does not exist in MongodB 
        file_obj.required_date = required_date
        file_obj.filename = filename 
        file_obj.start_time = time.time()
        file_obj.holding_time = 0    ## Variable stores value for holding time  

        print('Running Music Detection for: ', filename)

        # audio_dir_pre = '/home/cmdadmin/Script/CallRecordings1' ## audio files directory
        # file_path_pre = os.path.join(audio_dir_pre, filename + '.wav')   ## path to file

        # audio_dir = '/home/cmdadmin/Script/CallRecordings' ## audio files directory
        # file_path = os.path.join(audio_dir, filename + '.wav')
        # command = f'sox {file_path_pre} -b 16 {file_path}'
        # os.system(command)

        audio_dir = os.path.join(config.base, required_date) ## audio files directory
        file_path = os.path.join(audio_dir, filename)
        
        audiofile_mono = "uploaded_file_mono.wav"   ## Temp audio file
        audiofile_mono_path = os.path.join(config.upload_folder, audiofile_mono)
        
        y, sr = librosa.load(file_path, sr=22050)   ## returns time series audio array and sample rate (sr)
        y_mono = librosa.to_mono(y)     ## Converts to single audio channel
        soundfile.write(audiofile_mono_path, y_mono, sr)    ## Write audio file in path

        file_obj.audiofile_mono_path = file_path
        ######### Detecting Music and Performing Music-Speech Separation################
        musicDetectron = MusicDetectron(config.music_model_path, file_obj.audiofile_mono_path, config.upload_folder)
        file_obj.start_time = time.time()
        musicDetectron.voiceActivityDetection()
        file_obj.vad_execution_time = time.time() - file_obj.start_time
        
        values_voiced, indices_voiced = musicDetectron.framesToSamples(musicDetectron.keep_frames)
        file_obj.voiceTimeStamps = musicDetectron.indicesToTime(indices_voiced)

        values_silence, indices_silence = musicDetectron.framesToSamples(musicDetectron.silence_frames)
        file_obj.silenceTimeStamps = musicDetectron.indicesToTime(indices_silence)

        musicDetectron.setSilenceHoldingTime(3,5) ## setting the silence time
        file_obj.holding_time += musicDetectron.silence_hold / 60 ## adding the silence hold time

        file_obj.start_time = time.time()
        musicDetectron.detectMusic()
        file_obj.music_execution_time = time.time() - file_obj.start_time

        musicDetectron.seperate_files_writer2(filename + '.wav') ## Write music and speech files

        file_obj.music_filepath = os.path.join(config.upload_folder, 'music_' +  filename + '.wav')      ## music file path
        file_obj.speech_filepath = os.path.join(config.upload_folder, 'speech_' + filename + '.wav')    ## speech file path

        print("Music Detection completed for: ", filename)
        return file_obj