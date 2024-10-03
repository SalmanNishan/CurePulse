from sqlite3 import Timestamp
# import nemo.collections.asr as nemo_asr
import os
import numpy as np
from pydub import AudioSegment
import bisect
import whisper
import configparser
from models.transcription_utils import transcriptionUtils
from processes.Models.transcription_utils import *
import tensorflow as tf
# from nemo.collections.nlp.models import PunctuationCapitalizationModel


#calls_asr = nemo_asr.models.ASRModel.restore_from(restore_path='models/new_checkpoint68.nemo')
class Transcription():
    def __init__(self):
        self.whisper_asr = whisper.load_model("base.en", device='cpu')
        pass
    
    def transcribe_whisper(self, file_paths):
        transcription = []
        for file_path in file_paths:
            result = self.whisper_asr.transcribe(file_path)
            transcription.append(result['text'])
        return transcription

    def load_model(self, asr_path, lm_path = None):
        #function to load the asr and langauge model
        with tf.device('/CPU:0'):
            asr_model = nemo_asr.models.EncDecCTCModelBPE.restore_from(asr_path)
            if lm_path == None:
                return asr_model, None
            beam_search_lm = nemo_asr.modules.BeamSearchDecoderWithLM(
                vocab=list(asr_model.decoder.vocabulary),
                beam_width=360,
                alpha=1, beta=0.5,
                lm_path=lm_path,
                num_cpus=max(os.cpu_count(), 1),
                input_tensor=False)
            return asr_model, beam_search_lm

    def softmax(self, logits):
        #apply softmax on logits to compute probabilities
        e = np.exp(logits - np.max(logits))
        return e / e.sum(axis=-1).reshape([logits.shape[0], 1])

    def transcribe(self, asr_model, file_path, beam_search_lm = None):
        transcriptions = []
        if type(file_path) is list:
            logits = asr_model.transcribe(paths2audio_files=file_path, logprobs=True)
        if beam_search_lm == None:
            transcriptions = asr_model.transcribe(paths2audio_files = file_path)
            return transcriptions
        else:
            logits = asr_model.transcribe(paths2audio_files=[file_path], logprobs=True)
        for logit in logits:
            probs = self.softmax(logit)
            transcript = beam_search_lm.forward(log_probs = np.expand_dims(probs, axis=0), log_probs_length=None)
            transcriptions.append(transcript[0][0][1])
        return transcriptions
        # return transcript[0]

    def Transcription_function(self, time_start, time_end, audio_file, asr_model, beam_search_lm):
        time_start = time_start * 1000   #Works in milliseconds
        time_end =  time_end*1000 
        #read audio data from start time to end time of the audio segment
        audio_segment = AudioSegment.from_wav(audio_file)[time_start:time_end]

        #store the stripped audio in .wav format
        audio_segment.export('audio_segment.wav', format="wav")

        #get the paths of saved files
        audio_segment = os.path.join(os.getcwd(), 'audio_segment.wav')

        text_segment = self.transcribe(asr_model, audio_segment, beam_search_lm)

        #text_segment= dependencies.calls_asr.transcribe(paths2audio_files=audio_segment, batch_size=4)
        return text_segment

    def transcribe_file(self, timestamps, file, asr_model, beam_search_lm):

        #transcribe each timestamp of file if the duration is greater than 1 second
        transcriptions = []
        for timestamp in timestamps:
            if timestamp[1] > 1:
                #get the start and end time from timestamp
                start_time = timestamp[0]
                end_time = timestamp[0]+timestamp[1]
                #transcribe for the timestamp and append the results
                transcriptions.append(self.Transcription_function(start_time, end_time, 
                                                        file, asr_model, beam_search_lm))
        return transcriptions

    def write_files(self, timestamps, audiofile, dir = '/home/cmdadmin/Datalake Pusher/Diarization_Goto/Audio Segments'):
        audio_data = AudioSegment.from_wav(audiofile)
        basefilename = 'audio_segment'
        basefilepath = os.path.join(dir, basefilename)
        filepaths = []
        for index in range(len(timestamps)):
                start_time = timestamps[index][0] * 1000
                end_time = (timestamps[index][0] + timestamps[index][1]) * 1000
                segment_data = audio_data[start_time : end_time]
                filepath = basefilepath + str(index) + '.wav'
                segment_data.export(filepath, format = "wav")
                filepaths.append(filepath)
        return filepaths

    def getTranscription2(self, agent_timestamps, client_timestamps, upload_folder, speech_filepath, silence_timestamps):  
        utils = transcriptionUtils()
        musicTimes = utils.getMusicTimes(upload_folder)
        silence_times = [timestamp[0] for timestamp in silence_timestamps]

        agent_voiced = utils.mapTimes(agent_timestamps, musicTimes)
        client_voiced = utils.mapTimes(client_timestamps, musicTimes)

        agent_original = utils.mapTimes(agent_voiced, silence_times, segment_size = 5)
        client_original = utils.mapTimes(client_voiced, silence_times, segment_size = 5)

        total_timestapms = []
        original_timestamps = []
        total_timestapms.extend(agent_timestamps)
        total_timestapms.extend(client_timestamps)

        original_timestamps.extend(agent_original)
        original_timestamps.extend(client_original)

        total_timestapms.sort()
        original_timestamps.sort()
        cleaned_timestamps = utils.time_converter(original_timestamps)
        transcriptions = []
        filepaths = self.write_files(total_timestapms, speech_filepath)
        text = self.transcribe_whisper(filepaths)
        for index in range(len(total_timestapms)):
            #default naming of speakers, we will later use speaker identification to correctly label the speaker,
            #here purpose of speaker labeling is just to keep a placeholder
            if total_timestapms[index] in agent_timestamps:
                transcriptions.append({'Speaker' : "Agent", 
                                        'Time' : [cleaned_timestamps[index][0], cleaned_timestamps[index][1]], 
                                        'Text' : text[index]})
            else:
                transcriptions.append({'Speaker' :"Client", 
                                        'Time' : [cleaned_timestamps[index][0], cleaned_timestamps[index][1]], 
                                        'Text' : text[index]})
        return transcriptions


    def getTimeTranscriptions (self, time_start, time_end, transcriptions, speaker, joined = True):
        text_list = []
        times_list = []
        for dict in transcriptions:
            if dict['Speaker'] == speaker:
                times_list.append(dict['Time'][1])
                text_list.append(dict['Text'])
        utils = transcriptionUtils()
        seconds_list = utils.getSeconds(times_list)
        start_index = bisect.bisect_left(seconds_list, time_start)
        end_index = bisect.bisect_left(seconds_list, time_end)

        #If the required time is greater than available transcriptions time
        if start_index >= len(seconds_list):
            start_index = len(seconds_list)
        if end_index >= len(seconds_list):
            end_index = len(seconds_list)
        if joined == True:
            try:
                return ' '.join(text_list[start_index : end_index + 1])
            except:
                return ' '.join(text_list[start_index : end_index])
        else:
            try:
                return text_list[start_index : end_index + 1]
            except:
                return text_list[start_index : end_index]
