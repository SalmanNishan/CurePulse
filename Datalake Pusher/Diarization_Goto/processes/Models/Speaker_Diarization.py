# from torch._C import device
from distutils.fancy_getopt import wrap_text
from importlib_metadata import files
import librosa
import scipy as sp
import soundfile
import copy
import os
import time
from omegaconf import OmegaConf
import torch
from nemo.collections.asr.models import ClusteringDiarizer
from pydub import AudioSegment
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from processes.Models.transcription import Transcription
from thefuzz import fuzz
import tensorflow as tf
# import torch.multiprocessing as mp 
# mp.set_start_method('spawn')
# import nemo.collections.asr as nemo_asr



class SpeakerDiarization:
    def __init__(self, audiofile, configuration_path, output_dir,time_window, No_of_speakers):
        self.audiofile = audiofile
        self.file_duration = librosa.get_duration(filename=audiofile)
        self.time_window = time_window
        self.No_of_speakers = No_of_speakers
        self.configuration_path = configuration_path
        self.output_dir = output_dir
        self.audiofilechunk = 'temp.wav'
        self.audiofilechunk_path = os.path.join(self.output_dir, self.audiofilechunk)
        self.cfg_yaml = self.loadConfiguration()
        # self.nemo_vad_diarizer = ClusteringDiarizer(cfg=self.cfg_yaml)

    
    def writeAudioChunk(self,onset):
        if self.time_window <=self.file_duration:
            data, sr = librosa.load(self.audiofile, sr=22050, 
                                    offset=float(onset), duration=float(self.time_window))

            soundfile.write(self.audiofilechunk_path,data,sr)
        else:
            print("Given duration is more than audio time length")
        

    def getTimestampTuple(self, timestamps, index):
        desired_timestamp = timestamps[index]
        return desired_timestamp


    def readRTTFFile(self, filepath):
        uniq_speakers = []
        speakers_timestamp = {}

        with open(filepath, 'r') as rttm_file:
            segments = rttm_file.readlines()

        for segment_idx in range(len(segments)):
            words = segments[segment_idx].split()[3:-1] #removing first three columns
            speaker_tag = words[4] #speaker tag
            if speaker_tag not in uniq_speakers:
                uniq_speakers.append(speaker_tag) 

        for speaker in uniq_speakers:
            speaker_onset_dur_list = list()
            for segment_idx in range(len(segments)):
                words = segments[segment_idx].split()[3:-1] #removing first three columns
                onset = words[0] #speaker tag
                duration = words[1] #duration for which speaker is speaking
                speaker_tag = words[4] #speaker tag
                if speaker_tag == speaker:
                    #appending the timestamp-list to a list
                    speaker_onset_dur_list.append([onset,duration])
            speakers_timestamp[speaker] = speaker_onset_dur_list
        return speakers_timestamp


    def addOnset(self, cur_speakers_timestamp, last_speaker_timestamps):
        last_onset = float(last_speaker_timestamps[-1][0]) #fetching onset from last tuple
        for speaker in cur_speakers_timestamp.keys():
            timestamp_list = cur_speakers_timestamp[speaker]
            for t in range(len(timestamp_list)):
                #Step 1: access timestamps of a speaker
                #Step 2: access t-th timestamp of a speaker
                #Step 3: access on set of t-th timestamp of a speaker
                current_onset = float(cur_speakers_timestamp[speaker][t][0])
                cur_speakers_timestamp[speaker][t][0] = str(current_onset + last_onset)
                
        return cur_speakers_timestamp        


    def appendTimestamps(self, speakers_timestamp, cur_speakers_timestamp, ):
        for speaker in speakers_timestamp.keys():
            if speaker in cur_speakers_timestamp.keys():
                for t in cur_speakers_timestamp[speaker]:
                    speakers_timestamp[speaker].append(t) 
        return speakers_timestamp


    def getSpeakers(self, cur_speakers_timestamp):
        uniq_speakers = list(cur_speakers_timestamp.keys())
        return uniq_speakers


    def validateOneSpeaker(self, rttm_dir, filename, speakers_timestamps=None, last_speaker=None):
        cur_speakers_timestamp = self.readRTTFFile(os.path.join(rttm_dir, filename))
        uniq_speakers = self.getSpeakers(cur_speakers_timestamp)
        if last_speaker != None:
            temp = {last_speaker : cur_speakers_timestamp[uniq_speakers[0]]}
            cur_speakers_timestamp = temp
            #reassigning label of speaker #it will deal with the problem when last speaker
            #of previous segment has label speaker_a but speaker of #this segment label has speaker_b
            uniq_speakers[0] = last_speaker
    
        speaker = uniq_speakers[0]
        last_speaker_timestamps = speakers_timestamps[speaker]
        cur_speakers_timestamp =  self.addOnset(cur_speakers_timestamp,last_speaker_timestamps)

        speakers_timestamps = self.appendTimestamps(speakers_timestamps, cur_speakers_timestamp)
   
        return speakers_timestamps, speaker 

    def validateTwoSpeakers(self, rttm_dir, filename, speakers_timestamps=None, last_speaker=None):
        #we have to identify that in current frame which label corresponds to the label
        #of speaker in previous frame
        
        #speakers_timestamps is basically a history of timestamps for both speakers
        cur_speakers_timestamp = self.readRTTFFile(os.path.join(rttm_dir, filename))
        uniq_speakers = self.getSpeakers(cur_speakers_timestamp)
        if last_speaker == None:
            last_timestamp_s1 = cur_speakers_timestamp[uniq_speakers[0]][-1]
            last_timestamp_s2 = cur_speakers_timestamp[uniq_speakers[1]][-1]
            #this if statement gives us the label of last speaker
            if float(last_timestamp_s1[0]) > float(last_timestamp_s2[0]):
                last_speaker = uniq_speakers[0]
            else:
                last_speaker = uniq_speakers[1]
            speakers_timestamps = cur_speakers_timestamp
            
        else:
            #first timestamps of both speakers
            first_timestamp_s1 = cur_speakers_timestamp[uniq_speakers[0]][0]
            first_timestamp_s2 = cur_speakers_timestamp[uniq_speakers[1]][0]
            #if this is true then label uniq_speakers[1] of current speaker
            #is basically the label of last speaker in current frame
            #else label uniq_speakers[0] of current speaker
            #is basically the label of last speaker in current frame
            if float(first_timestamp_s1[0]) > float(first_timestamp_s2[0]):
            #first speaker data is present in speakers_timestamp["speaker_1"]
            #instead of being present in speakers_timestamp['speaker_0"]
                #adding the onset of last speaker to current timestamps

                last_speaker_timestamps = speakers_timestamps[last_speaker]
                cur_speakers_timestamp =  self.addOnset(cur_speakers_timestamp,last_speaker_timestamps)
                
                #in case of label mismatch for a speaker then perform the label-correnction
                if last_speaker != uniq_speakers[1]:
                    temp = cur_speakers_timestamp[uniq_speakers[0]]
                    cur_speakers_timestamp[uniq_speakers[0]] = cur_speakers_timestamp[uniq_speakers[1]] 
                    cur_speakers_timestamp[uniq_speakers[1]] = temp
                    
                    temp = uniq_speakers[1]
                    uniq_speakers[1] = last_speaker
                    uniq_speakers[0] = temp    
                
                speakers_timestamps = self.appendTimestamps(speakers_timestamps, cur_speakers_timestamp)
            else:
                
                last_speaker_timestamps = speakers_timestamps[last_speaker]
                cur_speakers_timestamp =  self.addOnset(cur_speakers_timestamp,last_speaker_timestamps)
                
                #in case of label mismatch for a speaker then perform the label-correnction
                if last_speaker != uniq_speakers[0]:
                    temp = cur_speakers_timestamp[uniq_speakers[0]]
                    cur_speakers_timestamp[uniq_speakers[0]] = cur_speakers_timestamp[uniq_speakers[1]] 
                    cur_speakers_timestamp[uniq_speakers[1]] = temp
                    
                    temp = uniq_speakers[0]
                    uniq_speakers[0] = last_speaker
                    uniq_speakers[1] = temp
                
                speakers_timestamps = self.appendTimestamps(speakers_timestamps, cur_speakers_timestamp)

            #Precondition: assuming label mismatch has been satisfied in previos
            #code blocks present above
            last_timestamp_s1 = speakers_timestamps[uniq_speakers[0]][-1]
            last_timestamp_s2 = speakers_timestamps[uniq_speakers[1]][-1]
            #this if statement gives us the label of last speaker

            if float(last_timestamp_s1[0]) > float(last_timestamp_s2[0]):
                last_speaker = uniq_speakers[0]
            else:
                last_speaker = uniq_speakers[1]
            
        return speakers_timestamps, last_speaker
        


    def loadConfiguration(self):
    
        config = OmegaConf.load(self.configuration_path) 
        
        pretrained_vad = 'vad_marblenet'
        pretrained_speaker_model = 'ecapa_tdnn'

        config.diarizer.paths2audio_files = [self.audiofilechunk_path]
        config.diarizer.path2groundtruth_rttm_files = None
        config.diarizer.out_dir = self.output_dir # Directory to store intermediate files and prediction outputs
        config.diarizer.speaker_embeddings.model_path = pretrained_speaker_model

        #Here we use our inhouse pretrained NeMo VAD 
        config.diarizer.oracle_num_speakers=self.No_of_speakers
        config.diarizer.vad.model_path = pretrained_vad
        config.diarizer.vad.window_length_in_sec = 0.15 #0.15
        config.diarizer.vad.shift_length_in_sec =  0.01 # 0.01

        config.diarizer.vad.postprocessing_params.onset = 0.8 
        config.diarizer.vad.postprocessing_params.offset = 0.7
        config.diarizer.vad.postprocessing_params.min_duration_on = 0.1
        config.diarizer.vad.postprocessing_params.min_duration_off =0.3
        
        return config

    def diarization(self):
        #config = self.loadConfiguration(output_dir, audiofilechunk)
        #print(OmegaConf.to_yaml(config))
        # nemo_vad_diarizer = ClusteringDiarizer(cfg=self.cfg_yaml)
        # nemo_vad_diarizer.diarize()
        filename = self.audiofilechunk_path.split("/")[-1] #gettting filename with extension
        
        #only fetching the base filename and appending rttm extension
        if len(filename.split(".")) == 2:
            basefilename = filename.split(".")[0] + ".rttm"
            return basefilename
        elif len(filename.split(".")) == 3:    
            basefilename = filename.split(".")[0] + "." + filename.split(".")[1] + ".rttm"
            return basefilename
        else:
            return 0

    def perfromDiarization(self):
        
        rttm_dir= os.path.join(self.output_dir,'pred_rttms')
        total_duration = librosa.get_duration(filename = self.audiofile) 
        onset = 0
        remain_time = total_duration

        last_speaker = None
        speakers_timestamps = {}
        while remain_time > 0.0:
            
            #################Added#############
            if total_duration < self.time_window:
                self.time_window = total_duration
                remain_time=0.0
            ###################################
            
            self.writeAudioChunk(onset)
            filename_rttm = self.diarization()
            cur_speakers_timestamp = self.readRTTFFile(os.path.join(rttm_dir, filename_rttm))
            uniq_speakers = self.getSpeakers(cur_speakers_timestamp)
            if len(uniq_speakers) == 2:
                speakers_timestamps, last_speaker = self.validateTwoSpeakers(rttm_dir, filename_rttm, 
                                                                speakers_timestamps, last_speaker)
                #updating the onset and time window parameters
                if remain_time > self.time_window:
                    #last timestamp of last speaker
                    onset, duration = speakers_timestamps[last_speaker][-1]
                    remain_time = total_duration - float(onset)
                    self.time_window = min(remain_time, self.time_window)
                    
                else:
                    remain_time = 0.0
                
            elif len(uniq_speakers) == 1:

                speakers_timestamps, last_speaker = self.validateOneSpeaker(rttm_dir, filename_rttm, 
                                                                speakers_timestamps, last_speaker)
 
                #updating the onset and time window parameters
                if remain_time > self.time_window:
                    #last timestamp of last speaker
                    onset, duration = speakers_timestamps[last_speaker][-1]
                    onset = str(float(onset) + (float(duration))/2 )
                    remain_time = total_duration - float(onset)
                    self.time_window = min(remain_time, self.time_window)
                    
                else:
                    remain_time = 0.0
                
                
        return speakers_timestamps


    def float_converter(self, times_dict):
        for key in times_dict.keys():
            stamps = times_dict[key]
            for index in range(len(stamps)):
                times_dict[key][index] = [float(times_dict[key][index][i]) for i in range(len(times_dict[key][index]))]
        return times_dict

    def dict_cleaner(self, timestamps):
        clean_dict ={}
        times_dict = self.float_converter(timestamps)
        list_speakers = list(times_dict.keys())
        for speaker in list_speakers:
            times_list = times_dict[speaker]
            temp = copy.deepcopy(times_list)
            for index in range(len(times_list)-1):
                if times_list[index][0] == times_list[index+1][0]:
                    temp.remove(times_list[index])
            times_list = temp
            clean_dict[speaker] = times_list
        return clean_dict

    def seperate_files_writer2(self, file_name, times_dict, dest_dir=r"/home/cmdadmin/Datalake Pusher/Diarization_Goto/static/assets/"):
        # writes two seperate files from the given audio filepath and dictionary of speaker times
        #Files are stored in current directory with names speaker.wav where speaker is the same as
        #in the input dictionary key

        #First, convert the timestamps into float
        times_dict = self.float_converter(times_dict)
        file_path = self.audiofile
        list_speakers = list(times_dict.keys())
        audio_file = AudioSegment.from_wav(file_path)

        #iterating for each speaker
        for speaker in list_speakers:
            filename = speaker + '_' + file_name
            times_list = times_dict[speaker]
            #initialize a empty audio segment
            speaker_audio = AudioSegment.empty()

            for time in times_list:
                start_time = time[0]*1000
                end_time = (time[0]+time[1])*1000
                speaker_audio += audio_file[start_time:end_time]
            speaker_audio.export(os.path.join(dest_dir,filename), format="wav")
        return dest_dir

    def comprehend(self, original_str):
        word_corpus_list = ['hello',  'hi', 'hey', 'my', 'name', 'is', 'doctor', 'from', 'you', 'how', 'press', 'unavailable']
        if len(original_str) < 2:
            return False
        
        original_str_list = original_str.split(' ')
        if len(original_str_list) < 2:
            return False

        for word in word_corpus_list:
            if fuzz.partial_ratio(original_str, word) == 100:
                return True
        return False

    def consideration(self, timestamps, text_speaker_0, text_speaker_1):
        ts0 = 0 #timestamp of speaker 0
        ts1 = 0 #timestamp of speaker 1
        neg = []
        for i in range(len(text_speaker_0)):
            #comprehend check if string contains the desired word or not
            #timestamps['speaker_0'][i][1] checks the i-th timetamp duration if it is greater then 0.5s
            if self.comprehend(text_speaker_0[i]) and timestamps['speaker_0'][i][1] > 0.5:
                ts0 = i
                neg.append('ts0')
                break

        for i in range(len(text_speaker_1)):
            if self.comprehend(text_speaker_1[i]) and timestamps['speaker_1'][i][1] > 0.5:
                ts1 = i
                neg.append('ts1')
                break
        return ts0, ts1, neg

    def speakerIdentification(self, filename, files_dir, speaker_0_path, speaker_1_path, transcriptions, timestamps, call_type, interview = True, time_start = 0, time_end = 60):
            #returns paths of agent and client's audio files saved

            agent_path = files_dir + '/' + 'Agent_' + filename
            client_path = files_dir + '/' + 'Client_' + filename
            transcription = Transcription()
            text_speaker_0 = transcription.getTimeTranscriptions(time_start, time_end, transcriptions, 'Agent', False) #find agent transcription in "transcriptions" parameter
            text_speaker_1 = transcription.getTimeTranscriptions(time_start, time_end, transcriptions, 'Client', False)
            
            # checks if dialogue is > 0.5 seconds and has comprehendable words
            ts0, ts1, neg = self.consideration(timestamps, text_speaker_0, text_speaker_1)

            if len(neg) == 1:
                if neg[0] == 'ts0':
                    ts1 = len(timestamps['speaker_1'])-1
                else:
                    ts0 = len(timestamps['speaker_0'])-1
            
            if len(neg) == 0:
                print('Nothing was considerable, please check')

            if call_type == 'incoming':
                if timestamps['speaker_0'][ts0][0] <= timestamps['speaker_1'][ts1][0]:
                    agent_file = agent_path
                    client_file = client_path
                    agent_times = timestamps['speaker_0']
                    client_times = timestamps['speaker_1']
                    os.rename(speaker_0_path, agent_path)
                    os.rename(speaker_1_path, client_path)
                else:
                    agent_file = agent_path
                    client_file = client_path
                    agent_times = timestamps['speaker_1']
                    client_times = timestamps['speaker_0']
                    os.rename(speaker_0_path, client_path)
                    os.rename(speaker_1_path, agent_path)
                    for index in range(len(transcriptions)):
                        if transcriptions[index]['Speaker'] == 'Agent':
                            transcriptions[index]['Speaker'] = 'Client'
                        else:
                            transcriptions[index]['Speaker'] = 'Agent'

            else:
                if timestamps['speaker_0'][ts0][0] > timestamps['speaker_1'][ts1][0]:
                    agent_file = agent_path
                    client_file = client_path
                    agent_times = timestamps['speaker_0']
                    client_times = timestamps['speaker_1']
                    os.rename(speaker_0_path, agent_path)
                    os.rename(speaker_1_path, client_path)
                else:
                    agent_file = agent_path
                    client_file = client_path
                    agent_times = timestamps['speaker_1']
                    client_times = timestamps['speaker_0']
                    os.rename(speaker_0_path, client_path)
                    os.rename(speaker_1_path, agent_path)
                    for index in range(len(transcriptions)):
                        if transcriptions[index]['Speaker'] == 'Agent':
                            transcriptions[index]['Speaker'] = 'Client'
                        else:
                            transcriptions[index]['Speaker'] = 'Agent'

            return agent_file, client_file, agent_times, client_times, transcriptions
