import warnings
from librosa import feature
from sklearn.exceptions import ConvergenceWarning
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

import os
import tensorflow as tf

import librosa
import numpy as np
from pydub import AudioSegment
import math
import soundfile
import time

class MusicDetectron():
    def __init__(self, model_path, source_file, dest_dir, timestep=4):
        self.model_path = model_path
        self.source_file = source_file
        self.dest_dir = dest_dir
        self.music_timestamps = {"music" : [], "speech" : []}
        self.music_timestep = timestep
        self.data, self.sr = self.load_audio()
        self.norm_data = self.normalize()
        self.total_duration = librosa.get_duration(filename = self.source_file)
        self.keep_frames = None
        self.silence_frames = None

    def load_audio(self):
        source_data, sr = librosa.load(self.source_file)
        return source_data, sr

    def normalize(self):
        max_sample = np.max(self.data)
        #this is normalized data
        norm_data = self.data/max_sample
        return norm_data

    def voiceActivityDetection(self, frame_length = 22050*5, threshold = 1e-3):
        total_frames = math.ceil( len(self.norm_data) / frame_length )
        samples_remain = len(self.norm_data)
        keep_frames = []
        silence_frames = []
        for frame_number in range(0,total_frames-1):
            start = frame_length * frame_number
            frame_length = min(frame_length, samples_remain)
            end = start + frame_length
            frame = self.norm_data[start:end]

            proportion = np.mean(np.abs(frame) > threshold).astype(np.float32)

            if proportion > 0.25:
                keep_frames.append(frame_number)
            else:
                silence_frames.append(frame_number)

            samples_remain = samples_remain - frame_length

        self.keep_frames = keep_frames
        self.silence_frames = silence_frames

    def setSilenceHoldingTime(self,frame_thresh=3, frame_seconds=5):
        consecutive_frames = 0
        self.silence_hold = 0
        for i in range(len(self.silence_frames) - 1):
            frame_1 = self.silence_frames[i+1]
            frame_0 = self.silence_frames[i]
            if (frame_1-frame_0) == 1:
                consecutive_frames += 1
            else:
                consecutive_frames += 1
                if consecutive_frames >= frame_thresh:
                    self.silence_hold += consecutive_frames*frame_seconds
                consecutive_frames = 0

    def framesToSamples(self, frames, frame_length = 22050*5):
        #here we are gathering all samples of frames in which
        #activity was detected
        values = []
        indices = []
        for frame_number in frames:
            start = frame_length * frame_number
            frame_length = min(frame_length, len(self.norm_data[start:]) )
            end =   frame_length + start
            indices.append([start,end])
            voice = self.norm_data[start:end]
            values.append(voice)
        if len(values) != 0:
            values = np.array(values)
            #rearranging the samples horizontally
            values = np.hstack(values)
        return values, indices

    def indicesToTime(self,indices):
        timestamps = librosa.samples_to_time(indices)
        return timestamps

    def writeAudioData(self,output_path, data):
        soundfile.write(output_path, data, 22050)

    def featureExtraction(self, onset, values):
        time = [onset, onset + self.music_timestep]
        #array containing start idx and end idx of samples corresponding to given time
        indices = librosa.core.time_to_samples(time, sr=self.sr)
        data_chunk = values[indices[0] : indices[1]]
        #trimming the part of audio which is 25 db less than max amplitude
        #data, index = librosa.effects.trim(data, top_db = 10, ref = np.max, frame_length = 22050*60, hop_length = 22050)
        power_spectrum = np.abs(librosa.stft(data_chunk))**2 #creating the power spectrum using the short-time fourier transform (STFT)
        spectogram = librosa.feature.melspectrogram(S=power_spectrum, sr=22050) #creating the spectogram
        #MFCC = librosa.feature.mfcc(S=librosa.power_to_db(spectogram))
        #deomposing the spectogram into bases and activations and
        #using activations as features
        comps, acts =  librosa.decompose.decompose(S=spectogram, n_components=8)
        vel = np.gradient(acts, axis = 1)
        accel = np.gradient(vel, axis = 1)

        #creating feature vector for current timestamp
        feature_vector = np.hstack( (np.mean(acts, axis = 1),
                        np.std(acts, axis = 1),
                        np.mean(vel, axis = 1),
                        np.std(vel, axis = 1),
                        np.mean(accel, axis = 1),
                        np.std(accel, axis = 1),
                        )
                    )
        return feature_vector



    def detectMusic(self):
        with tf.device('/cpu:0'):
         
            music_model = tf.keras.models.load_model(self.model_path, compile=False)
            values, _ = self.framesToSamples(self.keep_frames)
            total_duration = librosa.get_duration(y=values, sr=self.sr)
            remain_time = total_duration
            with open(os.path.join(self.dest_dir,"music_detection.txt"),'w') as file:
                    file.write("")
            total = 0
            with open(os.path.join(self.dest_dir,"music_detection.txt"),'a') as file:
                feature_matrix = []
                timestamp_list = []
                for onset in range(0,int(total_duration),self.music_timestep):
                    feature_vector = self.featureExtraction(onset, values)
                    feature_matrix.append(feature_vector)
                    #updating the remaining time and music timestamp
                    remain_time = remain_time - self.music_timestep
                    timestamp_list.append([onset, self.music_timestep])
                    self.music_timestep = min(self.music_timestep,remain_time)

                feature_matrix = np.array(feature_matrix)
                ##here we performing the prediction in batch form for optimizing execution time
                #music_model is tensorflow sequential model
                before = time.time()
                prediction = music_model.predict(feature_matrix)
       
                total += time.time() - before

                #deciding if prediction is music or speech then appending the timestamp to corresponding bucket
                for i in range(prediction.shape[0]):
                    if prediction[i][0] > prediction[i][1]:
                        self.music_timestamps["music"].append( timestamp_list[i] )
                    else:
                        self.music_timestamps["speech"].append(timestamp_list[i] )
            
                    file.write(f"{prediction[i]} for timestamp {timestamp_list[i]}\n")

    def seperate_files_writer(self):
            # writes two seperate files from the given audio filepath and dictionary of speaker times
            #Files are stored in current directory with names speaker.wav where speaker is the same as
            #in the input dictionary key
            list_speakers = list(self.music_timestamps.keys())
            audio_file = AudioSegment.from_wav(self.source_file)
            values, _ = self.framesToSamples(self.keep_frames)  
            #iterating for each speaker
            for speaker in list_speakers:
                filename = speaker + ".wav" # '_' + file_name
                times_list = self.music_timestamps[speaker]
                #initialize a empty audio segment
                # speaker_audio = AudioSegment.empty()
                speaker_audio = []
                for time in times_list:
                    start_time = time[0]
                    end_time = time[0]+time[1]
                    indices = librosa.core.time_to_samples([start_time, end_time])
                    
                    speaker_audio.extend(values[indices[0]:indices[1]])
                speaker_audio = np.array(speaker_audio)
                
                soundfile.write(os.path.join(self.dest_dir,filename), speaker_audio, self.sr)
            print(f'Wrote speech/music files successfully in current directory.')
            return os.getcwd()

    def seperate_files_writer2(self, file_name):
            # writes two seperate files from the given audio filepath and dictionary of speaker times
            #Files are stored in current directory with names speaker.wav where speaker is the same as
            #in the input dictionary key
            list_speakers = list(self.music_timestamps.keys())
            audio_file = AudioSegment.from_wav(self.source_file)
            values, _ = self.framesToSamples(self.keep_frames)  
            #iterating for each speaker
            for speaker in list_speakers:
                filename = speaker + '_' + file_name
                times_list = self.music_timestamps[speaker]
                #initialize a empty audio segment
                # speaker_audio = AudioSegment.empty()
                speaker_audio = []
                for time in times_list:
                    start_time = time[0]
                    end_time = time[0]+time[1]
                    indices = librosa.core.time_to_samples([start_time, end_time])
                    
                    speaker_audio.extend(values[indices[0]:indices[1]])
                speaker_audio = np.array(speaker_audio)
                
                soundfile.write(os.path.join(self.dest_dir,filename), speaker_audio, self.sr)
            print(f'Wrote speech/music files successfully in current directory.')
            return os.getcwd()

if __name__ == "__main__":
    start = time.time()
    model_path = "/home/sshaharyaar/Model Integration_new/models/MusicDetectron_2"
    target_file = r"/home/sshaharyaar/Uploaded Audio files/1620832678.83638.wav"
    target_dir = r"/home/sshaharyaar/Model Integration_new/uploads/"
    output_file = r"/home/sshaharyaar/Model Integration_new/uploads/music_output.wav"
    # print("Creating object")
    # prev = time.time()

    music_model = MusicDetectron(model_path, target_file, target_dir)

    vad =time.time()
    music_model.voiceActivityDetection()
    print(time.time() - vad)

    values, indices = music_model.framesToSamples(music_model.keep_frames)
    music_model.writeAudioData(output_file,values)
    time2 = music_model.indicesToTime(indices)

    before = time.time()
    music_model.detectMusic()
    print(time.time() - before)
    print(time.time() - start)
