import tensorflow as tf
import librosa
import numpy as np
import resampy

from processes.Models.utils import Utils

class MusicDetector():
    def __init__(self, ivr_model):
        self.ivr_model = ivr_model

    def remove_music(self, file_path, music_model_path, agent=False):
        try:
            audio, sr, speech_segments = Utils.vad_remove_silence_music(file_path, music_model_path)
            if not agent:
                with tf.device('/CPU:0'):
                    ivr_removed_audio, sr = Utils.ivr_removed_audio(np.array(audio), self.ivr_model)
            else:
                ivr_removed_audio = audio
        except:
            audio, sr = librosa.load(file_path, sr=8000)
            ivr_removed_audio = resampy.resample(audio, sr, 16000)
        duration = len(audio)/sr

        return ivr_removed_audio, speech_segments, duration
    
    def get_holding_time(self, speech_segments, total_audio_length, silence_threshold=10):
        # Sort the speech segments by their start time
        speech_segments.sort(key=lambda x: x[0])

        
        total_silence_time = 0
        last_speech_end = speech_segments[0][1]
        
        for start, end in speech_segments[1:]:
            # Calculate the duration of silence between segments
            silence_duration = start - last_speech_end
            
            if silence_duration >= silence_threshold:
                total_silence_time += silence_duration
            
            last_speech_end = end

        # Calculate the silence after the last speech segment
        final_silence_duration = total_audio_length - last_speech_end
        
        if final_silence_duration >= silence_threshold:
            total_silence_time += final_silence_duration
        
        return total_silence_time