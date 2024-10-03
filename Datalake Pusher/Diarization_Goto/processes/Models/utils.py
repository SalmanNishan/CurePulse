import numpy as np
from speechbrain.pretrained import VAD
import torch, soundfile as sf
import librosa, resampy
from pathlib import Path
import os
import tensorflow as tf

class Utils:
    def __init__(self) -> None:
        pass
        
    @staticmethod
    def vad_remove_silence_music(audio_):
        audio, sr = librosa.load(audio_, sr=None)
        audio_resampled = resampy.resample(audio, sr, 16000)
        filename = f"/home/cmdadmin/Datalake Pusher/CurePulse/resampled_{Path(audio_).stem}.wav"
        sf.write(filename, audio_resampled, samplerate=16000)

        vad = VAD.from_hparams(source="speechbrain/vad-crdnn-libriparty", savedir="pretrained_models/vad-crdnn-libriparty")

        boundaries = vad.get_speech_segments(filename, apply_energy_VAD=True)
        segments = vad.get_segments(boundaries, filename)
        try:
            concatenated_tensor = torch.cat(segments, dim=-1)
            vad_audio = concatenated_tensor.cpu().detach().numpy().tolist()[0]
        except:
            vad_audio = audio_resampled
        os.remove(filename)
        return vad_audio, 16000

    @staticmethod
    def ivr_removed_audiio(audio, ivr_model_path):
        model = tf.keras.models.load_model(ivr_model_path)
        audio, sample_rate = Utils.vad_remove_silence_music(audio)
        audio = np.array(audio)
        # Define segment duration and overlap
        segment_duration = 4  # Duration of each segment in seconds
        overlap = 0.5  # Overlap between segments as a fraction (e.g., 0.5 for 50% overlap)
        # Calculate segment hop length based on duration and overlap
        segment_hop = int(segment_duration * sample_rate * (1 - overlap))
        human_voice_segments = []
        # Preprocess and make predictions on each segment
        for i in range(0, len(audio), segment_hop):
            try:
                segment = audio[i:i + segment_hop]
                # Preprocess the segment (extract features)
                segment_features = Utils.__preprocess_audio(segment, sample_rate)
                # Reshape the segment features to match the expected input shape
                segment_features = np.reshape(segment_features, (1, -1))
                segment_features = np.expand_dims(segment_features, axis=1)
                # Make predictions using your trained model
                with tf.device('/CPU:0'):
                    predictions = model.predict(segment_features)
                # Interpret the predictions (e.g., classify as machine voice or human voice)
                if predictions[0] <= 0.7:
                    # Save segments with human voice
                    human_voice_segments.append(segment)
            except:
                pass
        # Concatenate human voice segments into a single audio
        human_voice_audio = np.concatenate(human_voice_segments)
        return human_voice_audio, 16000

    def __preprocess_audio(audio, sample_rate):
        # Perform feature extraction, e.g., using MFCC
        mfcc = librosa.feature.mfcc(y=audio, sr=sample_rate, n_mfcc=20)
        # Normalize the features
        normalized_mfcc = (mfcc - np.mean(mfcc)) / np.std(mfcc)
        normalized_mfcc = np.mean(normalized_mfcc, axis=1)
        return normalized_mfcc