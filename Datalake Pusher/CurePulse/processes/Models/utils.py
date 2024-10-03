from pydub.silence import split_on_silence
from pydub import AudioSegment
from scipy.io.wavfile import read, write
import numpy as np
from speechbrain.pretrained import VAD
import torch, soundfile as sf
import librosa, resampy
from pathlib import Path
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import tensorflow as tf
import requests
import ast

class Utils:
    def __init__(self) -> None:
        pass

    @staticmethod
    def remove_silence(audio):
        audio_path = audio
        # Pass audio path
        rate, audio = read(audio_path)
        # make the audio in pydub audio segment format
        aud = AudioSegment(audio.tobytes(),frame_rate = rate,
                            sample_width = audio.dtype.itemsize,channels = 1)
        # use split on sience method to split the audio based on the silence, 
        # here we can pass the min_silence_len as silent length threshold in ms and intensity thershold
        audio_chunks = split_on_silence(
            aud,
            min_silence_len = 2000,
            silence_thresh = aud.dBFS - 16,
            keep_silence = 100)
        #audio chunks are combined here
        audio_processed = sum(audio_chunks)
        try:
            output_audio = np.array(audio_processed.get_array_of_samples())
            return output_audio
        except:
            return audio

    @staticmethod        
    def vad_remove_silence_music(audio_, music_model_path):
        audio, sr = librosa.load(audio_, sr=None)
        audio_resampled = resampy.resample(audio, sr, 16000)
        filename = f"/home/cmdadmin/Datalake Pusher/CurePulse/resampled_{Path(audio_).stem}.wav"
        sf.write(filename, audio_resampled, samplerate=16000)

        vad = VAD.from_hparams(source=music_model_path, savedir="pretrained_models/vad-crdnn-libriparty")
        boundaries = vad.get_speech_segments(filename, apply_energy_VAD=True, close_th=3.0)

        segments = vad.get_segments(boundaries, filename)
        try:
            concatenated_tensor = torch.cat(segments, dim=-1)
            vad_audio = concatenated_tensor.cpu().detach().numpy().tolist()[0]
        except:
            vad_audio = audio
        os.remove(filename)
        return vad_audio, 16000, boundaries
    
    @staticmethod
    def ivr_removed_audio(audio, ivr_model_path):
        with tf.device('/CPU:0'):
            model = tf.keras.models.load_model(ivr_model_path)
        sample_rate = 16000
        try: audio, sr = librosa.load(audio)
        except: pass
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
                    predictions = model.predict(segment_features, verbose=0)
                # Interpret the predictions (e.g., classify as machine voice or human voice)
                if predictions[0] <= 0.98:
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

    @staticmethod
    def send_email(subject, email_from, email_to, html_text, importance):
        mime = MIMEMultipart()
        mime['Subject'] = subject
        mime['From'] = email_from
        mime['To'] = email_to
        mime['Importance'] = importance

        mime.attach(MIMEText(html_text, "html"))

        # establish SMTP connection
        server = smtplib.SMTP('sendmail.curemd.com', 25)

        # send the email with attachment
        server.sendmail(mime['From'], mime['To'].split(','), mime.as_string())

        # close the SMTP connection
        server.quit()

    
    @staticmethod
    def send_notification(msg, tokenURL, tokenHeader, msgURL):
        response = requests.post(tokenURL, data=tokenHeader)
        token_gen = ast.literal_eval(response.content.decode())
        
        apiHeader = {
            "Authorization": f"Bearer {token_gen['access_token']}",
            "Content-Type": "application/json",
            "Client" : "CureMD"
        }

        message = {
                    "IncidentId" : "12345164",
                    "IncidentMessage" : f"{msg}",
                    "GroupName" : "CurePulse Service Alerts"
                }
        response = requests.post(msgURL, json=message, headers=apiHeader)


