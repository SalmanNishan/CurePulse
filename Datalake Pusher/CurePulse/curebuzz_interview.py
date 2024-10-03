from processes.Models.music_detector import MusicDetector
from Config import Configuration
from processes.Models.transcription import Transcription
from processes.Models.speech_model import SpeechModel
from processes.Models.textModel import TextClassifier
from processes.runAccentDetection import get_accent
from processes.Models.Inference import Inference

from fastapi import FastAPI, Depends, HTTPException, File, UploadFile, Header, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
from datetime import datetime, timedelta

import uuid
import numpy as np
from pydub import AudioSegment
import io
import os
import gc
from tensorflow.keras.backend import clear_session
import torch
import time
import re
from sqlalchemy import create_engine, Column, String, DateTime, select, Float, Integer
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()

def grammarPerformance(stars):
    '''
    Rates grammar based on stars
    '''
    if stars > 3:
        return "Competent"
    elif stars == 3:
        return "Novice"
    else:
        return "Unacceptable"
    
def extractInferenceSentiment(total_score):
    '''
    States sentiment based on score
    '''
    if total_score >= 0.70:
        return "Positive"
    elif total_score < 0.50:
        return "Negative"
    else:
        return "Neutral"
    
def finalStars(score):
    '''
    Scale inference upto stars
    '''
    return round(score / 0.2)

def escape_special_characters(word):
    return re.escape(word)


class AudioProcessor:
    def __init__(self) -> None:
        self.config_filepath = '/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini'
        self.config = Configuration(self.config_filepath)
        self.config.loadConfiguration()
        self.music_detector = MusicDetector(self.config.ivr_model_path)
        self.transcript = Transcription("medium.en")
        self.speech_model = self.config.speech_model_path
        self.speechModel = SpeechModel(self.speech_model, self.config.labels, self.config.speech_model_class_dict)
        self.textModel = TextClassifier(self.config.textModelPath, self.config.labels)

    def __del__(self):
        del self.music_detector
        del self.transcript 
        del self.speech_model 
        del self.textModel 
    
    def transcription(self, file_path: bytes) -> dict:
        transcription = self.transcript.word_replace(self.transcript.transcribe_whisper(file_path)['text'])
        return {"Transcription" : transcription}
    
    def transcribe_segments(self, file_path: bytes) -> dict:
        transcription = self.transcript.transcribe_whisper(file_path)
        segments = []
        for segment in transcription['segments']:
            updated_text = self.transcript.word_replace(segment['text'])
            segments.append({"start": segment['start'],
                            "end": segment['end'],
                            "text": updated_text})
        return {"Segments": segments}

    def speech_conversion(self, file_path: bytes) -> dict:
        # Music Detection
        audio_music_removed, speech_segments, audio_length = self.music_detector.remove_music(file_path, self.config.music_model_path)
        holding_time = self.music_detector.get_holding_time(speech_segments.tolist(), audio_length)
        
        # Transcription
        transcription = self.transcript.word_replace(self.transcript.transcribe_whisper(np.array(audio_music_removed).astype(np.float32))['text'])

        # Tone Sentiment
        emotion_scores, pred_probabilitites_agent = self.speechModel.predictSentiment(audio_music_removed, timestep = 4)
        emotion_agent = self.config.labels[np.argmax(emotion_scores)]
        tone_stars, emotion_agent = self.speechModel.predict_stars(emotion_scores, self.config.tone_thresholds, self.config.stars_sentiment_mapping)

        # Text Sentiment
        textList = []
        textList.extend(transcription.split('.'))
        textList = [item for item in textList if item != ""]
        try:
            textEmotionScore, text_sentiments_list = self.textModel.predict_sentiment(textList)
            textstars, text_sentiment, text_sentiments_count, textEmotionScore = self.textModel.predict_stars_sentiment(text_sentiments_list, self.config.text_thresholds, self.config.stars_sentiment_mapping)
        except:
            textEmotionScore = {'Negative': 0, 'Neutral': 1, 'Positive': 0}
            textstars = 3
            text_sentiment = 'Neutral'
            text_sentiments_list = []
            text_sentiments_count = {'Negative': 0, 'Neutral': len(textList), 'Positive': 0}

        # language model
        language_scores, overall_language_score, language_score_percentage = self.textModel.predict_language_scores(self.config.LanguageModel, self.config.LanguageVectorizer, transcription)
        language_stars = self.textModel.get_language_stars(language_score_percentage, self.config.language_thresholds)
        
        # accent detection
        accent_score, accent_language, accent_type, accent_stars = get_accent(file_path, self.config)

        # inference engine
        inference_engine = Inference(tone_stars, emotion_scores, textstars, file_path)
        inference_score, holding_time, hold_time_stars = inference_engine.InferenceEngine(self.config, text_sentiments_count, holding_time, language_stars, accent_stars, 1)

        # getting total duration of a call
        callDuration = float("%.3f" % inference_engine.Duration)

        call_stats = {
            'Call Duration': callDuration,
            'Transcription':  transcription,
            'Tone Sentiment': emotion_agent,
            'Tone Stars': tone_stars,
            'Text Sentiment': text_sentiment,
            'Text Stars': textstars,
            'Language Performance': grammarPerformance(language_stars),
            'Language Stars': language_stars,
            'Accent_Type': accent_type,
            'Accent Stars': accent_stars,
            'Final Inference': extractInferenceSentiment(inference_score),
            'Inference Score': round(inference_score * 100, 2),
            'Inference Stars': finalStars(inference_score)
        }
        return call_stats
    
    def get_all_scores(self, file_path: bytes) -> dict:
        audio_music_removed, speech_segments, audio_length = self.music_detector.remove_music(file_path, self.config.music_model_path)
        holding_time = self.music_detector.get_holding_time(speech_segments.tolist(), audio_length)

        transcription = self.transcript.transcribe_whisper(np.array(audio_music_removed).astype(np.float32))['text']
        word_dict = {"CureMD": ['krmbery','krmd', 'qmd', 'cmd', 'karemd', 'QMD', 'KaremD', 'kembry', 'caremd', 'karem', 'kerm D', 'kmd', 'KmD', 'CaramD', 'qrmd', 'QRMD', 'Qrmd',
                                'qrmd', 'keremd', 'Karm Z', 'QRMD.', 'Career MD', 'Q&MD', 'KOMD', 'CRMD', 'crmd', 'care MD', 'Care MD', 'CARE MD', 'KMD']}
        for key, variations in word_dict.items():
            for variation in sorted(variations, key=len, reverse=True):
                # Use re.sub to replace all case-insensitive occurrences of the variation with the key
                pattern = re.compile(escape_special_characters(variation), re.IGNORECASE)
                transcription = pattern.sub(key, transcription)

        emotion_scores, pred_probabilitites_agent = self.speechModel.predictSentiment(audio_music_removed, timestep = 4)
        emotion_agent = self.config.labels[np.argmax(emotion_scores)]
        tone_stars, emotion_agent = self.speechModel.predict_stars(emotion_scores, self.config.tone_thresholds, self.config.stars_sentiment_mapping)

        textList = []
        textList.extend(transcription.split('.'))
        textList = [item for item in textList if item != ""]
        try:
            textEmotionScore, text_sentiments_list = self.textModel.predict_sentiment(textList)
            textstars, text_sentiment, text_sentiments_count, textEmotionScore = self.textModel.predict_stars_sentiment(text_sentiments_list, self.config.text_thresholds, self.config.stars_sentiment_mapping)

        except:
            textEmotionScore = {'Negative': 0, 'Neutral': 1, 'Positive': 0}
            textstars = 3
            text_sentiment = 'Neutral'
            text_sentiments_list = []
            text_sentiments_count = {'Negative': 0, 'Neutral': len(textList), 'Positive': 0}

        language_scores, overall_language_score, language_score_percentage = self.textModel.predict_language_scores(self.config.LanguageModel, self.config.LanguageVectorizer, transcription)
        language_stars = self.textModel.get_language_stars(language_score_percentage, self.config.language_thresholds)
        
        accent_score, accent_language, accent_type, accent_stars = get_accent(file_path, self.config)

        inference_engine = Inference(tone_stars, emotion_scores, textstars, file_path)
        inference_score, holding_time, hold_time_stars = inference_engine.InferenceEngine(self.config, text_sentiments_count, holding_time, language_stars, accent_stars, 1)

        # getting total duration of a call
        callDuration = float("%.3f" % inference_engine.Duration)

        call_stats = {
            'Call Duration': callDuration,
            'Transcription':  transcription,
            'Tone Scores [Negative, Neutral, Postive]': str(emotion_scores),
            'Tone Sentiment': emotion_agent,
            'Tone Stars': tone_stars,
            'Text Sentiment Count': text_sentiments_count,
            'Text Sentiment Score': textEmotionScore,
            'Text Sentiment': text_sentiment,
            'Text Stars': textstars,
            'language Scores': language_scores,
            'Overall Language Score': overall_language_score,
            'Language Score Percentage': language_score_percentage,
            'Language Performance': grammarPerformance(language_stars),
            'Language Stars': language_stars,
            'Accent Language': accent_language,
            'Accent Score': accent_score,
            'Accent_Type': accent_type,
            'Accent Stars': accent_stars,
            'Final Inference': extractInferenceSentiment(inference_score),
            'Inference Score': round(inference_score * 100, 2),
            'Inference Stars': finalStars(inference_score)
        }
        return call_stats

 
app = FastAPI()
security = HTTPBasic()
# Define the SQLAlchemy Base
# Base = declarative_base()

class APIKey(Base):
    __tablename__ = 'api_keys'
    key = Column(String, primary_key=True)
    expiry = Column(DateTime, nullable=False)

class CallAnalysisResults(Base):
    __tablename__ = 'call_analysis_results'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4())
    filename = Column(String)
    call_duration = Column(Integer)
    transcription = Column(String)
    tone_sentiment = Column(String)
    tone_stars = Column(Integer)
    text_sentiment = Column(String)
    text_stars = Column(Integer)
    language_performance = Column(String)
    language_stars = Column(Integer)
    accent_type = Column(String)
    accent_stars = Column(Integer)
    final_inference = Column(String)
    inference_score = Column(Float)
    inference_stars = Column(Integer)

class CallTranscription(Base):
    __tablename__ = 'call_transcription'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4())
    filename = Column(String)
    transcription = Column(String)

def word_replace(transcription):
    word_dict = {"CureMD": ['krmbery','krmd', 'qmd', 'cmd', 'karemd', 'QMD', 'KaremD', 'kembry', 'caremd', 'karem', 'kerm D', 'kmd', 'KmD', 'CaramD', 'qrmd', 'QRMD', 'Qrmd',
                            'qrmd', 'keremd', 'Karm Z', 'QRMD', 'Career MD', 'Q&MD', 'KOMD', 'CRMD', 'crmd', 'care MD', 'Care MD', 'CARE MD', 'KMD', 'QMV']}
    for i in range(len(transcription)):
        for key, values in word_dict.items():
            for value in values:
                if value in transcription:
                    transcription = transcription.replace(value, key)
    return transcription


# Database Setup
DATABASE_URL = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/CureBuzz_Interviews'
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
 
# Database for users
users_db = {"curepulse": "CureMD!123"}
 
# API keys storage
api_keys = {}
 
def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username in users_db and credentials.password == users_db[credentials.username]:
        return credentials.username
    else:
        raise HTTPException(status_code=401, detail="Unauthorized")
 
def generate_api_key():
    return secrets.token_urlsafe(16)
 

def verify_api_key(Authorization: str = Header(...), db: Session = Depends(get_db)):
    # Query API key directly using ORM
    api_key_info = db.query(APIKey).filter(APIKey.key == Authorization).first()
    
    # Check if API key is valid
    if api_key_info and datetime.utcnow() < api_key_info.expiry:
        return True
    
    raise HTTPException(status_code=401, detail="Unauthorized or expired API key")

@app.post("/auth")
def auth(username: str = Depends(authenticate), db: Session = Depends(get_db)):
    api_key = generate_api_key()
    expiry = datetime.utcnow() + timedelta(hours=1)
    db.add(APIKey(key=api_key, expiry=expiry))
    db.commit()
    return {"api_key": api_key, "expiry": expiry}

 
@app.post("/fileupload", dependencies=[Depends(verify_api_key)])
async def file_upload(file: UploadFile = File(...)):
    ap = AudioProcessor()
    try:
        file_content = await file.read()
        audio = AudioSegment.from_file(io.BytesIO(file_content), format='wav')

        wav_filename = "curepulse_audio_file.wav"
        wav_file_path = os.path.join(os.getcwd(), wav_filename)  # Saves in the current working directory

        # Convert and save to WAV format
        audio.export(wav_file_path, format="wav")

        result = ap.speech_conversion(wav_file_path)
        del ap
        clear_session()
        torch.cuda.empty_cache()
        gc.collect()
    except:
        raise HTTPException(status_code=400, detail="Unable to process audio. Please try again")
    return result

@app.post("/transcription", dependencies=[Depends(verify_api_key)])
async def file_transcription(file: UploadFile = File(...), segments: bool = Query(False, description="Set to True to enable segment-wise transcription")):
    ap = AudioProcessor()
    try:
        file_content = await file.read()
        audio = AudioSegment.from_file(io.BytesIO(file_content), format='wav').set_frame_rate(16000)

        wav_filename = "curepulse_audio_file_trans.wav"
        wav_file_path = os.path.join(os.getcwd(), wav_filename)  # Saves in the current working directory

        # Convert and save to WAV format
        audio.export(wav_file_path, format="wav", bitrate='128k')
        if segments:
            result = ap.transcribe_segments(wav_file_path)
        else:
            result = ap.transcription(wav_file_path)
        del ap
        clear_session()
        torch.cuda.empty_cache()
        gc.collect()

    except:
        raise HTTPException(status_code=400, detail="Unable to process audio. Please try again")
    return result

@app.post("/transcription_test", dependencies=[Depends(verify_api_key)])
async def file_transcription_test(file: UploadFile = File(...), segments: bool = Query(False, description="Set to True to enable segment-wise transcription")):
    ap = AudioProcessor()
    # try:
    file_content = await file.read()
    audio = AudioSegment.from_file(io.BytesIO(file_content), format='wav').set_frame_rate(16000).set_channels(1)

    wav_filename = "curepulse_audio_file_test.wav"
    wav_file_path = os.path.join(os.getcwd(), wav_filename)  # Saves in the current working directory

    # Convert and save to WAV format
    audio.export(wav_file_path, format="wav", bitrate='128k')
    samples = np.array(audio.get_array_of_samples())
    # Normalize the audio to float32 format
    samples = samples.astype(np.float32)
    print(len(samples))

    if segments:
        result = ap.transcribe_segments(samples)
    else:
        result = ap.transcription(samples)
    del ap
    clear_session()
    torch.cuda.empty_cache()
    gc.collect()

    # except:
    #     raise HTTPException(status_code=400, detail="Unable to process audio. Please try again")
    return result

@app.post("/getscores", dependencies=[Depends(verify_api_key)])
async def file_upload(file: UploadFile = File(...)):
    try:
        ap = AudioProcessor()

        file_content = await file.read()
        audio = AudioSegment.from_file(io.BytesIO(file_content), format='wav')

        wav_filename = "curepulse_audio_file.wav"
        wav_file_path = os.path.join(os.getcwd(), wav_filename)  # Saves in the current working directory

        # Convert and save to WAV format
        audio.export(wav_file_path, format="wav")

        result = ap.get_all_scores(wav_file_path)
        del ap
        clear_session()
        torch.cuda.empty_cache()
        gc.collect()
    except:
        raise HTTPException(status_code=400, detail="Unable to process audio. Please try again")
    return result