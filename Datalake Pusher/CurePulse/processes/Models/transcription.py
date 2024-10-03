import whisper
from fuzzywuzzy import fuzz
import numpy as np
import copy
import re

class Transcription():
    def __init__(self, model):
        self.whisper_asr = whisper.load_model(model) #("base.en")
        self.voicemail_phrases = [
                "Hi, you've reached", 
                "Hello, this is", 
                "You've reached the voicemail of", 
                "You've reached [Name] at [Company]", 
                "This is [Name]. I'm not available to take your call right now",
                "Sorry I missed your call",
                "Please leave a message after the beep",
                "Leave your name and number, and I'll get back to you",
                "Feel free to leave a message and I'll return your call",
                "Please leave a message with your name and phone number",
                "At the tone, please record your message",
                "Please leave a detailed message and I will get back to you shortly",
                "Leave a message and I'll return your call as soon as possible",
                "Leave a message after the beep, and I'll respond as soon as possible",
                "I'm currently unavailable",
                "I'm out of the office",
                "I'm away from my desk",
                "I can't take your call right now"
            ]

    # Function to check if a transcription is a voicemail
    def __is_voicemail(self, transcription):
        for phrase in self.voicemail_phrases:
            if re.search(re.escape(phrase), transcription, re.IGNORECASE):
                return True
        return False
    
    def transcribe_whisper(self, audio):
        result = self.whisper_asr.transcribe(audio)
        return result
    
    def __get_transcriptions_lists(self, transcribed_audio, transcribed_audio_agent, transcribed_audio_client):
        audio_transcription = self.word_replace([[line['text'], line['start'], line['end']] for line in transcribed_audio["segments"]])
        agent_transcription = self.word_replace([line['text'] for line in transcribed_audio_agent["segments"]])
        client_transcription = self.word_replace([line['text'] for line in transcribed_audio_client["segments"]])
        return audio_transcription, agent_transcription, client_transcription
    
    def word_replace(self, transcription):
        word_dict = {"CureMD": ["CARAMD", "Caramd", "caramd", "CARE MD", "Care md", "care md", "CAREER MD", "Career md", "career md", "CAREMD", "Caremd", "caremd", "CMD", "Cmd",
                                "cmd", "CRMD", "Crmd", "crmd", "CURE MEDI", "Cure medi", "cure medi","CURAMD", "Curamd", "curamd", "KAREM", "Karem", "karem", "KAREMD", "Karemd",
                                "karemd", "KARM Z", "Karm z", "karm z", "KEREMD", "Keremd", "keremd", "KERIMD", "Kerimd", "kerimd", "KERM D", "Kerm d", "kerm d", "KMD", "Kmd",
                                "kmd", "KOMD", "Komd", "komd", "KRMERY", "Krmery", "krmery", "KRMD", "Krmd", "krmd", "KEMBRY", "Kembry", "kembry", "QMD", "Qmd", "qmd", "QM-G",
                                "Qm-g", "qm-g", "QMV", "Qmv", "qmv", "Q&MD", "Q&md", "q&md", "QRMD", "Qrmd", "qrmd"]}

        for i in range(len(transcription)):
            for key, values in word_dict.items():
                for value in values:
                    if value in transcription:
                        transcription = transcription.replace(value, key)
        return transcription
            
    def __find_best_match(self, line, sentences):
        highest_score = 0
        for sentence in sentences:
            similarity_score = fuzz.ratio(line, sentence)
            if similarity_score > highest_score:
                highest_score = similarity_score
        return highest_score
    
    def __format_time(self, seconds):
        return f"{int(seconds//60):02d}:{int(seconds%60):02d}"

    def __merge_consecutive_segments(self, segment_list):
        transcription = copy.deepcopy(segment_list)
        merged_segments = []
        if not transcription:
            return merged_segments
        current_segment = transcription[0]
        for segment in transcription[1:]:
            if segment['Speaker'] == current_segment['Speaker']:
                current_segment['Text'] += ' ' + segment['Text']
                current_segment['End'] = segment['End']
            else:
                current_segment['Time'] = [self.__format_time(current_segment['Start']), self.__format_time(current_segment['End'])]
                merged_segments.append(current_segment)
                current_segment = segment
        # Add the last merged segment
        current_segment['Time'] = [self.__format_time(current_segment['Start']), self.__format_time(current_segment['End'])]
        merged_segments.append(current_segment)
        for segment in merged_segments:
            segment.pop('Start', None)
            segment.pop('End', None)
        return merged_segments
    
    def get_transcriptions(self, agent_name, client_name, audio, agent_audio, client_audio):
        transcribed_audio = self.transcribe_whisper(np.array(audio).astype(np.float32))
        transcribed_audio_agent = self.transcribe_whisper(agent_audio)
        transcribed_audio_client = self.transcribe_whisper(client_audio)
        audio_transcription, agent_transcription, client_transcription = self.__get_transcriptions_lists(transcribed_audio, transcribed_audio_agent, transcribed_audio_client)
        
        # for line in transcribed_audio_client:
        #     if self.__is_voicemail(line):
        #         raise "Voicemail Detected"
            
        paragraph_agent_transcription = ''.join(agent_transcription)
        paragraph_client_transcription = ''.join(client_transcription)
        self.transcriptions = []
        for line in audio_transcription:
            agent_score = self.__find_best_match(line[0], agent_transcription)
            client_score = self.__find_best_match(line[0], client_transcription)
            if agent_score > client_score:
                self.transcriptions.append({
                    "Speaker": "Agent", 
                    "Text": line[0],
                    "Start": round(line[1], 1),
                    "End": round(line[2], 1)
                    })
            else:
                self.transcriptions.append({
                    "Speaker": "Client", 
                    "Text": line[0],
                    "Start": round(line[1], 1),
                    "End": round(line[2], 1)
                    })
        updated_transcriptions = self.__merge_consecutive_segments(self.transcriptions)
        transcription = self.__update_speaker_names(updated_transcriptions, agent_name, client_name)
        return transcription, updated_transcriptions, paragraph_agent_transcription, paragraph_client_transcription
    
    def get_times(self):
        agent_times = [[segment["Start"], segment["End"]] for segment in self.transcriptions if segment["Speaker"] is "Agent"]
        client_times = [[segment["Start"], segment["End"]] for segment in self.transcriptions if segment["Speaker"] is "Client"]
        return agent_times, client_times
    
    def __update_speaker_names(self, data_list, agent_name, client_name):
        transcript = copy.deepcopy(data_list)
        for item in transcript:
            if item['Speaker'] == 'Agent':
                item['Speaker'] = agent_name
            elif item['Speaker'] == 'Client':
                item['Speaker'] = client_name
        return transcript