import numpy as np
from processes.Models.transcription import Transcription
import re
import os
import soundfile as sf


class Explainability:

    def __init__(self):
        pass
    
    def textExplanation(self, text_list, sentiments_list, stars):
        text_explanation = []
        if stars == 3:
            pass
        else:
            indices = []
            for index, entry in enumerate(sentiments_list):
                entry_label = entry[0]['label']
                entry_score = entry[0]['score']
                
                if stars >= 4 and entry_label == 'LABEL_2':
                    indices.append(index)
                elif stars <= 2 and entry_label == 'LABEL_0':
                    indices.append(index)

            for idx in indices:
                text = text_list[idx]
                if len(text.split()) >= 5 and not (re.search(r'\d', text) or re.search(r'\?', text)):
                    text_explanation.append(text)

        return text_explanation
    
    def toneExplanation(self, speaker, audio, pred_probabilitites, stars, filename, directory, sr = 16000):
        transcription = Transcription("base.en")
        if stars == 3:
            num_samples = int(sr * 0.1)
            reason_audio_data = np.zeros((num_samples, 2))
        else:
            reason_audio_data = []
            indices_audio = []
            for prob_index, probs in enumerate(pred_probabilitites):
                max_index = np.argmax(probs)
                if stars >= 4 and max_index == 2:
                    reason_audio_data.append(audio[(prob_index * 4)*sr:((prob_index+1) * 4)*sr])
                    indices_audio.append(prob_index)
                elif stars <= 2 and max_index == 0:
                    reason_audio_data.append(audio[(prob_index * 4)*sr:((prob_index+1) * 4)*sr])
                    indices_audio.append(prob_index)
            try:
                def find_max_consecutive_indices(indices):
                    max_start = 0
                    max_length = 0
                    current_start = 0
                    current_length = 0
                    for idx in range(1, len(indices)):
                        if indices[idx] == indices[idx - 1] + 1:
                            current_length += 1
                            if current_length > max_length:
                                max_length = current_length
                                max_start = current_start
                        else:
                            current_start = idx
                            current_length = 0
                    return list(range(max_start, max_start + max_length + 1))
                
                indices_audio = find_max_consecutive_indices(indices_audio)
                reason_audio_data = np.array([reason_audio_data[i] for i in indices_audio])
                reason_audio_data = np.concatenate(reason_audio_data)
            except:
                pass

        if len(reason_audio_data) == 0:
            num_samples = int(sr * 0.1)
            reason_audio_data = np.zeros((num_samples, 2))
        
        if ".wav" in filename:
            filename = filename.replace(".wav", "")

        reason_audio_path = filename  + "_" + speaker + "_clip.wav"
        _path = os.path.join(directory, reason_audio_path)
        sf.write(_path, reason_audio_data, samplerate=sr)
        try:
            result = transcription.transcribe_whisper(_path)
            reason_text = result["text"]
        except:
            reason_text = ''
        return reason_audio_path, reason_text