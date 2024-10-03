import torch
import torch.nn as nn
import torch.nn.functional as F
import torchaudio

import librosa
import numpy as np
import math
import soundfile as sf

from transformers import AutoConfig, Wav2Vec2FeatureExtractor
from transformers.models.hubert.modeling_hubert import (
    HubertPreTrainedModel,
    HubertModel
)
from torch.nn import BCEWithLogitsLoss, CrossEntropyLoss, MSELoss
from dataclasses import dataclass
from typing import Optional, Tuple
from transformers.file_utils import ModelOutput



@dataclass
class SpeechClassifierOutput(ModelOutput):
    loss: Optional[torch.FloatTensor] = None
    logits: torch.FloatTensor = None
    hidden_states: Optional[Tuple[torch.FloatTensor]] = None
    attentions: Optional[Tuple[torch.FloatTensor]] = None

class HubertClassificationHead(nn.Module):
    """Head for hubert classification task."""

    def __init__(self, config):
        super().__init__()
        self.dense = nn.Linear(config.hidden_size, config.hidden_size)
        self.dropout = nn.Dropout(config.final_dropout)
        self.out_proj = nn.Linear(config.hidden_size, config.num_labels)

    def forward(self, features, **kwargs):
        x = features
        x = self.dropout(x)
        x = self.dense(x)
        x = torch.tanh(x)
        x = self.dropout(x)
        x = self.out_proj(x)
        return x
    
class HubertForSpeechClassification(HubertPreTrainedModel):
    def __init__(self, config):
        super().__init__(config)
        self.num_labels = config.num_labels
        self.pooling_mode = config.pooling_mode
        self.config = config

        self.hubert = HubertModel(config)
        self.classifier = HubertClassificationHead(config)

        self.init_weights()

    def freeze_feature_extractor(self):
        self.hubert.feature_extractor._freeze_parameters()

    def merged_strategy(
            self,
            hidden_states,
            mode="mean"
    ):
        if mode == "mean":
            outputs = torch.mean(hidden_states, dim=1)
        elif mode == "sum":
            outputs = torch.sum(hidden_states, dim=1)
        elif mode == "max":
            outputs = torch.max(hidden_states, dim=1)[0]
        else:
            raise Exception(
                "The pooling method hasn't been defined! Your pooling mode must be one of these ['mean', 'sum', 'max']")

        return outputs

    def forward(
            self,
            input_values,
            attention_mask=None,
            output_attentions=None,
            output_hidden_states=None,
            return_dict=None,
            labels=None,
    ):
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict
        outputs = self.hubert(
            input_values,
            attention_mask=attention_mask,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
        )
        hidden_states = outputs[0]
        hidden_states = self.merged_strategy(hidden_states, mode=self.pooling_mode)
        logits = self.classifier(hidden_states)

        loss = None
        if labels is not None:
            if self.config.problem_type is None:
                if self.num_labels == 1:
                    self.config.problem_type = "regression"
                elif self.num_labels > 1 and (labels.dtype == torch.long or labels.dtype == torch.int):
                    self.config.problem_type = "single_label_classification"
                else:
                    self.config.problem_type = "multi_label_classification"

            if self.config.problem_type == "regression":
                loss_fct = MSELoss()
                loss = loss_fct(logits.view(-1, self.num_labels), labels)
            elif self.config.problem_type == "single_label_classification":
                loss_fct = CrossEntropyLoss()
                loss = loss_fct(logits.view(-1, self.num_labels), labels.view(-1))
            elif self.config.problem_type == "multi_label_classification":
                loss_fct = BCEWithLogitsLoss()
                loss = loss_fct(logits, labels)

        if not return_dict:
            output = (logits,) + outputs[2:]
            return ((loss,) + output) if loss is not None else output

        return SpeechClassifierOutput(
            loss=loss,
            logits=logits,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )
    
class SpeechModel:
    def __init__(self, model_path, labels_dict, class_dict) :
        self.labels_dict = labels_dict
        self.class_dict = class_dict
        self.predictions = []
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        # self.device = torch.device("cpu")
        self.config = AutoConfig.from_pretrained(model_path)
        self.feature_extractor = Wav2Vec2FeatureExtractor.from_pretrained(model_path)
        self.sampling_rate = self.feature_extractor.sampling_rate
        self.model = HubertForSpeechClassification.from_pretrained(model_path).to(self.device)

    def __speech_file_to_array_fn(self, path):
        speech_array, _sampling_rate = torchaudio.load(path)
        resampler = torchaudio.transforms.Resample(_sampling_rate)
        speech = resampler(speech_array).squeeze().numpy()
        return speech
    
    def predict(self, path, sampling_rate):
        speech = self.__speech_file_to_array_fn(path)
        features = self.feature_extractor(speech, sampling_rate=sampling_rate, return_tensors="pt", padding=True)

        input_values = features.input_values.to(self.device)

        with torch.no_grad():
            logits = self.model(input_values).logits

        scores = F.softmax(logits, dim=1).detach().cpu().numpy()[0]
        outputs = [{"Label": self.config.id2label[i], "Score": f"{round(score * 100, 3):.1f}%"} for i, score in enumerate(scores)]
        return outputs

    def prediction(self, path):
        speech, sr = torchaudio.load(path)
        speech = speech[0].numpy().squeeze()
        speech = librosa.resample(y=np.asarray(speech), orig_sr=sr, target_sr=self.sampling_rate)

        outputs = self.predict(path, self.sampling_rate)
        scores = [(float(item['Score'].strip('%')))/100 for item in outputs]
        return scores

    def predictSentiment(self, audio, timestep = 30, sr=16000):
        duration = librosa.get_duration(y=np.array(audio), sr=sr)
        for i in range(0, math.ceil(duration), timestep):
            try:
                start_frame = int(i * sr)
                end_frame = int((i + timestep)  * sr)
                if end_frame > len(audio):
                    end_frame = len(audio)
                segment = audio[start_frame:end_frame]
                segment = np.array(segment)
                sf.write("tone_temp.wav", segment, samplerate=sr)
                self.predictions.append(self.prediction("tone_temp.wav"))
            except:
                pass
        average_preds = np.mean(self.predictions, axis=0)
        return average_preds, self.predictions
    
    def predict_stars(self, average_preds, thresholds, mapping):
        negative_score = average_preds[0]
        neutral_score = average_preds[1]
        positive_score = average_preds[2]

        for star, condition in thresholds.items():
            if eval(condition):
                return star, mapping[star]
        return 3, mapping[3]
    
    def get_labels_count(self):
        label_counts = {label: 0 for label in self.labels_dict.values()}
        # Iterate through the data
        for inner_list in self.predictions:
            max_index = inner_list.index(max(inner_list))  # Get the index of the maximum value
            max_label = self.labels_dict[max_index]  # Get the corresponding label
            if max_label is not None:
                label_counts[max_label] += 1
        return label_counts