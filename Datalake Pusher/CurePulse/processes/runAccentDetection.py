import time
from speechbrain.pretrained import EncoderClassifier
import librosa
import soundfile as sf

# Process 7
def runAccentDetection(config, file_obj, call_fetcher):
    '''
    Classifies accent into native or non-native and scores it
    Input: Agent file
    Output: Accent Scores
    '''
    print('Running Accent Model for: ', file_obj.filename)

    ## Load Accent Model
    start_time = time.time()
    accent_classifier = EncoderClassifier.from_hparams(source=config.accent_model_path, savedir="pretrained_models/accent_model_hugging_face")

    y, sr = librosa.load(file_obj.agent_file)
    duration = librosa.get_duration(y=y)
    if duration > 1800:
        y = y[: 1800*sr]
        sf.write("accent_temp.wav", y, sr)
        out_prob, score, index, text_lab = accent_classifier.classify_file("accent_temp.wav")

    else:
        out_prob, score, index, text_lab = accent_classifier.classify_file(file_obj.agent_file)
    
    file_obj.accent_execution_time = time.time() - start_time

    ## Classify as native or non-native
    score = score[0].item()
    file_obj.agent_accent_score = score
    file_obj.agent_accent_language = text_lab[0]
    
    stars, accent_type = predict_stars(text_lab[0], score, config.accent_thresholds, config.accent_stars_mapping)
    file_obj.agent_accent_type = accent_type
    file_obj.agent_accent_stars = stars

    print('Accent Model completed for: ', file_obj.filename)
    return file_obj

def get_accent(agent_file, config):
    from tensorflow.keras.backend import clear_session
    import torch
    import gc
    accent_classifier = EncoderClassifier.from_hparams(source=config.accent_model_path, savedir="pretrained_models/accent_model_hugging_face")

    y, sr = librosa.load(agent_file)
    duration = librosa.get_duration(y=y)
    if duration > 1800:
        y = y[: 1800*sr]
        sf.write("accent_temp.wav", y, sr)
        out_prob, score, index, text_lab = accent_classifier.classify_file("accent_temp.wav")

    else:
        out_prob, score, index, text_lab = accent_classifier.classify_file(agent_file)
    

    ## Classify as native or non-native
    score = score[0].item()
    agent_accent_score = score
    agent_accent_language = text_lab[0]
    
    stars, accent_type = predict_stars(text_lab[0], score, config.accent_thresholds, config.accent_stars_mapping)
    agent_accent_type = accent_type
    agent_accent_stars = stars
    del accent_classifier
    clear_session()
    torch.cuda.empty_cache()
    gc.collect()

    return agent_accent_score, agent_accent_language, agent_accent_type, agent_accent_stars

def predict_stars(text_lab, score, thresholds, mapping):
    if text_lab in ['us']:
        for star, condition in thresholds['us'].items():
            if eval(condition):
                return star, mapping[star]
        return 3, mapping[3]
    elif text_lab in ['england', 'canada']:
        for star, condition in thresholds['england'].items():
            if eval(condition):
                return star, mapping[star]
        return 3, mapping[3]
    else:
        for star, condition in thresholds['others'].items():
            if eval(condition):
                return star, mapping[star]
        return 2, mapping[2]