import time, librosa

from processes.Models.Inference import Inference

# Process 8
def runInference(config, file_obj, call_fetcher):

    '''
    Inferences the overall client and agent sentiment score.
    Takes tuple as input for client inference
    Input: Scores
    Output: Inference Score
    '''

    print('Running Inference for: ', file_obj.filename)

    start_time = time.time()
    client_inference_engine = Inference(file_obj.client_tone_stars, file_obj.scores_client_emotion, file_obj.textclientstars, file_obj.audiofile_mono_path, agent = False)
    file_obj.client_inference_score = client_inference_engine.InferenceEngine(config, file_obj.text_sentiments_count_client)
    
    # for agent inference
    agent_inference_engine = Inference(file_obj.agent_tone_stars, file_obj.scores_agent_emotion, file_obj.textagentstars, file_obj.audiofile_mono_path)
    file_obj.agent_inference_score, file_obj.holding_time, file_obj.hold_time_stars = agent_inference_engine.InferenceEngine(config, file_obj.text_sentiments_count_agent, 
                                                                            file_obj.holding_time, file_obj.language_stars, file_obj.agent_accent_stars, file_obj.client_inference_score)
    file_obj.inference_execution_time = time.time() - start_time

    # storing the value of holding time
    # holding time is common for both speakers

    # getting total duration of a call
    file_obj.callDuration = float("%.3f" % agent_inference_engine.Duration)
    file_obj.music_hold_time = file_obj.holding_time
    print('Inference completed for: ', file_obj.filename)

    return file_obj