import time

from processes.Models.Explainability_file import Explainability

def runExplanationEngine(config, file_obj, call_fetcher):
    '''
    Outputs the reason in the form of text sentences for the predicted tone sentiment
    Input: Tone Predictions, Transcriptions
    Output: Text files with sentences
    '''

    print('Running ExplanationEngine for: ', file_obj.filename)

    # getting explaination of agent and client emotions
    ClientExplainable = Explainability()
    AgentExplainable = Explainability()

    start_time = time.time()

    file_obj.agent_text_reason = AgentExplainable.textExplanation(file_obj.AgentTextList, file_obj.text_sentiments_list_agent, file_obj.textagentstars)
    file_obj.client_text_reason = ClientExplainable.textExplanation(file_obj.ClientTextList, file_obj.text_sentiments_list_client, file_obj.textclientstars)

    file_obj.agent_reason_path, file_obj.agent_tone_reason = AgentExplainable.toneExplanation("Agent", file_obj.agent_music_removed, file_obj.pred_probabilitites_agent, file_obj.agent_tone_stars, file_obj.filename, config.directory)
    file_obj.client_reason_path, file_obj.client_tone_reason = ClientExplainable.toneExplanation("Client", file_obj.client_music_removed, file_obj.pred_probabilitites_client, file_obj.client_tone_stars, file_obj.filename, config.directory)

    file_obj.tone_explaination_execution_time = time.time() - start_time

    print('ExplanationEngine completed for: ', file_obj.filename)

    return file_obj