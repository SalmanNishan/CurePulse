import time
import gc

from processes.Models.transcription import Transcription

# Process 5
def runPunctuation(config, file_obj, call_fetcher):
    '''
    Adds punctuation to the transcriptions
    Input: Transcriptions
    Output: Punctuated transcriptions
    '''

    Transcript = Transcription()

    print('Running Punctuation for: ', file_obj.filename)

    start_time = time.time()

    ## Get punctuations
    file_obj.punctuated_list = Transcript.punctuate(file_obj.transcriptions, file_obj.filename, 
                                                config.transcriptions_dir, config.store_transcriptions)
    file_obj.punctuation_execution_time = time.time() - start_time


    gc.collect()
    print('Punctuation completed for: ', file_obj.filename)

    return file_obj