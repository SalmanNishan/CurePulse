import gc
import multiprocessing

from processes.runMusicDectectron import runMusicDectectron
from processes.runDiarization import runDiarization

from fileobj import FileObject
from pymongo import MongoClient
from tqdm import tqdm

class Stages:
    def __init__(self, config, call_fetcher, storage_manager, filenames, queues):

        self.s1 = multiprocessing.Process(target = self.init_stage, args=(config, filenames, runMusicDectectron, call_fetcher.required_date, storage_manager, queues.p1_queue_1_conn,))
        self.s2 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runDiarization, queues.p2_queue_1_conn, call_fetcher))

    def init_stage(self, config, filenames, function, required_date, storage_manager, output_queue):
        for filename in filenames:
            client = MongoClient(config.mongo_url)

            # Select the desired database and collection
            db = client['CurePulse']
            collection = db['Diarized_Calls_GotoAPI']

            # Define a query to search for specific data
            query = {"filename": filename}

            # Check if data exists by searching for the query
            result = collection.find_one(query)

            # Check if result is not None to determine if data exists or not
            if result is not None:
                print("Data exists")
            else:
                try:
                    file_obj = FileObject()
                    file_obj = function(config, file_obj, filename, required_date, storage_manager)
                    
                    if file_obj != None:
                        output_queue.send(file_obj)
                
                except:
                    client = MongoClient(config.mongo_url)
                    mydb = client['CurePulse']
                    mycollection = mydb['Diarized_Calls_GotoAPI']
                    data = {
                        "date": required_date,
                        "filename": filename,

                    }
                    mycollection.insert_one(data)
                    client.close()
                    continue
        ## Send packet (-1) to indicate that the process has finished 
        file_obj = FileObject()
        file_obj.filename = -1
        output_queue.send(file_obj)
        gc.collect()

    def generic_stage(self, config, filenames, function, input_queue, call_fetcher = None):
        for filename in tqdm(filenames):
            try:
                file_obj = input_queue.recv()

                if file_obj.filename == -1:
                    break

                file_obj = function(config, file_obj, call_fetcher)

            
            except:
                client = MongoClient(config.mongo_url)
                mydb = client['CurePulse']
                mycollection = mydb['Diarized_Calls_GotoAPI']
                data = {
                    "date": call_fetcher.required_date,
                    "filename": filename,

                }
                mycollection.insert_one(data)
                client.close()
                continue
        ## Send packet (-1) to indicate that the process has finished 
        file_obj = FileObject()
        file_obj.filename = -1
        # output_queue.send(file_obj)
        gc.collect()

