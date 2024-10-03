import gc
import multiprocessing

from processes.runMusicDectectron import runMusicDectectron
from processes.runTranscription import runTranscription
from processes.runSpeechModel import runSpeechModel
from processes.runTextModel import runTextModel
from processes.runExplanationEngine import runExplanationEngine
from processes.runAccentDetection import runAccentDetection
from processes.runInference import runInference
from processes.pushDB import pushDB
from processes.csv_gen import csv_gen
from processes.runCorpusChecker import runCorpusChecker

from fileobj import FileObject
from datetime import datetime
from processes.Models.utils import Utils
from pymongo import MongoClient

import soundfile as sf
from tqdm import tqdm


class Stages:
    def __init__(self, config, call_fetcher, storage_manager, filenames, queues, postgresconn):
        
        client = MongoClient(config.mongo_url)
        db = client['CurePulse']                          
        self.collection = db['Exception_Calls']

        self.postgresconn = postgresconn
        # create variable for every process to be run
        self.s1 = multiprocessing.Process(target = self.init_stage, args=(config, filenames, runMusicDectectron, call_fetcher.required_date, storage_manager, queues.p1_queue_1_conn,))
        self.s2 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runTranscription, queues.p2_queue_1_conn, queues.p2_queue_2_conn, call_fetcher,))
        self.s3 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runSpeechModel, queues.p3_queue_2_conn, queues.p3_queue_3_conn,))
        self.s4 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runTextModel, queues.p4_queue_3_conn, queues.p4_queue_4_conn,))
        self.s5 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runExplanationEngine, queues.p5_queue_4_conn, queues.p5_queue_5_conn,))
        self.s6 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runAccentDetection, queues.p6_queue_5_conn, queues.p6_queue_6_conn,))
        self.s7 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runInference, queues.p7_queue_6_conn, queues.p7_queue_7_conn,))
        self.s8 = multiprocessing.Process(target = self.generic_stage, args=(config, filenames, runCorpusChecker, queues.p8_queue_7_conn, queues.p8_queue_8_conn,))
        self.s9 = multiprocessing.Process(target = self.mongo_stage, args=(config, filenames, pushDB, queues.p9_queue_8_conn, queues.p9_queue_9_conn, storage_manager,))
        self.s10 = multiprocessing.Process(target = self.csv_stage, args=(config, filenames, csv_gen, call_fetcher.mycollection, call_fetcher.day_of_week, queues.p10_queue_9_conn,))

    def init_stage(self, config, filenames, function, required_date, storage_manager, output_queue):
        for filename in tqdm(filenames):
            # if filename in ['1723729942.157859.wav']:
                query = {"Filename": filename}
                # Check if data exists by searching for the query
                result = self.collection.find_one(query)
                # Check if result is None to determine if data exists or not
                if result is None:
                    try:
                        y, sr = sf.read(f"{config.base}/{required_date}/{filename}")
                    except:
                        continue
                    duration = len(y)/sr
                    if duration >= -1:
                        try:
                            file_obj = FileObject()
                            file_obj.total_duration = duration
                            file_obj = function(config, file_obj, filename, required_date, storage_manager)
                            
                            if file_obj != None:
                                output_queue.send(file_obj)
                        
                        except:
                            data = {
                                "Processing Time": str(datetime.now()),
                                "Call Date": required_date,
                                "Filename": filename,
                                "Function": str(function).split(' ')[1] 
                            }
                            self.collection.insert_one(data)
                            continue

        ## Send packet (-1) to indicate that the process has finished 
        file_obj = FileObject()
        file_obj.filename = -1
        output_queue.send(file_obj)
        gc.collect()

    # multiproccesing function for each stage
    def generic_stage(self, config, filenames, function, input_queue, output_queue, call_fetcher = None):
        for _ in filenames:
            try:
                file_obj = input_queue.recv()
                if file_obj.filename == -1:
                    break

                file_obj = function(config, file_obj, call_fetcher)
                if file_obj!= None:
                    output_queue.send(file_obj)
            
            except Exception as error:
                data = {
                        "Processing Time": str(datetime.now()),
                        "Call Date": file_obj.required_date,
                        "Filename": file_obj.filename,
                        "Function": str(function).split(' ')[1] 
                    }
                self.collection.insert_one(data)
                if 'CUDA out of memory.' in str(error):
                    msg = f'''
                        <h2 style="color:red;">WARNING!</h2>
                        <h2>CUDA OUT OF MEMORY.</h2>
                        '''
                    Utils.send_notification(msg, config.token_url, config.tokenHeader, config.msg_url)
                    break
                continue
        ## Send packet (-1) to indicate that the process has finished 
        file_obj = FileObject()
        file_obj.filename = -1
        output_queue.send(file_obj)
        gc.collect()

    # multiproccesing function to push data to superset MongoDB
    def mongo_stage(self, config, filenames, function, input_queue, output_queue, storage_manager):
        for _ in filenames:
            try:
                
                file_obj = input_queue.recv()

                if file_obj.filename == -1:
                    break

                document = function(file_obj, storage_manager, config)

                if file_obj != None:
                    output_queue.send(document)
            
            except:
                data = {
                        "Processing Time": str(datetime.now()),
                        "Call Date": file_obj.required_date,
                        "Filename": file_obj.filename,
                        "Function": str(function).split(' ')[1] 
                    }
                self.collection.insert_one(data)
                continue
        # Send packet (-1) to indicate that the process has finished 
        document = {}
        document['Filename'] = -1
        output_queue.send(document)
        gc.collect()

    # multiproccesing function to push data to superset dB
    def csv_stage(self, config, filenames, function, mycollection, day_of_week, input_queue):
        for _ in filenames:
            try:
                document = input_queue.recv()

                if document['Filename'] == -1:
                    break

                function(config, document, mycollection, day_of_week, self.postgresconn)
            
            except:
                data = {
                        "Processing Time": str(datetime.now()),
                        "Call Date": document.required_date,
                        "Filename": document.filename,
                        "Function": str(function).split(' ')[1] 
                    }
                self.collection.insert_one(data)
                continue

        gc.collect()

    # for tting, it return the error wherever it oocurs
    def generic_stage2(self, config, filenames, function, input_queue, output_queue, call_fetcher = None):
        for _ in filenames:
            file_obj = input_queue.recv()

            if file_obj.filename == -1:
                break

            if file_obj.filename == 0:
                continue

            file_obj = function(config, file_obj, call_fetcher)

            if file_obj!= None:
                output_queue.send(file_obj)
            
        ## Send packet (-1) to indicate that the process has finished 
        file_obj = FileObject()
        file_obj.filename = -1
        output_queue.send(file_obj)
        gc.collect()