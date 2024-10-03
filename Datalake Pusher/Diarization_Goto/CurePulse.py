import gc

from Queue import Queue
from Stages import Stages
from Config import Configuration
from StorageManager import StorageManager
from CallsFetcher import CallFetcher

import soundfile as sf

class CurePulse:
    def __init__(self, date):

       # Config Files
        self.config_filepath = '/home/cmdadmin/Datalake Pusher/Diarization_Goto/config/Config_file.ini'
        self.config = Configuration(self.config_filepath)
        self.config.loadConfiguration()
        self.date = date

    def start(self):

        ### Create instances of classes
        self.call_fetcher = CallFetcher(self.config, self.date)
        self.storage_manager = StorageManager(self.config)
        self.queues = Queue()
        
        ### Get Files
        # filenames = self.call_fetcher.process_goto_files()
        filenames = self.call_fetcher.process_downloaded_files()
        
        filenames = list(set(filenames))
        new_list = []
        for filename in filenames:
            try:
                y, sr = sf.read(f"{self.config.base}/{self.call_fetcher.required_date}/{filename}")
            except:
                continue
            new_list.append((filename, len(y)/sr))
        new_list = sorted(new_list, key=lambda x: x[1], reverse=False)
        filenames = [filename for filename, _ in new_list]

        ### Create instance of stages class
        self.stages = Stages(self.config, self.call_fetcher, self.storage_manager, filenames, self.queues)

        if len(filenames) > 0:

            gc.enable()   ## Enables automatic garbage collection

            ### Start Processes
            self.stages.s1.start()
            self.stages.s2.start()

            ## Wait until process terminates
            self.stages.s1.join(timeout=600)
            self.stages.s2.join(timeout=600)

            self.stages.s1.terminate()
            self.stages.s2.terminate()

    def end(self):
        
        ### Remove extra files created
        self.storage_manager.files_cleaner()

        ### Close Mongo Connection
        self.storage_manager.client.close()
        print('client closed')

        # Delete instances (clear memory space)

        del self.queues
        del self.stages
        del self.call_fetcher
        del self.storage_manager

        ## Clean garbage values
        print("Total Objects : ", len(gc.get_objects()))
        print("Flushed : ", gc.collect())

        print('Call processing ended')