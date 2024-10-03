import gc
import datetime

from Queue import Queue
from Stages import Stages
from Config import Configuration
from StorageManager import StorageManager
from CallsFetcher import CallFetcher
import time
import os
from sqlalchemy import create_engine
from pymongo import MongoClient
from processes.Models.utils import Utils
from datetime import datetime
import soundfile as sf
from PMO_Dashboard import main as pmo_main


class CurePulse:
    def __init__(self):

       # Config Files
        self.config_filepath = '/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini'
        self.config = Configuration(self.config_filepath)
        self.config.loadConfiguration()

    def start(self):

        with open("/home/cmdadmin/Datalake Pusher/CurePulse/time_check.txt", "a") as f:
            f.writelines('Start time: {}'.format(datetime.now()))
            f.writelines('\n')

        ### Create instances of classes
        self.call_fetcher = CallFetcher(self.config)
        self.storage_manager = StorageManager(self.config)
        self.queues = Queue()

        conn_string = 'postgres://curepulseadmin:Saluteryjanisar0!#@172.16.101.152/curepulse_data_source'
        db = create_engine(conn_string)
        self.postgresconn = db.connect()
        
        ### Get Files
        current_time = time.time() # Get current time in seconds since epoch
        current_time_s = time.localtime(current_time) # Convert to local time struct
        seconds_since_midnight = current_time_s.tm_hour * 3600 + current_time_s.tm_min * 60 + current_time_s.tm_sec # Calculate seconds since midnight

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
       
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

        with open("/home/cmdadmin/Datalake Pusher/CurePulse/time_check.txt", "a") as f:
            f.writelines('Number of calls: {}'.format(len(filenames)))
            f.writelines('\n')

        ### Create instance of stages class
        self.stages = Stages(self.config, self.call_fetcher, self.storage_manager, filenames, self.queues, self.postgresconn)

        if len(filenames) > 0:

            gc.enable()   ## Enables automatic garbage collection

            ### Start Processes
            self.stages.s1.start()
            self.stages.s2.start()
            self.stages.s3.start()
            self.stages.s4.start()
            self.stages.s5.start()
            self.stages.s6.start()
            self.stages.s7.start()
            self.stages.s8.start()
            self.stages.s9.start()
            self.stages.s10.start()

            ## Wait until process terminates
            self.stages.s1.join()
            self.stages.s2.join()
            self.stages.s3.join()
            self.stages.s4.join()
            self.stages.s5.join()
            self.stages.s6.join()
            self.stages.s7.join()
            self.stages.s8.join()
            self.stages.s9.join()
            self.stages.s10.join()

            self.stages.s1.terminate()
            self.stages.s2.terminate()
            self.stages.s3.terminate()
            self.stages.s4.terminate()
            self.stages.s5.terminate()
            self.stages.s6.terminate()
            self.stages.s7.terminate()
            self.stages.s8.terminate()
            self.stages.s9.terminate()
            self.stages.s10.terminate()

    def end(self):
        
        ### Remove extra files created
        self.storage_manager.files_cleaner()

        ### Close Mongo Connection
        self.storage_manager.client.close()
        print('client closed')

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        date_obj = datetime.strptime(self.call_fetcher.required_date, '%Y-%m-%d')
        weekday = date_obj.weekday()
        if (current_time >= '20:00:00' or current_time <= "06:00:00") and weekday < 5:
            client = MongoClient(self.config.mongo_url)
            db = client['CurePulse']                          
            collection = db['CurePulse_Processed_Calls']

            pipeline = [
                {
                    '$match': {
                        'Date': self.call_fetcher.required_date
                    }
                },
                {
                    '$project': {
                        '_id': 0, 
                        'Filename': 1,
                    }
                }
            ]
            result = list(collection.aggregate(pipeline))
            # subject = f'CurePulse Processed Calls [{self.call_fetcher.required_date}]'
            # email_from = 'CurePulse Alerts <curepulsealerts@curemd.com>'
            # email_to = "syed.obaid@curemd.com, salman.nishan@curemd.com"
            # message = f"""
            #         Hi,
            #         <br><br>
            #         Please find attached the CurePulse Processed Calls count for the date {self.call_fetcher.required_date}.
            #         <br>
            #         Calls Count: {len(result)}
            #         <br><br>
            #         Regards,
            #         <br>
            #         CurePulse Admins
            #         <br><br>

            #         <u><b>Note: This is an automated email. Do not reply on this.</b></u>
            # """
            # html_text = '<html><body><p>' + message + '</p></body></html>'
            # Utils.send_email(subject, email_from, email_to, html_text, 'Low')
        
        pmo_main()
        # Delete instances (clear memory space)

        del self.queues
        del self.stages
        del self.call_fetcher
        del self.storage_manager

        ## Clean garbage values
        print("Total Objects : ", len(gc.get_objects()))
        print("Flushed : ", gc.collect())

        print('Call processing ended')

        with open("/home/cmdadmin/Datalake Pusher/CurePulse/time_check.txt", "a") as f:
            f.writelines('End time: {}'.format(datetime.now()))
            f.writelines('\n')
            f.writelines('\n')

    def __get_agent_names(self, usernames):
        names = []
        for user in usernames:
            first_name, last_name = user.split('.')
            first_name = first_name.capitalize()
            last_name = last_name.capitalize()
            names.append(first_name + ' ' + last_name)
        return names