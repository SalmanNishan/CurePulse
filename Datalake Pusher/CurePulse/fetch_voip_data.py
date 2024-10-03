from Config import Configuration
from CallsFetcher import CallFetcher
import requests
from processes.Models.utils import Utils
import json
import os
from tqdm import tqdm
from datetime import datetime
import pandas as pd
import csv
from pymongo import MongoClient
import pymongo

config = Configuration('/home/cmdadmin/Datalake Pusher/CurePulse/config/Config_file.ini')
config.loadConfiguration()
call_fetcher = CallFetcher(config)
voip_link_date = call_fetcher.required_date[2:]

call_type_flags = [config.outgoing, config.incoming]
# call_type_flags = [config.outgoing]

def time_to_seconds(time_str):
    hours, minutes, seconds = map(int, time_str.split(":"))
    total_seconds = ((hours * 60 + minutes) * 60 + seconds)
    return total_seconds

def get_agent_names(usernames):
    names = []
    for user in usernames:
        first_name, last_name = user.split('.')
        first_name = first_name.capitalize()
        last_name = last_name.capitalize()
        names.append(first_name + ' ' + last_name)
    return names

def url_fixer(url):
    '''
    Converts URL to useable format in Linux Server
    '''
    link = url.split('/')[3]
    new_url = config.base_url + link
    return new_url

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
if current_time >= '17:00:00' or current_time <= "11:00:00":
    for i, call_type_flag in enumerate(call_type_flags):
        response_API = requests.get(config.url + voip_link_date + call_type_flag)
        if response_API.status_code != 200:
            msg = f'''
                    <h2 style="color:red;">WARNING!</h2>
                    <h2>UNABLE TO ACCESS VOIP DATA LINK</h2>
                    <h2>Response: {response_API.status_code}</h2> 
                    '''
            Utils.send_notification(msg, config.token_url, config.tokenHeader, config.msg_url)
            continue
        data = response_API.text
        response = json.loads(data)
        if i == 0:
            with open(f'{config.voip_data_path}/outgoing_{call_fetcher.required_date}.json', 'w') as file:
                json.dump(response, file)
        else:
            with open(f'{config.voip_data_path}/incoming_{call_fetcher.required_date}.json', 'w') as file:
                json.dump(response, file)

        base_path = config.base
        target_dir = os.path.join(base_path, call_fetcher.required_date)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        already_downloaded_files_list = os.listdir(target_dir)

        list_records = response['data'] # get list of all the records
        
        for record in tqdm(list_records):
            url = url_fixer(record['recording'])  # url of the audio file
            agent_url = url_fixer(record['agent_recording'])  # url of the audio file
            client_url = url_fixer(record['client_recording'])  # url of the audio file
            
            filename = record['call_id'] + '.wav' # name of the audio file
            agent_filename = "agent_" + filename
            client_filename = "client_" + filename

            if filename not in already_downloaded_files_list:
                save_path = os.path.join(target_dir, filename)
                r = requests.get(url, allow_redirects = True) # get the audio file data
                open(save_path, 'wb').write(r.content)  # save to the desired path

                save_path = os.path.join(target_dir, agent_filename)
                r = requests.get(agent_url, allow_redirects = True) # get the audio file data
                open(save_path, 'wb').write(r.content)  # save to the desired path

                save_path = os.path.join(target_dir, client_filename)
                r = requests.get(client_url, allow_redirects = True) # get the audio file data
                open(save_path, 'wb').write(r.content)  # save to the desired path

now = datetime.now()
current_time = now.strftime("%H:%M:%S")

if current_time >= '07:00:00' and current_time <= "17:30:00":
    filenames = call_fetcher.process_downloaded_files()
    client = MongoClient(config.mongo_url)
    db = client['CurePulse']                          
    collection = db['Calls_Count']
    cs_calls = 0
    sales_calls = 0
    
    agent_files_path = '/home/cmdadmin/Data Ambient Intelligence/CSV Database'
    with open(os.path.join(agent_files_path, 'agent_names.txt'), 'r') as f:
        usernames = [name.replace('\n', '') for name in f.readlines()]
    cs_agents = get_agent_names(usernames)
    with open(os.path.join(agent_files_path, 'sales_agent_names.txt'), 'r') as f:
        usernames = [name.replace('\n', '') for name in f.readlines()]
    salesagents = get_agent_names(usernames)

    for filename in filenames:
        _, agent_name, _, _ = call_fetcher.fetch_voip_data(filename)
        if agent_name in cs_agents:
            cs_calls += 1
        elif agent_name in salesagents:
            sales_calls += 1
    result = collection.find_one({"Date": call_fetcher.required_date})
    # Check if result is None to determine if data exists or not
    if result is None:
        data = {
            "Current Time": str(datetime.now()),
            "Date": call_fetcher.required_date,
            "CS Calls Count": cs_calls,
            "Sales Calls Count": sales_calls
        }
        collection.insert_one(data)