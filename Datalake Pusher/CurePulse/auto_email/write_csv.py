import csv
import time
import itertools
from openpyxl import Workbook
import os

class CSVWriter:
    def __init__(self, date):
       self.date = date
       self.current_time = str(time.time()).split('.')[0]
       self.file_ = open(f'./csv_files/{date}_{self.current_time}.csv', 'a', newline='', encoding='utf-8')
       self.file_name = self.file_.name
       self.writer = csv.writer(self.file_)

    def write_calls_count(self, outgoing_voip, incoming_voip, outgoing_processed, incoming_processed):
        self.writer.writerow(["Outgoing Calls", "Total Calls", "Processed Calls", "Percentage", "", "", 
                              "Incoming Calls", "Total Calls", "Processed Calls", "Percentage"])
        self.writer.writerow(["> 15 min", outgoing_voip[-1], outgoing_processed[-1], f"{str(round((0 if outgoing_voip[-1] == 0 else min(outgoing_processed[-1]/outgoing_voip[-1], 1))*100))}%", "", "", 
                              "> 15 min", incoming_voip[-1], incoming_processed[-1], f"{str(round((0 if incoming_voip[-1] == 0 else min(incoming_processed[-1]/incoming_voip[-1], 1))*100))}%"])
        self.writer.writerow(["10 - 15 min", outgoing_voip[-2], outgoing_processed[-2], f"{str(round((0 if outgoing_voip[-2] == 0 else min(outgoing_processed[-2]/outgoing_voip[-2], 1))*100))}%", "", "", 
                              "10 - 15 min", incoming_voip[-2], incoming_processed[-2], f"{str(round((0 if incoming_voip[-2] == 0 else min(incoming_processed[-2]/incoming_voip[-2], 1))*100))}%"])
        self.writer.writerow(["5 - 10 min", outgoing_voip[-3], outgoing_processed[-3], f"{str(round((0 if outgoing_voip[-3] == 0 else min(outgoing_processed[-3]/outgoing_voip[-3], 1))*100))}%", "", "", 
                              "5 - 10 min", incoming_voip[-3], incoming_processed[-3], f"{str(round((0 if incoming_voip[-3] == 0 else min(incoming_processed[-3]/incoming_voip[-3], 1))*100))}%"])
        self.writer.writerow(["4 - 5 min", outgoing_voip[-4], outgoing_processed[-4], f"{str(round((0 if outgoing_voip[-4] == 0 else min(outgoing_processed[-4]/outgoing_voip[-4], 1))*100))}%", "", "", 
                              "4 - 5 min", incoming_voip[-4], incoming_processed[-4], f"{str(round((0 if incoming_voip[-4] == 0 else min(incoming_processed[-4]/incoming_voip[-4], 1))*100))}%"])
        self.writer.writerow(["3 - 4 min", outgoing_voip[-5], outgoing_processed[-5], f"{str(round((0 if outgoing_voip[-5] == 0 else min(outgoing_processed[-5]/outgoing_voip[-5], 1))*100))}%", "", "", 
                              "3 - 4 min", incoming_voip[-5], incoming_processed[-5], f"{str(round((0 if incoming_voip[-5] == 0 else min(incoming_processed[-5]/incoming_voip[-5], 1))*100))}%"])
        self.writer.writerow(["2 - 3 min", outgoing_voip[-6], outgoing_processed[-6], f"{str(round((0 if outgoing_voip[-6] == 0 else min(outgoing_processed[-6]/outgoing_voip[-6], 1))*100))}%", "", "", 
                              "2 - 3 min", incoming_voip[-6], incoming_processed[-6], f"{str(round((0 if incoming_voip[-6] == 0 else min(incoming_processed[-6]/incoming_voip[-6], 1))*100))}%"])
        self.writer.writerow(["1 - 2 min", outgoing_voip[-7], outgoing_processed[-7], f"{str(round((0 if outgoing_voip[-7] == 0 else min(outgoing_processed[-7]/outgoing_voip[-7], 1))*100))}%", "", "", 
                              "1 - 2 min", incoming_voip[-7], incoming_processed[-7], f"{str(round((0 if incoming_voip[-7] == 0 else min(incoming_processed[-7]/incoming_voip[-7], 1))*100))}%"])
        self.writer.writerow(["< 1 min", outgoing_voip[-8], outgoing_processed[-8], f"{str(round((0 if outgoing_voip[-8] == 0 else min(outgoing_processed[-8]/outgoing_voip[-8], 1))*100))}%", "", "", 
                              "< 1 min", incoming_voip[-8], incoming_processed[-8], f"{str(round((0 if incoming_voip[-8] == 0 else min(incoming_processed[-8]/incoming_voip[-8], 1))*100))}%"])
        self.writer.writerow("")

    def write_first_rows(self, total_calls, calls_made, outgoing_calls, incoming_calls, total_time, agent_score, client_score,
                         agent_positive, agent_negative, agent_neutral, client_positive, client_negative, client_neutral):         
        self.writer.writerow(["Calls Made", "Processed Calls", "Outgoing", "Incoming", "Time", "Agent Score", "Client Score",
                              "Agent Positive", "Agent Negative", "Agent Neutral", "Client Positive", "Client Negative", "Client Neutral"])
        self.writer.writerow([calls_made, total_calls, outgoing_calls, incoming_calls, total_time, agent_score, client_score,
                              agent_positive, agent_negative, agent_neutral, client_positive, client_negative, client_neutral])
        self.writer.writerow("")

    def write_second_row(self, total_agents, average_calls_per_agent, average_talktime_per_agent):         
        self.writer.writerow(["Total Agents", "Average Calls per Agent", "Average Talk Time per Agent"])
        self.writer.writerow([total_agents, average_calls_per_agent, average_talktime_per_agent])
        self.writer.writerow("")

    def write_5_agents(self, top_5_agents, bottom_5_agents):
        if not top_5_agents and not bottom_5_agents:
            agents = ['']
        else:
            top_5_agents = [[d["Agent Name"], f"{d['Score']:.2f}%"] for d in top_5_agents] 
            bottom_5_agents = [[d["Agent Name"], f"{d['Score']:.2f}%"] for d in bottom_5_agents] 
            agents = []
            for row1, row2 in itertools.zip_longest(top_5_agents, bottom_5_agents, fillvalue=[]):
                new_row = []
                new_row.extend(row1)
                new_row.extend(row2)
                agents.append(new_row)
        self.writer.writerow(['Top 5 Agents', ' ', 'Bottom 5 Agents'])
        self.writer.writerows(agents)
        self.writer.writerow("")

    def write_5_cleints(self, top_5_clients, bottom_5_clients):
        if not top_5_clients and not bottom_5_clients:
            clients = ['']
        else:   
            top_5_clients = [[d["Client Name"], f"{d['Score']:.2f}%"] for d in top_5_clients] 
            bottom_5_clients = [[d["Client Name"], f"{d['Score']:.2f}%"] for d in bottom_5_clients]
            clients = []
            for row1, row2 in itertools.zip_longest(top_5_clients, bottom_5_clients, fillvalue=[]):
                new_row = []
                new_row.extend(row1)
                new_row.extend(row2)
                clients.append(new_row)
        self.writer.writerow(['Top 5 Clients', ' ', 'Bottom 5 Clients'])
        self.writer.writerows(clients)
        self.writer.writerow("")

    def write_all_data(self, data):
        self.writer.writerow("")
        try:
            self.writer.writerow(data.columns) 
            for _, row in data.iterrows():
                self.writer.writerow(row)   
        except:
            self.writer.writerow(data)
    
    def close_file(self):
        self.file_.close()

    def csv_to_excel(self):
        data = []
        with open(self.file_name, 'r', newline='') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                data.append(row)

        wb = Workbook()
        sheet = wb.active
        for row in data:
            sheet.append(row)
        excel_filename = os.path.splitext(self.file_name)[0] + '.xlsx'
        wb.save(excel_filename)

        return excel_filename
