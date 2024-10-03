from pymongo import MongoClient
import json
import pandas as pd
from datetime import date, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

from data import CurePulseData

url = "mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1"
db = 'CurePulse'
collection = 'Exception_Calls'

client = MongoClient(url)
mydb = client[db]
mycollection = mydb[collection]

voip_data_path = "/home/cmdadmin/Datalake Pusher/CurePulse/VOIP_data"
required_date = str(date.today() - timedelta(days=1))
# required_date = '2024-01-04'

def original_data(call_flag, ext, client_no):
    try:
        call_list = []
        file_name = f"{voip_data_path}/{call_flag}_{required_date}.json"
        with open(file_name, 'r') as file:
            data = json.load(file)
        for call in data['data']:
            call_id = call["call_id"]
            call_duration = call["talk_time"]
            agent_name = call["agent_name"]
            extension = call[ext]
            client_number = call[client_no]

            call_list.append({
                "Filename": call_id + '.wav',
                "Call_Duration": int(call_duration),
                "Agent_Name": agent_name,
                "Extension": extension,
                "Call to/from": client_number
            })
        original_data = pd.DataFrame(call_list)
    except:
        original_data = pd.DataFrame([])
    return original_data

outgoing_data = original_data('outgoing', 'call_from', "call_to")
incoming_data = original_data('incoming', 'call_to', "call_from")

mongo_query = {'Call Date': required_date}

result = mycollection.find(mongo_query)

data_list = []

for res in result:
    try:
        if res["Filename"] in outgoing_data["Filename"].values:
            call_data = outgoing_data[outgoing_data["Filename"] == res["Filename"]].iloc[0]
            call_data["Call Type"] = 'Outgoing'
        elif res["Filename"] in incoming_data["Filename"].values:
            call_data = incoming_data[incoming_data["Filename"] == res["Filename"]].iloc[0]
            call_data["Call Type"] = 'Incoming'
        call_data = call_data.to_dict()
        if res["Function"] == "runMusicDectectron":
            call_data["Reason"] = "Input Signal Length Too Small"
        elif res["Function"] == "pushDB":
            call_data["Reason"] = "Transferred Call (Agent Name Empty)"
        else:
            continue
        data_list.append(call_data)
    except:
        pass
df = pd.DataFrame(data_list)
df = df.sort_values(by="Call_Duration")
filename = f'exception_calls/{required_date}_Exception_Calls.csv'
df.to_csv(filename, index=False)

url = "mongodb://curepulse_admin:Cure123pulse!*@172.16.101.152:27017/CurePulse?authMechanism=SCRAM-SHA-1"
db = 'CurePulse'
collection = 'CurePulse_Processed_Calls'
CURE_PULSE_DATA = CurePulseData(url, db, collection, "CS")
_, processed_count_cs = CURE_PULSE_DATA.get_all_data()
CURE_PULSE_DATA = CurePulseData(url, db, collection, "Sales")
_, processed_count_sales = CURE_PULSE_DATA.get_all_data()
processed_count = processed_count_cs + processed_count_sales
less_30_count = (len(outgoing_data)+len(incoming_data)) - len(df) - processed_count

df_stats = pd.DataFrame([{"Total Calls": (len(outgoing_data)+len(incoming_data)),
                          "Processed Calls": processed_count,
                          "Agent/Client Less Than 30 Seconds" : less_30_count,
                        #   "Total Exception Calls": len(df), 
                          "Transferred Call (Agent Name Empty)": len(df[df["Reason"] == "Transferred Call (Agent Name Empty)"]),
                          "Input Signal Length Too Small": len(df[df["Reason"] == "Input Signal Length Too Small"])}])

style_table_1 = f"""
                table {{ 
                    border-collapse: collapse; 
                    width: 85%; 
                    margin: auto; 
                    table-layout: fixed;
                    }}
                th, td {{ 
                    text-align: center; 
                    padding: 8px; 
                    font-size: 16px; 
                    }}
                th {{ 
                    background-color: #6699CC; 
                    color: white
                    }}
                tr:nth-child(even) {{ 
                    background-color: #f2f2f2; 
                    }} 
        """
table_html_1 = '<style>{}</style>'.format(style_table_1, df_stats.to_html(index=False))

message = f"""
                Hi,
                <br><br>
                Please find attached CurePulse Exception Calls Stats for {required_date}.
                <br>
                {df_stats.to_html(index=False)}
                <br><br>
                Regards,
                <br>
                CurePulse Admins
                <br><br>

                <b>This is an automated email. Do not reply on this.</b>
            """
html = '<html><body><p>' + table_html_1 + message + '</p></body></html>'

to = "syed.obaid@curemd.com, salman.nishan@curemd.com, kashif.latif@curemd.com, muddassar.farooq@curemd.com, saqlain.nawaz@curemd.com, ali.asif@curemd.com, hamza.ansar@curemd.com, muhammad.ahsan@curemd.com"
# to = "syed.obaid@curemd.com"

mime = MIMEMultipart()
mime['Subject'] = f"Exception Calls ({required_date})"
mime['From'] = f'CurePulse Alerts <aialerts@curemd.com>'
mime['To'] = to

mime.attach(MIMEText(html, "html"))

if os.path.exists(filename):
    with open(filename, 'r', encoding='utf-8') as attachment:
        att = MIMEApplication(attachment.read())
        att.add_header('Content-Disposition','attachment', filename= os.path.basename(filename))
        mime.attach(att)

# establish SMTP connection
server = smtplib.SMTP('sendmail.curemd.com', 25)

# send the email with attachment
server.sendmail(mime['From'], mime['To'].split(','), mime.as_string())

# close the SMTP connection
server.quit()