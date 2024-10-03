import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os, time

###########################
# def send_email(date, file_name, calls_made, total_calls, scores, below_60_clients, below_60_agents, dept, to):
def send_email(date, file_name, scores, below_60_clients, below_60_agents, dept, to, calls_count):
###########################
    # create message object instance
    msg = MIMEMultipart()
    msg['Subject'] = f'CurePulse {dept} Call Stats {date}'
    msg['From'] = f'CurePulse {dept} Alerts <aialerts@curemd.com>'
    msg['To'] = to
    style_table_1 = f"""
                    table {{ 
                        border-collapse: collapse; 
                        width: 85%; 
                        margin: auto; 
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
    table_html_1 = '<style>{}</style>'.format(style_table_1, below_60_agents)
    style_table_2 = f"""
                    table {{ 
                        width: 85%; 
                        margin: auto; 
                        }}
                    th, td {{ 
                        text-align: center; 
                        }}
            """
    table_html_2 = '<style>{}</style>'.format(style_table_2, calls_count)

    # create a message body
    message2 = f"""
                <u><b>Agents with below 60% Talk Time or Quality Score:</b></u>
                <br>
                {below_60_agents}
                <br>
                <u><b>Clients with below 60% Satisfaction Score:</b></u>
                <br>
                {below_60_clients}

                <br><br>

                Regards,
                <br>
                CurePulse Admins
                <br><br>

                <b>This is an automated email. Do not reply on this.</b>
            """
    html1 = '<html><body><p>' + table_html_1 + message2 + '</p></body></html>'
    message = f"""
                Hi,
                <br><br>
                Please find attached CurePulse {dept} Call Stats for {date}.
                <br><br><div style="font-weight:bold;">
                {calls_count}
                </div>
                <br>
                <u><b>Note: Only those calls are processed in which agent and client talk time is greater than 30 seconds.</b></u>
                <br><br><div style="font-weight:bold;">
                {scores}
                </div>
                <br><br>
                {html1}
            """
    html = '<html><body><p>' + table_html_2 + message + '</p></body></html>'
    # attach message to the email


    msg.attach(MIMEText(html, "html"))

    while not os.path.exists(file_name):
        time.sleep(1)  # Adjust the delay (in seconds) between each check
    
    # open and read the file in binary mode
    if os.path.exists(file_name):
        with open(file_name, 'rb') as attachment:
            att = MIMEApplication(attachment.read(), _subtype='xlsx')
            att.add_header('Content-Disposition','attachment', filename= os.path.basename(file_name))
            msg.attach(att)

    # establish SMTP connection
    server = smtplib.SMTP('sendmail.curemd.com', 25)

    # send the email with attachment
    server.sendmail(msg['From'], msg['To'].split(','), msg.as_string())

    # close the SMTP connection
    server.quit()
