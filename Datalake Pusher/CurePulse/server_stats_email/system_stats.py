import psutil
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import requests
import ast

def send_notification(msg, tokenURL, tokenHeader, msgURL):
    response = requests.post(tokenURL, data = tokenHeader)
    
    token_gen = ast.literal_eval(response.content.decode())
    
    apiHeader = {
        "Authorization": f"Bearer {token_gen['access_token']}",
        "Content-Type": "application/json",
        "Client" : "CureMD"
    }

    message = {
                "IncidentId" : "12345164",
                "IncidentMessage" : f"{msg}",
                "GroupName" : "CurePulse Service Alerts"
            }

    response = requests.post(msgURL, json=message, headers=apiHeader)
    print("Send msg x", response.content)

def convert_bytes(byte_value):
    units = ['bytes', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    while byte_value >= 1024 and unit_index < len(units) - 1:
        byte_value /= 1024
        unit_index += 1
    converted_value = round(byte_value, 2)
    return str(converted_value) + ' ' + units[unit_index]


# CPU Stats
def get_cpu_stats():
    cpu_stats = psutil.cpu_times_percent(interval=1, percpu=False)
    cpu_stats_dict = cpu_stats._asdict()
    cpu_usage = [{
        'usr': cpu_stats_dict['user'],
        'nice': cpu_stats_dict['nice'],
        'sys': cpu_stats_dict['system'],
        'iowait': cpu_stats_dict['iowait'],
        'irq': cpu_stats_dict['irq'],
        'soft': cpu_stats_dict['softirq'],
        'steal': cpu_stats_dict['steal'],
        'guest': cpu_stats_dict['guest'],
        'idle': cpu_stats_dict['idle']
    }]
    return cpu_usage


# RAM Stats
def get_ram_stats():
    ram_stats = psutil.virtual_memory()
    ram_usage = [{
        'Total': ram_stats.total,
        'Used': ram_stats.used,
        'Free': ram_stats.free,
        'Shared': ram_stats.shared,
        'Buffer': ram_stats.buffers,
        'Cached': ram_stats.cached
    }]
    return ram_usage


# Disk Stats
def get_disk_stats():
    disk_stats = psutil.disk_partitions()
    disk_usage = []
    for disk in disk_stats:
        disk_usage.append({
            'Filesystem': disk.device,
            'Size': convert_bytes(psutil.disk_usage(disk.mountpoint).total),
            'Used': convert_bytes(psutil.disk_usage(disk.mountpoint).used),
            'Available': convert_bytes(psutil.disk_usage(disk.mountpoint).free),
            'Percentage Used': psutil.disk_usage(disk.mountpoint).percent,
            'Mounted On': disk.mountpoint
        })

    disk_usage = [disk for disk in disk_usage if 'loop' not in disk['Filesystem']]
    return disk_usage

# # Render HTML template
# env = Environment(loader=FileSystemLoader('.'))
# template = env.get_template('python_scripts/template.html')
# output = template.render(nodeip='172.16.101.152', time= datetime.now().strftime("%H:%M:%S"), cpu_stats=get_cpu_stats(), 
#                          ram_stats=get_ram_stats(), disk_stats=get_disk_stats())

# # Save HTML output to a file
# with open('python_scripts/stats_output.html', 'w') as f:
#     f.write(output)

if __name__ == "__main__":
    tokenHeader = {
            "client_id" : "5c856e91-1e1d-48a2-8a7f-213caa747cf6",
            "client_secret" : "00652d35-45e9-452a-a170-a4947ccd4fd0c1",
            "grant_type" : "password",
            "username" : "apiuser",
            "password" : "51460647c0bff6ab62faf2d37e6572045e9d4d3f314cbbe375bfe47762b9cf86"
        }

    token_url = 'https://curesms-staging.curemd.com/NotificationHub/connect/token'
    msg_url = 'https://curesms-staging.curemd.com/NotificationHub/OutageAlerts/Send'
    
    disk_stats = get_disk_stats()
    for disk in disk_stats:
        if disk['Percentage Used'] > 90:
            msg = f'''
            <h2>WARNING! Disk Usage has exceeded limit.</h2>
            <table>
                <tr>
                    <th>Filesystem</th>
                    <th>Size</th>
                    <th>Used</th>
                    <th>Available</th>
                    <th>Percentage Used</th>
                    <th>Mounted On</th>
                </tr>
                <tr>
                    <td>{disk['Filesystem']}</td>
                    <td>{disk['Size']}</td>
                    <td>{disk['Used']}</td>
                    <td>{disk['Available']}</td>
                    <td>{disk['Percentage Used']}%</td>
                    <td>{disk['Mounted On']}</td>
                </tr>
            </table>'''
            # print(msg)
            send_notification(msg, token_url, tokenHeader, msg_url)

    ram_stats = get_ram_stats()[0]
    percentage_used = (ram_stats['Total'] - ram_stats['Free'])/ram_stats['Total']
    if percentage_used >= 0.95:
        msg = f'''
        <h2>WARNING! RAM Usage has exceeded limit.</h2>
        <table>
            <tr>
            <th>Total</th>
            <th>Used</th>
            <th>Free</th>
            <th>Shared</th>
            <th>Buffers</th>
            <th>Cached</th>
        </tr>
        <tr>
            <td>{convert_bytes(ram_stats['Total'])}</td>
            <td>{convert_bytes(ram_stats['Used'])}</td>
            <td>{convert_bytes(ram_stats['Free'])}</td>
            <td>{convert_bytes(ram_stats['Shared'])}</td>
            <td>{convert_bytes(ram_stats['Buffer'])}</td>
            <td>{convert_bytes(ram_stats['Cached'])}</td>
        </tr>
        </table>'''
        # print(msg)
        send_notification(msg, token_url, tokenHeader, msg_url)