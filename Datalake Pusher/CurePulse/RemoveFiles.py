import os

def delete_files(directory, pattern):
    for filename in os.listdir(directory):
        if filename == "Client_textmodel.txt" or filename == "Agent_textmodel.txt":
            pass
        elif pattern in filename:
            file_path = os.path.join(directory, filename)
            os.remove(file_path)

directory = "/home/cmdadmin/Datalake Pusher/CurePulse/"


#remove agent files
pattern = 'Agent_'
delete_files(directory, pattern)

#remove client files
pattern = 'Client_'
delete_files(directory, pattern)

#remove resampled files
pattern = 'resampled'
delete_files(directory, pattern)

pattern = 'agent_'
delete_files(directory, pattern)