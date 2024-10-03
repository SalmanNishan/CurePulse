import os
import json

text_tasks = []

ROOT_DIR = "/home/cmdadmin/Data Ambient Intelligence/Auto Transcribed Files"

id = 1
for dir in os.listdir(ROOT_DIR):
    for file in os.listdir(os.path.join(ROOT_DIR,dir)):
        format = file.split(".")[-1]
        if format == "txt":
            file_path = os.path.join(ROOT_DIR,dir,file)
            with open(file_path, 'r') as f:
                content = f.readlines()
                content = list(map(str.strip , content) )
                content = [sentence for sentence in content if sentence != '']
                content = [sentence for sentence in content if sentence[0] != '#' and sentence[0] !='$']
                content = ' '.join(content)
                task = {"id" : id ,  "data" : { "call_id" : file, "text" : content  } }
                text_tasks.append(task)
                f.close()
                break
    id += 1
                

with open("text_sentiment_tasks.json" , 'w') as f:
    
    f.write(json.dumps(text_tasks))
    f.close()