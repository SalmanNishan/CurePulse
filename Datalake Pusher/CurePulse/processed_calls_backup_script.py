# import os
# import datetime
# import shutil

# def extract_number(file_name):
#     num_part = "".join(c for c in file_name if c.isdigit())
#     num_part = int(num_part[:10])
#     return num_part

# utc_time = 1714503598
# start_time = datetime.datetime.utcfromtimestamp(utc_time)

# total_files = 0  # initialize counter variable
# file_count_remain = 0  # initialize counter variable
# file_count_move = 0  # initialize counter variable

# source_directory = '/mnt/CurePulse_Audio_Data/Reasons'
# destination_directory =  '/media/cmdadmin/Backup/CurePulse_Processed_Calls_Backup/CurePulse_Process_Audio_Data_Backup_2024-04-30/Reasons'

# for file_name in os.listdir(source_directory):
#     total_files += 1
#     file_path = os.path.join(source_directory, file_name)
#     creation_time = datetime.datetime.utcfromtimestamp(os.path.getctime(file_path))
#     num_value = extract_number(file_name)
#     print(file_name)
#     print(num_value)

#     try:
#         if creation_time <= start_time or num_value <= utc_time:
#         # if num_value <= utc_time:
#             # Move the file to a separate folder
#             shutil.move(file_path, destination_directory)
#             print(file_name)
#             file_count_move += 1  # increment counter variable
        
#         else:
#             # Do something with the file
#             file_count_remain += 1  # increment counter variable
#     except:
#         pass

# print(f'Total files: {total_files}')  # print total count of files
# print(f'Total files count remain: {file_count_remain}')  # print total count of files
# print(f'Total files count moved: {file_count_move}')  # print total count of files






# import os
# import datetime
# import shutil

# def extract_number(file_name):
#     num_part = "".join(c for c in file_name if c.isdigit())
#     num_part = int(num_part[:10])
#     return num_part

# utc_time = 1704049200
# start_time = datetime.datetime.utcfromtimestamp(utc_time)

# total_files = 0  # initialize counter variable
# file_count_remain = 0  # initialize counter variable
# file_count_move = 0  # initialize counter variable

# source_directory = '/mnt/CurePulse_Audio_Data/Diarized_Segments'
# destination_directory =  '/media/cmdadmin/Backup/CurePulse_Processed_Calls_Backup/CurePulse_Process_Audio_Data_Backup_2023-09-30/Diarized_Segments'

# for file_name in os.listdir(source_directory):
#     total_files += 1
#     file_path = os.path.join(source_directory, file_name)
#     creation_time = datetime.datetime.utcfromtimestamp(os.path.getctime(file_path))
#     num_value = extract_number(file_name)
#     print(file_name)
#     print(num_value)

#     try:
#         if creation_time <= start_time or num_value <= utc_time:
#         # if num_value <= utc_time:
#             # Move the file to a separate folder
#             shutil.move(file_path, destination_directory)
#             print(file_name)
#             file_count_move += 1  # increment counter variable
        
#         else:
#             # Do something with the file
#             file_count_remain += 1  # increment counter variable
#     except:
#         pass

# print(f'Total files: {total_files}')  # print total count of files
# print(f'Total files count remain: {file_count_remain}')  # print total count of files
# print(f'Total files count moved: {file_count_move}')  # print total count of files






import os
import datetime
import shutil

def extract_number(file_name):
    num_part = "".join(c for c in file_name if c.isdigit())
    num_part = int(num_part[:10])
    return num_part

utc_time = 1714503598
start_time = datetime.datetime.utcfromtimestamp(utc_time)

total_files = 0  # initialize counter variable
file_count_remain = 0  # initialize counter variable
file_count_move = 0  # initialize counter variable

source_directory = '/mnt/CurePulse_Audio_Data/Original'
destination_directory =  '/media/cmdadmin/Backup/CurePulse_Processed_Calls_Backup/CurePulse_Process_Audio_Data_Backup_2024-04-30/Original'

for file_name in os.listdir(source_directory):
    total_files += 1
    file_path = os.path.join(source_directory, file_name)
    creation_time = datetime.datetime.utcfromtimestamp(os.path.getctime(file_path))
    num_value = extract_number(file_name)
    print(file_name)
    print(num_value)

    try:
        if creation_time <= start_time or num_value <= utc_time:
        # if num_value <= utc_time:
            # Move the file to a separate folder
            shutil.move(file_path, destination_directory)
            print(file_name)
            file_count_move += 1  # increment counter variable
        
        else:
            # Do something with the file
            file_count_remain += 1  # increment counter variable
    except:
        pass

print(f'Total files: {total_files}')  # print total count of files
print(f'Total files count remain: {file_count_remain}')  # print total count of files
print(f'Total files count moved: {file_count_move}')  # print total count of files