import copy 
import time

def mapTimes(timestamps, musicTimes):
    copy_timestapms = copy.deepcopy(timestamps)
    for time in musicTimes:
        for index in range(len(copy_timestapms)):
            start_time = copy_timestapms[index][0]
            if start_time > time:
                copy_timestapms[index][0] += 4
    return copy_timestapms

def time_converter(timestamps):
    copy_timestamps = copy.deepcopy(timestamps)
    for index in range(len(timestamps)):
        copy_timestamps[index][0] = time.strftime('%M:%S', time.gmtime( timestamps[index][0] ))
        copy_timestamps[index][1] = time.strftime('%M:%S', time.gmtime( timestamps[index][0] + timestamps[index][1]) )
    return copy_timestamps

def writeTextFile(punctuated_list, filename):
    filepath = filename + '.txt'
    chars_line = 100
    with open(filepath, 'w') as file:
        file.write("")
    with open(filepath, 'a') as file:
        for line in punctuated_list:
            file.write(line['Speaker'] + " ")
            file.write('(' + line['Time'][0] + " - " + line['Time'][1] + ') : ')
            if len(line['Text']) > chars_line:
                rem_len = len(line['Text'])
                count = 1
                last_period = 0
                while rem_len > chars_line:
                    period = line['Text'].find('.', chars_line * count)
                    if count == 1:
                        file.write(line['Text'][ :period + 1] + '\n')
                    else:
                        file.write(' ' * 24 + line['Text'][last_period + 1: period + 1] + '\n \n')
                    rem_len -= len(line['Text'][last_period  : period + 1])
                    last_period = period
                    count += 1
                file.write('\n')

            else:
                file.write(line['Text'] + '\n \n')

def getMusicTimes(music_text_path):
    with open(music_text_path, 'r') as file:
        text = file.readlines()
    musicTimes = []
    for index in range(len(text)):
        music_prob = float(text[index].split('[[')[1].split(' ')[0])
        if  music_prob > 0.5:
            musicTimes.append(index*4)
    return musicTimes