import pandas as pd

path = '/home/cmdadmin/Data Ambient Intelligence/CSV Database/'

def client_data(client_numbers):
    print('Formatting client curepulse data')
    new_client_numbers = []
    for original_number in client_numbers:

        number = str(original_number)

        if len(number) == 10:
            new_client_numbers.append('+1' + number)
        elif len(number) == 11:
            new_client_numbers.append('+' + number)
        elif len(number) == 12:
            new_client_numbers.append(number)
        elif len(number) == 15:
            new_client_numbers.append(number[4:])
        else:
            print('Error in format for number: ', number)
            new_client_numbers.append(number)
    
    print('')
    return new_client_numbers

def dict_maker(numbers, names):
    mydict = {}
    for i in range(len(numbers)):
        mydict[numbers[i]] = names[i]
    return mydict

if __name__ == '__main__':
    df = pd.read_csv(path + 'curepulse_data.csv')
    client_IDs = client_data(df['Client_Name'].values)

    df2 = pd.read_csv(path + 'client_mappings_curepulse.csv')
    client_numbers = client_data(df2['Client Numbers'].values)
    client_names = df2['Client Names'].values

    client_mappings = dict_maker(client_numbers, client_names)
    
    scaled_client_names = []
    count = 0
    for ID in client_IDs:
        if ID in client_mappings:
            scaled_client_names.append(client_mappings[ID])
            count += 1
        else:
            scaled_client_names.append('Unknown')
    
    df['Client_Name'] = scaled_client_names
    df['Client_IDs'] = client_IDs

    df = df.drop(columns=['Unnamed: 0'])

    df.to_csv(path + 'curepulse_data.csv')

    print(df.head())

    print('')
    print('Total Matches: ', count)
    print('Total Overlap %', round(100*(count/len(client_IDs)), 1))