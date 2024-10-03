import pandas as pd
from pymongo import MongoClient

from utils.supporter_functions import *

def csv_gen(config, document, mycollection, day_of_week, conn):
    '''
    Generates a DataFrame from Mongo records and appends each record it to CS, Finance or RCM CSVs depending on the record.
    Also pushes the CSV to Superset 
    Input: Document record
    Output: CSV, data pushed to Superset
    '''

    print('Uploading CSV for: ', document['Filename'])

    ## Checking in record in MongodB
    if (document['total_agent_duration'] < 30) or (document['total_client_duration'] < 30) or (document['total_duration'] < 60) or (document['Zero_time'] == 1):
        client = MongoClient(config.mongo_url)
        db = client[config.db_name]                          
        collection = db["CurePulse_Processed_Exception_Calls"]
        no_of_docs =  collection.count_documents({'Filename': document['Filename']})
    else:
        no_of_docs = mycollection.count_documents({'Filename': document['Filename']})

    ## If document not in Mongodb, skip file
    if no_of_docs != 0:
    
        ## Extracting relevant data from document dict
        doc = data_extractor(document, day_of_week)
        engg_corpus_doc = engg_teams_dict_to_df(document)

        ## Three departments, three CSVs

        if doc['Team'] == 'Finance':

            del doc['Team'] ### Delete additional information

            ### Read existing CSV
            df = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'finance_curepulse_data.csv')
            df = df.append(doc, ignore_index=True)
            df = df_cleaner(df)

            ### Append data to CSV
            df.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'finance_curepulse_data.csv')

            df.to_sql('Finance Data', conn, if_exists = 'replace', index = False)

        elif doc['Team'] == 'CS':

            del doc['Team'] ### Delete additional information

            ### Read existing CSV
            if (document['total_agent_duration'] < 30) or (document['total_client_duration'] < 30) or (document['total_duration'] < 60) or (document['Zero_time'] == 1):
                doc = pd.DataFrame([doc])
                exists = conn.execute("""SELECT 1 FROM "CurePulseDataExceptions" WHERE "File_Name" = %s LIMIT 1""", (document['Filename'],)).fetchone() is not None
                if exists:
                    conn.execute("""DELETE FROM "CurePulseDataExceptions" WHERE "File_Name" = %s""", (document['Filename'],))
                
                doc.to_sql('CurePulseDataExceptions', conn, if_exists='append', index=False)            

            else:
                try:
                    df = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'curepulse_data.csv')
                    df = df.append(doc, ignore_index=True)
                    df = df_cleaner(df)
                    df.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'curepulse_data.csv')
                except:
                    pass
                df_engineering = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'curepulse_data_engineering.csv')
                df_engineering = df_engineering.append(engg_corpus_doc, ignore_index=True)
                df_engineering = df_cleaner(df_engineering)
                df_engineering = df_engineering[(df_engineering['Engineering Corpus'].notnull()) & (df_engineering['Engineering Corpus'] != '')]

                teams_count_df = teams_count(df_engineering)
                df_teams_count = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'curepulse_data_engineering_teams_count.csv')
                df_teams_count = df_teams_count.append(teams_count_df, ignore_index=True)
                df_teams_count = df_cleaner2(df_teams_count)

                ### Append data to CSV
                df_engineering.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'curepulse_data_engineering.csv')
                df_teams_count.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'curepulse_data_engineering_teams_count.csv')

                doc = pd.DataFrame([doc])
                exists = conn.execute("""SELECT 1 FROM "CurePulseData" WHERE "File_Name" = %s LIMIT 1""", (document['Filename'],)).fetchone() is not None
                if exists:
                    conn.execute("""DELETE FROM "CurePulseData" WHERE "File_Name" = %s""", (document['Filename'],))
                
                doc.to_sql('CurePulseData', conn, if_exists='append', index=False)

                engg_corpus_doc = pd.DataFrame([engg_corpus_doc])
                exists = conn.execute("""SELECT 1 FROM "EngineerngData" WHERE "File_Name" = %s LIMIT 1""", (document['Filename'],)).fetchone() is not None
                if exists:
                    conn.execute("""DELETE FROM "EngineerngData" WHERE "File_Name" = %s""", (document['Filename'],))
                engg_corpus_doc.to_sql('EngineerngData', conn, if_exists = 'append', index = False)

                exists = conn.execute("""SELECT 1 FROM "EngineerngTeamsCountData" WHERE "File_Name" = %s LIMIT 1""", (document['Filename'],)).fetchone() is not None
                if exists:
                    conn.execute("""DELETE FROM "EngineerngTeamsCountData" WHERE "File_Name" = %s""", (document['Filename'],))
                teams_count_df.to_sql('EngineerngTeamsCountData', conn, if_exists = 'append', index = False)
                
        
        elif doc['Team'] == 'RCM':

            del doc['Team'] ### Delete additional information

            ### Read existing CSV
            df = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'rcm_curepulse_data.csv')
            df = df.append(doc, ignore_index=True)
            df = df_cleaner(df)

            ### Append data to CSV
            df.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'rcm_curepulse_data.csv')

            df.to_sql('RCM Data', conn, if_exists = 'replace', index = False)

        elif doc['Team'] == 'Sales':

            del doc['Team'] ### Delete additional information
            if (document['total_agent_duration'] < 30) or (document['total_client_duration'] < 30) or (document['total_duration'] < 60) or (document['Zero_time'] == 1):
                pass
            else:
                ### Read existing CSV
                df = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'sales_curepulse_data.csv')
                df = df.append(doc, ignore_index=True)
                df = df_cleaner(df)

                ### Append data to CSV
                df.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'sales_curepulse_data.csv')

                doc = pd.DataFrame([doc])
                exists = conn.execute("""SELECT 1 FROM "SalesData" WHERE "File_Name" = %s LIMIT 1""", (document['Filename'],)).fetchone() is not None
                if exists:
                    conn.execute("""DELETE FROM "SalesData" WHERE "File_Name" = %s""", (document['Filename'],))
                doc.to_sql('SalesData', conn, if_exists = 'append', index = False)

        elif doc['Team'] == 'India':

            del doc['Team'] ### Delete additional information
            if (document['total_agent_duration'] < 30) or (document['total_client_duration'] < 30) or (document['total_duration'] < 60) or (document['Zero_time'] == 1):
                pass
            else:
                ### Read existing CSV
                df = pd.read_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'indian_curepulse_data.csv')
                df = df.append(doc, ignore_index=True)
                df = df_cleaner(df)

                ### Append data to CSV
                df.to_csv('/home/cmdadmin/Data Ambient Intelligence/CSV Database/' + 'indian_curepulse_data.csv')

                doc = pd.DataFrame([doc])
                exists = conn.execute("""SELECT 1 FROM "IndianData" WHERE "File_Name" = %s LIMIT 1""", (document['Filename'],)).fetchone() is not None
                if exists:
                    conn.execute("""DELETE FROM "IndianData" WHERE "File_Name" = %s""", (document['Filename'],))
                doc.to_sql('IndianData', conn, if_exists = 'append', index = False)

        print('Documents have been added successfully for: ' + document['Date'] + ' / ' + document['Filename'])

    else:
        print('Documents already exist for: ' + document['Date'] + ' / ' + document['Filename'])