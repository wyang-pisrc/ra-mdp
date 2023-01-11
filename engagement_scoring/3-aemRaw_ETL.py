# Connect using pyodbc, sqlalchemy, and pandas
# import numpy as np
# import pandas as pd
import sqlalchemy
import getpass
import pandas as pd

from contentScoreShareUtils import datetime_interval_iterator
from DataLoader import Query_DataLoader
from preprocessingUtils import aem_raw_preprocessing
from Analyzer import Analyzer
import configparser
import json
import gzip

if __name__ == '__main__':

    config = configparser.ConfigParser()   
    config.read('config.txt')
    password = getpass.getpass('Enter database password: ')
    engine = sqlalchemy.create_engine(f"mssql+pyodbc://{config['mssql']['username']}:{password}@{config['mssql']['server']}/{config['mssql']['database']}?driver={config['mssql']['driver']}")
    session = engine.connect()
    
    ## data store + label assign
    dataLoader = Query_DataLoader(engine, config['ETL'].getboolean('is_preload'), config['data-export']['path'])
    dataLoader.save_elq_bridge()
    updated_labels, valid_mcvisid, drop_mcvisid = dataLoader.load_updated_labels() # including data storing and label assign

    ## start processing by batch
    logs = []
    batch_size = config['ETL'].getint('batch_size')
    print("Start processing...")
    for (start_month, start_day, end_month, end_day) in datetime_interval_iterator(event_start_month=config['ETL'].getint('BINGE_START_MONTH'), event_end_month=config['ETL'].getint('BINGE_END_MONTH')):
        idx = 0
        filename = f"aemRaw_keyColumns_2022{start_month}{start_day}-2022{end_month}{end_day}_{config['ETL']['parse_version']}{config['ETL']['summary_version']}.csv" + ".gz"
    
        print("    Loading from SQL Server... ")
        query_string = dataLoader.generate_aemRaw_query(start_month, end_month, start_day, end_day)
        cursor = session.execute(query_string)
        columns = [col for col in cursor.keys()]
        while True:
            idx +=1 
            aem_raw = pd.DataFrame(cursor.fetchmany(batch_size), columns=columns, dtype=str) # out.writerows(cursor.fetchall()) # high RAM consume
            if aem_raw.shape[0] == 0:
                print("    End for this month")
                break
            
            aem_raw, log = aem_raw_preprocessing(aem_raw, valid_mcvisid, drop_mcvisid) # clean up irrelevant rows
            print(f"        {idx}: row {(idx-1)*batch_size} - row {idx * batch_size}, finish loading/processing from SQL Server, keep {aem_raw.shape[0]} rows")
            isRefresh = (idx == 1)
            aem_raw.drop(columns=["DateTime_UTC"], inplace=True)
            aem_raw.to_csv(config['data-export']['path']+filename, mode=('w' if isRefresh else 'a'), header=isRefresh, index=None, compression="gzip") # pd.read_csv('random_data.csv.gz', compression='gzip')
            logs.append([filename, idx] + log)


        aem_raw_interval = pd.read_csv(config['data-export']['path']+filename, compression='gzip')
        analysis_data = aem_raw_interval.merge(updated_labels, on="mcvisid", how="left") # if label - NaN: it is anonymousVistor 
        panel_snapshot = Analyzer.panel_summary(analysis_data, exclude_products=config['ETL'].getboolean('exclude_products_URL'))
        snapshot_filename = f"snapshot_2022{start_month}{start_day}-2022{end_month}{end_day}_{config['ETL']['parse_version']}{config['ETL']['summary_version']}.csv"
        panel_snapshot.to_csv(config['snapshot-export']['path'] + snapshot_filename)
        
        if config['ETL'].getboolean('is_month_break'):
            isContinue = input("Enter Y to continue: ")
            if not ((isContinue.lower() == "y") | (isContinue.lower() == "yes")):
                break
            
    logs_df = pd.DataFrame(logs, columns=[
                "Filename",
                "Start index",
                "Original raw rows",
                "Drop irrelevant mcvisid (testing emails) with rows",
                "Drop duplicate rows by ['mcvisid','date', 'hour', 'min', 'PageURL']",
                "Drop duplicate rows by ['mcvisid','date', 'hour', 'min', 'PageURL'] ratio",
                "Drop irrelevant PageURL (by inner join) with rows",
                "After ETL keep mcvisid rows",
                "After ETL keep unique mcvisid",
                "After ETL keep unique pageURL",])

    logs_df.to_csv(f"{config['report-export']['path']}AEM_table_ETL_logs_{config['ETL']['parse_version']}_{config['ETL']['summary_version']}.csv")
    
    # url checking
    URLs_validation = aem_raw_interval[["clean_PageURL", "PageURL"]].drop_duplicates().groupby("clean_PageURL").apply(lambda x: x["PageURL"].drop_duplicates().tolist()).to_dict()
    with gzip.open("URLs_validation.json.gz", 'wt', encoding='UTF-8') as zipfile:
        json.dump(URLs_validation, zipfile)