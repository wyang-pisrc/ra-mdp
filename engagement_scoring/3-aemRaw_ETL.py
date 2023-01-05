# Connect using pyodbc, sqlalchemy, and pandas
# import numpy as np
# import pandas as pd
import sqlalchemy
import re
import csv
import getpass
import pandas as pd
from queryUtils import Scoring_DataLoader
from contentScoreShareUtils import mcvisid_label_assign, url_clean, url_filter
from datetime import date
import os 

def datetime_interval_iterator(event_start_month = 4, event_end_month=12):
    months = [str(month).zfill(2) for month in range(event_start_month, event_end_month+2)] # +2 for iterator
    month_pair = list(zip(months, months[1:]))
    days = ["01", "15", "31"]
    day_pair = list(zip(days, days[1:]))
    month_max_days = {"04":"30", "06":"30","09":"30","11":"30"}

    for (_start_month, _end_month) in month_pair:
        span = 0
        for (start_day, end_day) in day_pair:
            span +=1
            if span in [1,2]:
                start_month, end_month = _start_month, _start_month
            if span in [3,4]:
                start_month, end_month = _end_month, _end_month

            if (end_month in month_max_days.keys()) & (end_day == "31"):
                end_day = "30"

            print(f"Range month: 2022{start_month}{start_day}-2022{end_month}{end_day}")
            yield start_month, start_day, end_month, end_day
            
def add_timestamp_columns(stage1_raw):
    times_mapper = {
        "date": stage1_raw["DateTime_UTC"].str.slice(0, 10).str.replace("-","").astype(int),
        "hour": stage1_raw["DateTime_UTC"].str.slice(11, 13),
        "minute": stage1_raw["DateTime_UTC"].str.slice(14, 16)
    }
    stage1_raw = stage1_raw.assign(**times_mapper)
    return stage1_raw

def aem_raw_preprocessing(aem_raw, drop_mcvisid, valid_mcvisid):
    stage1_raw = aem_raw
    log = []
    
    row1 = stage1_raw.shape[0]
    log.append(row1)

    stage1_raw = stage1_raw[~stage1_raw["mcvisid"].isin(drop_mcvisid["mcvisid"])] # drop irrelevant mcvisid
    row2 = stage1_raw.shape[0]
    log.append(row1 - row2)

    ## deduplicate with minute granularity
    stage1_raw = add_timestamp_columns(stage1_raw)    
    stage1_raw = stage1_raw.drop_duplicates(subset=["mcvisid","date", "hour", "minute", "PageURL"], keep="first")
    row3 = stage1_raw.shape[0]
    log.append(row2 - row3)
    log.append((row2 - row3)/row2)

    ## get clean PageURL and drop irrelevant PageURL - inner join to drop unmatched rows
    url_maps = url_filter(stage1_raw[["PageURL"]].drop_duplicates())
    url_maps["clean_PageURL"] = url_maps["PageURL"].apply(lambda x: url_clean(x))
    # s = url_maps[url_maps["PageURL"].str.contains("details")].sample(100)
    stage1_raw = stage1_raw.merge(url_maps, on="PageURL")
    row4 = stage1_raw.shape[0]
    log.append(row3 - row4)

    ## create labels
    stage1_raw["existElqContactID"] = stage1_raw["mcvisid"].isin(valid_mcvisid) # mark pos and neg mcvisid
    row5 = stage1_raw.shape[0]
    log.append(row5)
    log.append(stage1_raw["mcvisid"].unique().shape[0])
    log.append(stage1_raw["clean_PageURL"].unique().shape[0])
    
    return stage1_raw, log




if __name__ == '__main__':
    today = str(date.today()).replace("-","")

    server = "sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net"
    database = "Staging"
    username = "pisrc-inkoo"
    password = getpass.getpass('Enter database password: ')
    driver = "ODBC Driver 17 for SQL Server"

    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
    )

    session = engine.connect()

    BINGE_START_MONTH = 4
    BINGE_END_MONTH = 4
    batch_size = 1000000
    month_break = False
    parse_version = 'p1'
    summary_version = 'v1'
    preload = False
    
    ## data store 
    dataLoader = Scoring_DataLoader(engine, preload)
    email_mcvisid, valid_mcvisid, drop_mcvisid = dataLoader.load_email_mcvisid()
    _lead = dataLoader.load_crm_lead()
    dataLoader.save_elq_bridge()
    
    ## label assign
    updated_labels = mcvisid_label_assign(_lead, email_mcvisid)
    
    ## start processing by batch
    logs = []
    for (start_month, start_day, end_month, end_day) in datetime_interval_iterator(event_start_month=BINGE_START_MONTH, event_end_month=BINGE_END_MONTH):
        print("    Loading from SQL Server... ")
        query_string = dataLoader.generate_aemRaw_query(start_month, end_month, start_day, end_day)
        cursor = session.execute(query_string)

        print("    Start processing...")
        idx = 0
        filename = f"aemRaw_keyColumns_2022{start_month}{start_day}-2022{end_month}{end_day}_{parse_version}{summary_version}.csv" + ".gz"
        os.remove(filename)
        columns = [col for col in cursor.keys()]
        while True:
            idx +=1 
            aem_raw = pd.DataFrame(cursor.fetchmany(batch_size), columns=columns, dtype=str) # out.writerows(cursor.fetchall()) # high RAM consume
            if aem_raw.shape[0] == 0:
                print("    End for this month")
                break
            
            aem_raw, log = aem_raw_preprocessing(aem_raw, drop_mcvisid, valid_mcvisid) # clean up irrelevant rows
            print(f"        {idx}: row {(idx-1)*batch_size} - row {idx * batch_size}, finish loading/processing from SQL Server, keep {aem_raw.shape[0]} rows")
            hasHeader = (idx == 1)
            aem_raw.drop(columns=["DateTime_UTC"], inplace=True)
            aem_raw.to_csv(filename, mode='a', header=hasHeader, index=None, compression="gzip") # pd.read_csv('random_data.csv.gz', compression='gzip')
            logs.append([filename, idx] + log)

        
        aem_raw_interval = pd.read_csv(filename, compression='gzip')
        panel_data = aem_raw_interval.merge(updated_labels, on="mcvisid", how="left") # drop no valid mcvisid
        # aem_raw_interval.to_csv(f"model_input_EDA_{summary_version}.csv") # 
        
        ## obtain crmGood, crmBad, crmNeutu
        ## calc metrics: 
        ## store in local
        
        if month_break:
            isContinue = input("Enter Y to continue: ")
            if not ((isContinue.lower() == "y") | (isContinue.lower() == "yes")):
                break
            
    logs_df = pd.DataFrame(logs, columns=[
                "Filename",
                "Start index",
                "Original raw rows",
                "Drop irrelevant mcvisid (testing emails) with rows",
                "Drop duplicate rows by ['mcvisid','date', 'hour', 'PageURL']",
                "Drop duplicate rows by ['mcvisid','date', 'hour', 'PageURL'] ratio",
                "Drop irrelevant PageURL (by inner join) with rows",
                "After ETL keep mcvisid rows",
                "After ETL keep unique mcvisid",
                "After ETL keep unique pageURL",])

    logs_df.to_excel(f"AEM_table_ETL_logs_{parse_version}_{summary_version}.xlsx")


