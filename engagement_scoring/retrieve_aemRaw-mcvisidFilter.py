# Connect using pyodbc, sqlalchemy, and pandas
# import numpy as np
# import pandas as pd
import sqlalchemy
import csv
import getpass


  
def get_aemRaw_query():
    s = f"""
    WITH TMP1 AS (
    SELECT DISTINCT [EloquaContactId], [mcvisid]
    FROM [Staging].[aem].[RawTraffic]
    WHERE EloquaContactId <> '' AND VisitStartDateTime > '2022-04-21'
    ), 

    keepAEM AS (
    SELECT DISTINCT LOWER(elq.[EmailAddress]) AS "EmailAddress", TMP1.[mcvisid]
    FROM [Staging].[elq].[Contact] AS elq
    INNER JOIN TMP1 ON TMP1.EloquaContactId = elq.EloquaContactId
    WHERE elq.EloquaContactId <> '' AND elq.[EmailAddress] <> ''
    )

    SELECT 
        keepAEM.[EmailAddress] AS "ElqEmailAddress"
        ,[EloquaContactId]
        ,[EloquaGUID]
        ,AemRaw.[mcvisid]
        ,[SessionVisitorId]
        ,[VisitPageNumber]
        ,[VisitNumber]
        ,[NewVisit]
        ,[EventList]
        ,[DateTime_UTC]
        ,[PageURL]
        ,[VisitReferrer]
        ,[VisitReferrerType]
        ,[VisitorDomain]
        ,[External_Audience]
        ,[External_AudienceSegment]
        ,[External_Industry]
        ,[External_Website]
        ,[GeoCity]
        ,[GeoCountry]
        ,[GeoRegion]
        ,[PDFurl]
        ,[PDFpagecount]
        ,[BingeId]
        ,[BingeCriticalScore]
        ,[BingeScoredAssetPath]
        ,[BingeScoredAssetScore]
    FROM [Staging].[aem].[RawTraffic] AS AemRaw
    INNER JOIN keepAEM ON keepAEM.[mcvisid] = AemRaw.[mcvisid]
    WHERE (VisitStartDateTime > '2022-04-21') AND (VisitStartDateTime < '2022-12-01') 
     """
    return s


server = "sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net"
database = "Staging"
username = "pisrc-inkoo"
# password = input("Enter database password: ")
password = getpass.getpass('Enter database password: ')
driver = "ODBC Driver 17 for SQL Server"

engine = sqlalchemy.create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
)

session = engine.connect()
arraysize = 100000

query_string = get_aemRaw_query()
cursor = session.execute(query_string)

print("    Start saving...")
with open(f"aemRaw_keyColumns_20220421-20221201_filtered_mcvisid.csv", 'w') as f:
    idx = 0
    out = csv.writer(f)
    out.writerow([col for col in cursor.keys()])
    # out.writerows(cursor.fetchall()) # high RAM consume
    
    while True:
        idx +=1 
        print(f"        {idx}: row {idx*arraysize} - row {(idx+1)*arraysize}")
        results = cursor.fetchmany(arraysize)
        if not results:
            print("End")
            break
        out.writerows(results)
