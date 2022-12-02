# Connect using pyodbc, sqlalchemy, and pandas
# import numpy as np
# import pandas as pd
import sqlalchemy
import csv

  
def get_aemRaw_query(start_month, end_month, start_day, end_day):
    s = f"""
    SELECT [SessionVisitorId]
    --   ,[VisitStartDateTime]
    ,[VisitPageNumber]
    ,[VisitNumber]
    ,[NewVisit]
    ,[EventList]
    --   ,[ExcludeHit]
    --   ,[HitSource]
    ,[DateTime_UTC]
    ,[PageURL]
    --   ,[PageName]
    ,[VisitReferrer]
    ,[VisitReferrerType]
    ,[VisitorDomain]
    --   ,[IPaddress]
    --   ,[Language]
    --   ,[PageType]
    --   ,[ContentType]
    --   ,[UTM_Source]
    --   ,[UTM_Medium]
    --   ,[UTM_Campaign]
    --   ,[UTM_Content]
    --   ,[UTM_Term]
    --   ,[VideoName]
    --   ,[FileDownload]
    --   ,[FileType]
    --   ,[FileCategory]
    --   ,[GatedContentURL]
    --   ,[GatedContentFormType]
    --   ,[GatedContentFormName]
    --   ,[External_Id]
    --   ,[External_Company]
    ,[External_Audience]
    ,[External_AudienceSegment]
    --   ,[External_Address]
    --   ,[External_City]
    --   ,[External_StateProv]
    --   ,[External_PostalCode]
    --   ,[External_Country]
    ,[External_Industry]
    --   ,[External_SubIndustry]
    --   ,[External_SIC]
    ,[External_Website]
    --   ,[External_BusPhone]
    --   ,[External_NAICS]
    ,[EloquaContactId]
    ,[EloquaGUID]
    ,[mcvisid]
    ,[GeoCity]
    ,[GeoCountry]
    --   ,[GeoDMA]
    ,[GeoRegion]
    --   ,[GeoPostalCode]
    --   ,[Post_PostalCode]
    --   ,[Post_PageName]
    --   ,[Post_PageURL]
    --   ,[UploadedAt]
    ,[PDFurl]
    --   ,[PDFtitle]
    ,[PDFpagecount]
    ,[BingeId]
    --   ,[BingeName]
    ,[BingeCriticalScore]
    --   ,[BingeCampaignId]
    --   ,[BingeScoredAsset]
    ,[BingeScoredAssetPath]
    ,[BingeScoredAssetScore]
    FROM [Staging].[aem].[RawTraffic]
    WHERE (VisitStartDateTime > '2022-{start_month}-{start_day}') AND (VisitStartDateTime < '2022-{end_month}-{end_day}') """
    return s


server = "sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net"
database = "Staging"
username = "pisrc-inkoo"
password = input("Enter database password: ")
driver = "ODBC Driver 17 for SQL Server"

engine = sqlalchemy.create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
)

session = engine.connect()


BINGE_START_MONTH = 4
months = [str(month).zfill(2) for month in range(BINGE_START_MONTH, 13)]
month_pair = list(zip(months, months[1:]))
days = [0, 15, 31]
day_pair = list(zip(days, days[1:]))
arraysize = 100000
month_break = False

for pair in month_pair:
    start_month = pair[0]
    end_month = pair[1]
    
    for pair in day_pair:
        start_day = pair[0]
        end_day = pair[1]
        print(f"Range month: 2022{start_month}{start_day}-2022{end_month}{end_day}")
        print("    Loading... 5mins~10mins")
        query_string = get_aemRaw_query(start_month, end_month)
        cursor = session.execute(query_string)

        print("    Start saving...")
        with open(f"aemRaw_keyColumns_2022{start_month}{start_day}-2022{end_month}{end_day}.csv", 'w') as f:
            idx = 0
            out = csv.writer(f)
            out.writerow([col for col in cursor.keys()])
            # out.writerows(cursor.fetchall()) # high RAM consume
            
            while True:
                print(        f"{idx+1}: row {idx*arraysize} - row {(idx+1)*arraysize}")
                results = cursor.fetchmany(arraysize)
                if not results:
                    print("End for this month")
                    break
                out.writerows(results)

    if month_break:
        isContinue = input("Enter Y to continue: ")
        if not (isContinue.lower() == "y" | isContinue.lower() == "yes"):
            break;