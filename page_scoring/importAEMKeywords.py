import csv
import pymysql
import glob
import re
import socket, struct
import configparser
from urllib.parse import unquote
from dateutil.parser import parse
from datetime import datetime
import pandas as pd

timestampPattern = re.compile('^[0-9]+\-[0-9]+\-[0-9]+ [0-9]+:[0-9]+:[0-9]+\-[0-9]+:[0-9]+$')

def isPattern(pattern, datestr):
  if datestr is None:
    return False
  if (pattern.match(str(datestr))):
    return True
  else:
    return False

config = configparser.ConfigParser()
config.sections()
config.read('config.txt')

mydb = pymysql.connect(host=config['database']['host'],
                             user=config['database']['user'],
                             password=config['database']['password'],
                             db=config['database']['db'],
                             charset='utf8mb4')

cursor = mydb.cursor()

files=[
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220401-20220415.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220415-20220430.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220501-20220515.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220515-20220531.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220601-20220615.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220615-20220630.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220701-20220715.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220715-20220731.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220801-20220815.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220815-20220831.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220901-20220915.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220915-20220930.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221001-20221015.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221015-20221031.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221101-20221115.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221115-20221130.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221201-20221215.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221215-20221231.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20230101-20230115_p1v1.csv.gz",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20230115-20230131_p1v1.csv.gz",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20230201-20230215_p1v1.csv.gz",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20230215-20230228_p1v1.csv.gz",
]


# mapping from 0 : SessionVisitorId
# mapping from 1 : VisitPageNumber
# mapping from 2 : VisitNumber
# mapping from 3 : NewVisit
# mapping from 4 : EventList
# mapping from 5 : DateTime_UTC 5 -> 0
# mapping from 6 : PageURL 6 -> 1
# mapping from 7 : VisitReferrer 7 -> 2
# mapping from 8 : VisitReferrerType
# mapping from 9 : VisitorDomain 9 -> 3
# mapping from 10 : External_Audience
# mapping from 11 : External_AudienceSegment
# mapping from 12 : External_Industry
# mapping from 13 : External_Website
# mapping from 14 : EloquaContactId 14 -> 4
# mapping from 15 : EloquaGUID
# mapping from 16 : mcvisid 16 -> 5
# mapping from 17 : GeoCity
# mapping from 18 : GeoCountry 18 -> 6
# mapping from 19 : GeoRegion
# mapping from 20 : PDFurl
# mapping from 21 : PDFpagecount
# mapping from 22 : BingeId
# mapping from 23 : BingeCriticalScore
# mapping from 24 : BingeScoredAssetPath 24 -> 7
# mapping from 25 : BingeScoredAssetScore

VERSION_FLAG = config['data-import']['versionFlag']

for file in files:
    print("Working on %s" % file)
    if VERSION_FLAG in file:
        df = pd.read_csv(file, compression="gzip", dtype=str).fillna("")
        csv_data = df.iterrows()
    else:
        csv_data = csv.reader(open(file, 'r'), delimiter=',', quotechar='"')
        next(csv_data)
    stmt = "INSERT IGNORE INTO aem_keywords (mcvisid, keywords) VALUES (%s, %s)"

    i=0;
    for row in csv_data:
        try:
            if VERSION_FLAG in file:
                idx, row = row # first element is index of row
                keywordsRaw=str(row[1]) or None
                mcvisid=str(row[3]) or None
            else:
                keywordsRaw=str(row[7]) or None
                mcvisid=str(row[16]) or None
            keywords=None
            if keywordsRaw is not None:
                for frag in keywordsRaw.split(";"):
                    if frag[0:8] == "keyword=":
                        keywords=unquote(unquote(frag[8:])).lower().strip()
                        if keywords == '':
                            keywords=None
            if keywords is not None:
                cursor.execute(stmt,(mcvisid, keywords))
        except Exception as e:
            print("Import parse exception : %s" % e)
            print(row)
        i+=1
        if (i%100 == 0):
            mydb.commit()
            
                
        if config['processing'].getboolean('isTestMode'):
            print("Test mode, breaking after 100 rows")
            break
        
    mydb.commit()
mydb.close()
