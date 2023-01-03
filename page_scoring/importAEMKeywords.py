import csv
import pymysql
import glob
import re
import socket, struct
import configparser
from urllib.parse import unquote
from dateutil.parser import parse
from datetime import datetime

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
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221001-20221015.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221015-20221031.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220401-20220415.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221101-20221115.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20220415-20220430.csv",
    config['data-import']['datapath'] + "aemRaw_keyColumns_20221115-20221130.csv",
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
]



for file in files:
    print("Working on %s" % file)
    csv_data = csv.reader(open(file, 'r'), delimiter=',', quotechar='"')


    next(csv_data)
    stmt = "INSERT IGNORE INTO aem_keywords (mcvisid, keywords) VALUES (%s, %s)"

    i=0;
    for row in csv_data:
        try:
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
    mydb.commit()
mydb.close()
