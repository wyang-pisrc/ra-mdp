import csv
import pymysql
import glob
import re
import socket, struct
import configparser
from dateutil.parser import parse
from datetime import datetime
import pandas as pd

config = configparser.ConfigParser()   
config.sections()
config.read('config.txt')

mydb = pymysql.connect(host=config['database']['host'],
                             user=config['database']['user'],
                             password=config['database']['password'],
                             db=config['database']['db'],
                             charset='utf8mb4')

cursor = mydb.cursor()

# csv_data = csv.reader(open(config['data-import']['datapath'] + 'crm_Lead_all_20230105.csv', 'r'))
# i=0;

df = pd.read_csv(config['data-import']['datapath'] + 'crm_Lead_all_20230307.csv.gz', compression='gzip', dtype=str).fillna("")
csv_data = df.iterrows()
columns = df.columns
stmt = 'INSERT INTO `crm_data` (leadid, emailaddress1, firstname, lastname, jobtitle, customerid, customeridname, ra_generalengagementscore, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, statecodename, statuscodename, ra_salesacceptedname, address1_country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

if config['processing'].getboolean('isTestMode'):
    for i, col in enumerate(columns):
        print("%s: %s" % (i, col))

for i,row in csv_data:
    try:
        leadid=row[107] or None
        emailaddress1=(row[81].lower() or None)
        firstname=row[92] or None
        lastname=row[104] or None
        jobtitle=row[103] or None
        customerid=row[64] or None
        customeridname=row[65] or None
        ra_generalengagementscore=row[202] or None
        ra_leadstagename=row[212] or None
        ra_salesrejectionreasonname=row[250] or None
        ra_telerejectionreasonname=row[264] or None
        statecodename=row[294] or None
        statuscodename=row[296] or None
        ra_salesacceptedname=row[242] or None
        address1_country=row[6] or None
        cursor.execute(stmt, (leadid, emailaddress1, firstname, lastname, jobtitle, customerid, customeridname, ra_generalengagementscore, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, statecodename, statuscodename, ra_salesacceptedname, address1_country));
    except Exception as e:
        print("Import parse exception : %s" % e)
        print(row)
        print("data: ", (leadid, emailaddress1, firstname, lastname, jobtitle, customerid, customeridname, ra_generalengagementscore, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, statecodename, statuscodename, ra_salesacceptedname, address1_country))
        print("statuscodename %s " % statuscodename)

                    
    if config['processing'].getboolean('isTestMode'):
        print("Test mode, breaking after 100 rows")
        break

mydb.commit()
mydb.close()
