import csv
import pymysql
import glob
import re
import socket, struct
import configparser
from dateutil.parser import parse
from datetime import datetime

config = configparser.ConfigParser()   
config.sections()
config.read('config.txt')

mydb = pymysql.connect(host=config['database']['host'],
                             user=config['database']['user'],
                             password=config['database']['password'],
                             db=config['database']['db'],
                             charset='utf8mb4')

cursor = mydb.cursor()

csv_data = csv.reader(open(config['data-import']['datapath'] + 'crm_Lead_20221116_all.csv', 'r'))

stmt = 'INSERT INTO `crm_data` (leadid, emailaddress1, firstname, lastname, jobtitle, customerid, customeridname, ra_generalengagementscore, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, statecodename, statuscodename, ra_salesacceptedname, address1_country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

i=0;
for row in csv_data:
    if (i < 1):
        print("%i : %s" % (i,row))
        pi=0;
        i+=1
        for col in row:
            print("%i : %s" % (pi,col))
            pi+=1
    else: 
        try:
            leadid=row[108]
            emailaddress1=row[82].lower()
            firstname=row[93] or None
            lastname=row[105] or None
            jobtitle=row[104] or None
            customerid=row[65] or None
            customeridname=row[66] or None
            ra_generalengagementscore=row[203] or None
            ra_leadstagename=row[213] or None
            ra_salesrejectionreasonname=row[251] or None
            ra_telerejectionreasonname=row[265] or None
            statecodename=row[295] or None
            statuscodename=row[297] or None
            ra_salesacceptedname=row[243] or None
            address1_country=row[7] or None
            cursor.execute(stmt, (leadid, emailaddress1, firstname, lastname, jobtitle, customerid, customeridname, ra_generalengagementscore, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, statecodename, statuscodename, ra_salesacceptedname, address1_country));
        except Exception as e:
            print("Import parse exception : %s" % e)
            print(row)
            print("statuscodename %s " % statuscodename)

mydb.commit()
mydb.close()
