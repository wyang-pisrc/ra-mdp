import csv
import pymysql
import glob
import re
import socket, struct
from dateutil.parser import parse
from datetime import datetime

mydb = pymysql.connect(host='localhost',
                             user='rockwell',
                             password='rockwell',
                             db='page_scoring',
                             charset='utf8mb4')


cursor = mydb.cursor()

csv_data = csv.reader(open('crm_Lead_20221116_all.csv', 'r'))
next(csv_data)

stmt = 'INSERT INTO crm_data (leadid, emailaddress1, firstname, lastname, jobtitle, companyname, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, address1_country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

i=0;
for row in csv_data:
    if (i < 1):
        print("%i : %s" % (i,row))
        pi=0;
        i+=1
        for col in row:
            print("%i : %s" % (pi,col))
            pi+=1
    try:
        leadid=str(row[108])
        emailaddress1=str(row[82] or None).lower()
        firstname=str(row[93] or None)
        lastname=str(row[105] or None)
        jobtitle=str(row[104] or None)
        companyname=str(row[55] or None)
        ra_leadstagename=str(row[213] or None)
        ra_salesrejectionreasonname=str(row[251] or None)
        ra_telerejectionreasonname=str(row[265] or None)
        address1_country=str(row[7] or None)
    except Exception as e:
        print("Import parse exception : %s" % e)
        print(row)

    cursor.execute(stmt, (leadid, emailaddress1, firstname, lastname, jobtitle, companyname, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, address1_country));

mydb.commit()
mydb.close()
