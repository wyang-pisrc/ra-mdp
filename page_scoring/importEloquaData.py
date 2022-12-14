import csv
import pymysql
import glob
import re
import socket, struct
from dateutil.parser import parse
from datetime import datetime

badEmails = ["rockwellautomation","@pisrc.com","@bounteous.com","@ra.rockwell.com","demandbaseexport"]

mydb = pymysql.connect(host='localhost',
                             user='rockwell',
                             password='rockwell',
                             db='page_scoring',
                             charset='utf8mb4')


cursor = mydb.cursor()

csv_data = csv.reader(open('elq_all_bridge-only.csv', 'r'))
next(csv_data)

stmt = 'INSERT INTO eloqua_data (EloquaContactId, EmailAddress) VALUES (%s, %s)'

i=0;
for row in csv_data:
    try:
        EloquaContactId=str(row[1] or None).upper()
        EmailAddress=str(row[2] or None).lower()
    except Exception as e:
        print("Import parse exception : %s" % e)
        print(row)

    thisEmailIsGood = True
    for bad in badEmails:
        if bad in EmailAddress:
            # print("%s is a bad email" % EmailAddress)
            thisEmailIsGood = False
    if thisEmailIsGood:
        # print("Importing %s" % EmailAddress)
        cursor.execute(stmt, (EloquaContactId, EmailAddress));
        mydb.commit()
mydb.close()
