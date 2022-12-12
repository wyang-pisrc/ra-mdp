import csv
import pymysql
import glob
import re
import socket, struct
from dateutil.parser import parse
from datetime import datetime

"""
Table holds raw Adobe Analytics data
----
CREATE TABLE `aem_data` (
  `DateTime` datetime DEFAULT NULL,
  `PageURL` varchar(256) NOT NULL,
  `EloquaContactId` varchar(32) DEFAULT NULL,
  `mcvisid` varchar(64) NOT NULL,
  `GeoCountry` varchar(16) NOT NULL,
  KEY `mcvisid_idx` (`mcvisid`),
  KEY `eloqua_idx` (`EloquaContactId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

95870140 records total
25876740 records with EloquaContactId
69993400 records without EloquaContactId


Table hold Adobe Analytics data deduplicated by hour
----
CREATE TABLE `aem_data_hour` (
  `DateTime` datetime DEFAULT NULL,
  `PageURL` varchar(256) NOT NULL,
  `EloquaContactId` varchar(32) DEFAULT NULL,
  `mcvisid` varchar(64) NOT NULL,
  `GeoCountry` varchar(16) NOT NULL,
  UNIQUE KEY `visitMomentConstraint` (`DateTime`,`PageURL`,`mcvisid`),
  KEY `eloqua_idx` (`EloquaContactId`),
  KEY `mcvisid_idx` (`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

set autocommit=1;
INSERT IGNORE INTO aem_data_hour 
SELECT DATE_FORMAT(DateTime, '%Y-%m-%d %H'), PageURL, EloquaContactId, mcvisid, GeoCountry FROM aem_data;

15898358 records total
3169387 records with EloquaContactId
12728971 records without EloquaContactId

Holds a mapping of analytics id with eloqua id
----
CREATE TABLE `aem_map` (
  `EloquaContactId` varchar(32) NOT NULL,
  `mcvisid` varchar(64) NOT NULL,
  PRIMARY KEY (`EloquaContactId`,`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci


set autocommit=1;
INSERT INTO aem_map (EloquaContactId, mcvisid)
SELECT DISTINCT EloquaContactId, mcvisid FROM aem_data_hour WHERE EloquaContactId IS NOT NULL;

166971 records

Holds a mapping of pages without country site differentiation
with their analytics id, back-dated eloqua id, and back-dated lead id
----
CREATE TABLE `aem_eloqua_crm` (
  `DateTime` datetime DEFAULT NULL,
  `PageURL` varchar(255) NOT NULL,
  `EloquaContactId` varchar(32) DEFAULT NULL,
  `EmailAddress` varchar(128) DEFAULT NULL,
  `goodLeadId` varchar(255) DEFAULT NULL,
  `badLeadId` varchar(255) DEFAULT NULL,
  `mcvisid` varchar(64) NOT NULL,
  `GeoCountry` varchar(16) NOT NULL,
  KEY `page_idx` (`PageURL`),
  KEY `eloqua_idx` (`EloquaContactId`),
  KEY `mcvisid_idx` (`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

set autocommit=1;
INSERT INTO aem_eloqua_crm (DateTime, PageURL, EloquaContactId, mcvisid, GeoCountry)
SELECT a.DateTime, regexp_replace(trim(leading 'https://www.rockwellautomation.com' from (trim(leading 'https://www.rockwellautomation.com.cn' from a.PageURL))), '/[a-z][a-z][_-][a-z][a-z]/' , '/'), e.EloquaContactId, a.mcvisid, a.GeoCountry from aem_data_hour AS a
LEFT JOIN aem_map AS e 
ON a.mcvisid=e.mcvisid
WHERE a.PageURL like 'https://www.rockwellautomation.com%';

15538423 records

Holds a mapping of crm lead id with eloqua id
----
CREATE TABLE `lead_map` (
  `EloquaContactId` varchar(32) NOT NULL,
  `EmailAddress` varchar(128) NOT NULL,
  `leadid` varchar(255) NOT NULL,
  `opportunity` boolean DEFAULT false,
  PRIMARY KEY (`EloquaContactId`,`EmailAddress`, `leadid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci


set autocommit=1;
INSERT INTO lead_map (EloquaContactId, EmailAddress, leadid)
select distinct e.EloquaContactId, e.EmailAddress, c.leadid from eloqua_data e, crm_data c where e.EmailAddress = c.emailaddress1;

194951 records

update lead_map set opportunity = 1 where leadid in (select distinct c.leadid from eloqua_data e, crm_data c where e.EmailAddress = c.emailaddress1 AND c.ra_leadstagename in ('Awaiting Sales Acceptance',  'Qualified', 'Awaiting Tele Acceptance', 'Distributor Lead', 'External Lead') );

82948 records

Populate previously created aem_eloqua_crm empty leadid fields with applicable leadids from the lead_map
----
UPDATE aem_eloqua_crm a, lead_map l SET a.goodLeadId = l.leadid, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = 1;
369624 records

UPDATE aem_eloqua_crm a, lead_map l SET a.badLeadId = l.leadid, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = 0;
425574 records

CREATE TABLE `counts` (
  `PageURL` varchar(256),
  `total` INT DEFAULT 0,
  `eloqua` INT DEFAULT 0,
  `crmGood`  INT DEFAULT 0,
  `crmBad`  INT DEFAULT 0,
  PRIMARY KEY (`PageURL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO counts (PageUrl, total, eloqua, crmGood, crmBad)
SELECT a.PageURL, a.c, b.c, lgood.c, lbad.c from ( select PageURL, count(PageURL) c from aem_eloqua_crm group by PageURL) AS a
LEFT JOIN  ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE EloquaContactId is not null group by PageURL) b 
ON a.PageURL = b.PageURL
LEFT JOIN ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE goodLeadId is not null group by PageURL) lgood
ON a.PageURL = lgood.PageURL
LEFT JOIN ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE badLeadId is not null group by PageURL) lbad
ON a.PageURL = lbad.PageURL;

469836 records


SELECT PageURL, total, eloqua, crmGood, crmBad, eloqua/total AS eRatio, crmGood/total AS lgRatio, crmBad/total AS lbRatio FROM counts WHERE total > 100 ORDER BY total DESC INTO OUTFILE '/Users/ikim/output-count.csv' FIELDS ENCLOSED BY '"' TERMINATED BY ';' ESCAPED BY '"' LINES TERMINATED BY '\n';

"""

timestampPattern = re.compile('^[0-9]+\-[0-9]+\-[0-9]+ [0-9]+:[0-9]+:[0-9]+\-[0-9]+:[0-9]+$')

def isPattern(pattern, datestr):
  if datestr is None:
    return False
  if (pattern.match(str(datestr))):
    return True
  else:
    return False

mydb = pymysql.connect(host='localhost',
                             user='rockwell',
                             password='rockwell',
                             db='page_scoring',
                             charset='utf8mb4')


cursor = mydb.cursor()

files=[
    "aemRaw_keyColumns_20221001-20221015.csv",
    "aemRaw_keyColumns_20221015-20221031.csv",
    "aemRaw_keyColumns_20220401-20220415.csv",
    "aemRaw_keyColumns_20221101-20221115.csv",
    "aemRaw_keyColumns_20220415-20220430.csv",
    "aemRaw_keyColumns_20221115-20221130.csv",
    "aemRaw_keyColumns_20220501-20220515.csv",
    "aemRaw_keyColumns_20220515-20220531.csv",
    "aemRaw_keyColumns_20220601-20220615.csv",
    "aemRaw_keyColumns_20220615-20220630.csv",
    "aemRaw_keyColumns_20220701-20220715.csv",
    "aemRaw_keyColumns_20220715-20220731.csv",
    "aemRaw_keyColumns_20220801-20220815.csv",
    "aemRaw_keyColumns_20220815-20220831.csv",
    "aemRaw_keyColumns_20220901-20220915.csv",
    "aemRaw_keyColumns_20220915-20220930.csv",
]


for file in files:
    print("Working on %s" % file)
    csv_data = csv.reader(open(file, 'r'))
    next(csv_data)
    stmt = "INSERT INTO aem_data (DateTime, PageURL, EloquaContactId, mcvisid, GeoCountry) VALUES (%s, \"%s\", '%s', '%s', '%s')"

    i=0;
    for row in csv_data:
        try:
            DateTimeString=str(row[5])[0:19]
            PageURL=str(row[6] or None).lower()
            EloquaContactId=str(row[14] or None).upper()
            mcvisid=str(row[16] or None)
            GeoCountry=str(row[18] or None)
            DateTime="".join(["STR_TO_DATE(\"",DateTimeString, "\",\"%Y-%m-%d %H:%i:%S\")"])

            executableStmt=stmt % (DateTime,PageURL,EloquaContactId,mcvisid,GeoCountry)
            cursor.execute(executableStmt)
        except Exception as e:
            print("Import parse exception : %s" % e)
            print(row)

        i+=1
        if (i%100 == 0):
            mydb.commit()
    mydb.commit()
    cursor.execute("UPDATE aem_data set EloquaContactId = NULL WHERE EloquaContactId = 'NONE'")
    mydb.commit()
mydb.close()
