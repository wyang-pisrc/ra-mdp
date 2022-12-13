1. create database page_scoring;

2. use page_scoring;

3. CREATE TABLE `crm_data` (
    `leadid` VARCHAR(255) NOT NULL,
    `emailaddress1` VARCHAR(64) NULL,
    `firstname` VARCHAR(64) NULL,
    `lastname` VARCHAR(128) NULL,
    `jobtitle` VARCHAR(128) NULL,
    `companyname` VARCHAR(128) NULL,
    `ra_leadstagename` VARCHAR(32) NULL,
    `ra_salesrejectionreasonname` VARCHAR(128) NULL,
    `ra_telerejectionreasonname` VARCHAR(128) NULL,
    `address1_country` VARCHAR(32) NULL,
    PRIMARY KEY(leadid)
) DEFAULT CHARSET=utf8mb4;

grant all privileges on page_scoring.* to 'rockwell'@'localhost';
CREATE INDEX crm_email_idx ON crm_data(emailaddress1);

4. python importCrmData.py

5. CREATE TABLE `eloqua_data` (
    `EloquaContactId` VARCHAR(32) NOT NULL,
    `EmailAddress` VARCHAR(128) NOT NULL,
    PRIMARY KEY(EloquaContactId)
) DEFAULT CHARSET=utf8mb4;

CREATE INDEX email_idx ON eloqua_data (EmailAddress);

6. python importEloquaData.py

7. Table holds raw Adobe Analytics data
CREATE TABLE `aem_data` (
  `DateTime` datetime DEFAULT NULL,
  `PageURL` varchar(256) NOT NULL,
  `EloquaContactId` varchar(32) DEFAULT NULL,
  `mcvisid` varchar(64) NOT NULL,
  `GeoCountry` varchar(16) NOT NULL,
  KEY `mcvisid_idx` (`mcvisid`),
  KEY `eloqua_idx` (`EloquaContactId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

8. python importAEMData.py

9.  Table to hold Adobe Analytics data deduplicated by hour

CREATE TABLE `aem_data_hour` (
  `DateTime` datetime DEFAULT NULL,
  `PageURL` varchar(256) NOT NULL,
  `EloquaContactId` varchar(32) DEFAULT NULL,
  `mcvisid` varchar(64) NOT NULL,
  `GeoCountry` varchar(16) NOT NULL,
  UNIQUE KEY `visitMomentConstraint` (`DateTime`,`PageURL`,`mcvisid`),
  KEY `eloqua_idx` (`EloquaContactId`),
  KEY `mcvisid_idx` (`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

set autocommit=1;
INSERT IGNORE INTO aem_data_hour
SELECT DATE_FORMAT(DateTime, '%Y-%m-%d %H'), PageURL, EloquaContactId, mcvisid, GeoCountry FROM aem_data;

15898358 records total
3169387 records with EloquaContactId
12728971 records without EloquaContactId

10. 
Holds a mapping of analytics id with eloqua id
----
CREATE TABLE `aem_map` (
  `EloquaContactId` varchar(32) NOT NULL,
  `mcvisid` varchar(64) NOT NULL,
  PRIMARY KEY (`EloquaContactId`,`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


set autocommit=1;
INSERT INTO aem_map (EloquaContactId, mcvisid)
SELECT DISTINCT EloquaContactId, mcvisid FROM aem_data_hour WHERE EloquaContactId IS NOT NULL;

166971 records

11.
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

12.
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

13.
update lead_map set opportunity = 1 where leadid in (select distinct c.leadid from eloqua_data e, crm_data c where e.EmailAddress = c.emailaddress1 AND c.ra_leadstagename in ('Awaiting Sales Acceptance',  'Qualified', 'Awaiting Tele Acceptance', 'Distributor Lead', 'External Lead') );

82948 records

13.
Populate previously created aem_eloqua_crm empty leadid fields with applicable leadids from the lead_map
----
UPDATE aem_eloqua_crm a, lead_map l SET a.goodLeadId = l.leadid, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = 1;
369624 records

UPDATE aem_eloqua_crm a, lead_map l SET a.badLeadId = l.leadid, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = 0;
425574 records


14. 
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


15.
SELECT PageURL, total, eloqua, crmGood, crmBad, eloqua/total AS eRatio, crmGood/total AS lgRatio, crmBad/total AS lbRatio FROM counts WHERE total > 100 ORDER BY total DESC INTO OUTFILE '/Users/ikim/output-count-month-only.csv' FIELDS ENCLOSED BY '"' TERMINATED BY ';' ESCAPED BY '"' LINES TERMINATED BY '\n';

