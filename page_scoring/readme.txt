1. CREATE DATABASE page_scoring;

2. USE page_scoring;

3. CREATE TABLE `crm_data` (
  `leadid` varchar(255) NOT NULL,
  `emailaddress1` varchar(64) DEFAULT NULL,
  `firstname` varchar(64) DEFAULT NULL,
  `lastname` varchar(128) DEFAULT NULL,
  `jobtitle` varchar(128) DEFAULT NULL,
  `customerid` varchar(64) DEFAULT NULL,
  `customeridname` varchar(128) DEFAULT NULL,
  `ra_generalengagementscore` varchar(12) DEFAULT NULL,
  `ra_leadstagename` varchar(32) DEFAULT NULL,
  `ra_salesrejectionreasonname` varchar(128) DEFAULT NULL,
  `ra_telerejectionreasonname` varchar(128) DEFAULT NULL,
  `statecodename` varchar(16) DEFAULT NULL,
  `statuscodename` varchar(42) DEFAULT NULL,
  `ra_salesacceptedname` varchar(5) DEFAULT NULL,
  `address1_country` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`leadid`),
  KEY `crm_email_idx` (`emailaddress1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

GRANT ALL PRIVILEGES ON page_scoring.* TO 'rockwell'@'localhost';

4. python importCrmData.py

5. CREATE TABLE `eloqua_data` (
  `EloquaContactId` varchar(32) NOT NULL,
  `EmailAddress` varchar(128) NOT NULL,
  PRIMARY KEY (`EloquaContactId`),
  KEY `email_idx` (`EmailAddress`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

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

9. Table to hold adobe analytics data deduplicated by minute

create table `aem_data_minute` (
  `datetime` datetime default null,
  `pageurl` varchar(256) not null,
  `eloquacontactid` varchar(32) default null,
  `mcvisid` varchar(64) not null,
  `geocountry` varchar(16) not null,
  unique key `visitmomentconstraint` (`datetime`,`pageurl`,`mcvisid`),
  key `eloqua_idx` (`eloquacontactid`),
  key `mcvisid_idx` (`mcvisid`)
) engine=innodb default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;

set autocommit=1;
INSERT IGNORE INTO aem_data_minute
SELECT DATE_FORMAT(DateTime, '%Y-%m-%d %H:%i'), REGEXP_REPLACE(PageURL, '#.*$', ''), EloquaContactId, mcvisid, GeoCountry FROM aem_data;

---- 29980263 records total
---- 7199661 records with EloquaContactId
---- 22780602 records without EloquaContactId
---- (takes a while)


10. Holds a mapping of analytics id with eloqua id

CREATE TABLE `aem_map` (
  `EloquaContactId` varchar(32) NOT NULL,
  `mcvisid` varchar(64) NOT NULL,
  PRIMARY KEY (`EloquaContactId`,`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


set autocommit=1;
INSERT INTO aem_map (EloquaContactId, mcvisid)
SELECT DISTINCT EloquaContactId, mcvisid FROM aem_data_minute WHERE EloquaContactId IS NOT NULL;

---- 162081 records

11. Holds a mapping of pages without country site differentiation with their analytics id, back-dated eloqua id, and back-dated lead id

CREATE TABLE `aem_eloqua_crm` (
  `DateTime` datetime DEFAULT NULL,
  `PageURL` varchar(255) NOT NULL,
  `EloquaContactId` varchar(32) DEFAULT NULL,
  `EmailAddress` varchar(128) DEFAULT NULL,
  `goodLead` boolean DEFAULT 0,
  `neutralLead` boolean DEFAULT 0,
  `badLead` boolean DEFAULT 0,
  `mcvisid` varchar(64) NOT NULL,
  `GeoCountry` varchar(16) NOT NULL,
  KEY `page_idx` (`PageURL`),
  KEY `eloqua_idx` (`EloquaContactId`),
  KEY `mcvisid_idx` (`mcvisid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

set autocommit=1;
INSERT INTO aem_eloqua_crm (DateTime, PageURL, EloquaContactId, mcvisid, GeoCountry)
SELECT a.DateTime, a.flatURL, e.EloquaContactId, a.mcvisid, a.GeoCountry FROM (
    SELECT DateTime, REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                trim(leading 'https://www.rockwellautomation.com' FROM (
                    trim(leading 'https://www.rockwellautomation.com.cn' from PageURL)
                )), '/[a-z][a-z][_-][a-z][a-z][a-z]?/' , '/'
            ), '#.*', ''
        ), '//', '/'
    ) AS flatURL, EloquaContactId, mcvisid, GeoCountry
    FROM aem_data_minute
    WHERE PageURL LIKE 'https://www.rockwellautomation.com%'
) AS a
LEFT JOIN aem_map AS e
ON a.mcvisid=e.mcvisid
WHERE
    a.flatURL NOT REGEXP '(^/adfs/|^(http.*|file:.*)|https?:|.*\.gif$|.*\.js$|^/\%.*|change\-password)';

---- 15537075 records
---- (takes 46 minutes)

12.  Holds a mapping of crm lead id with eloqua id

CREATE TABLE `lead_map` (
  `EloquaContactId` varchar(32) NOT NULL,
  `EmailAddress` varchar(128) NOT NULL,
  `opportunity` int DEFAULT NULL, 
  PRIMARY KEY (`EloquaContactId`,`EmailAddress`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


set autocommit=1;
INSERT INTO lead_map (EloquaContactId, EmailAddress)
select distinct e.EloquaContactId, e.EmailAddress from eloqua_data e, crm_data c where e.EmailAddress = c.emailaddress1;

---- 144050 records

13.
set autocommit=1; 
UPDATE lead_map SET opportunity = 1 WHERE EmailAddress IN (
    SELECT DISTINCT c.emailaddress1
    FROM eloqua_data e, crm_data c
    WHERE e.EmailAddress = c.emailaddress1
        AND (
            c.statecodename = 'Qualified'
            OR c.statuscodename = 'Qualified'
            OR c.statuscodename in ('Assigned to Distribution', 'External Processing', 'Already Active Opportunity', 'Qualified')
        )
);

---- Changed: 36089

UPDATE lead_map SET opportunity = 0
WHERE 
opportunity IS NULL
AND EmailAddress IN (
    SELECT DISTINCT c.emailaddress1
    FROM eloqua_data e, crm_data c
    WHERE e.EmailAddress = c.emailaddress1
        AND c.statecodename = 'Disqualified'
        AND (
            c.statuscodename in ('Duplicate Lead', 'Admin Only: Abandoned by Sales', 'Does not meet campaign criteria')
            OR c.statuscodename IS NULL
        )
);

---- Changed: 25467

UPDATE lead_map SET opportunity = -1
WHERE
opportunity IS NULL
AND EmailAddress IN (
    SELECT DISTINCT c.emailaddress1
    FROM eloqua_data e, crm_data c
    WHERE e.EmailAddress = c.emailaddress1
        AND c.statecodename = 'Disqualified'
        AND c.statuscodename in ('Not Decision Maker', 'Not buying or influence location', 'No Interest', 'Insufficient information to contact', 'No buying intention', 'Unable to make contact (via phone,email)', 'Unable to make contact',  'No viable contact', 'Max Attempts', 'Competitor/Non RA distributor', 'Selling barrier to high', 'Unable to process', 'Credit hold or watch', 'Not Buying Location', 'No RA solution', 'Bad Contact Information')
);

---- Changed: 57856

13.  Populate previously created aem_eloqua_crm empty leadid fields with applicable leadids from the lead_map

UPDATE aem_eloqua_crm a, lead_map l SET a.neutralLead = TRUE, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = 0;
---- 306615 records

UPDATE aem_eloqua_crm a, lead_map l SET a.badLead = TRUE, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = -1;
---- 519422 records

UPDATE aem_eloqua_crm a, lead_map l SET a.goodLead = TRUE, a.EmailAddress = l.EmailAddress WHERE a.EloquaContactId = l.EloquaContactId and l.opportunity = 1;
---- 432412 records


14. 
CREATE TABLE `counts` (
  `PageURL` varchar(256),
  `total` INT DEFAULT 0,
  `eloqua` INT DEFAULT 0,
  `crmGood`  INT DEFAULT 0,
  `crmNeutral`  INT DEFAULT 0,
  `crmBad`  INT DEFAULT 0,
  PRIMARY KEY (`PageURL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO counts (PageUrl, total, eloqua, crmGood, crmNeutral, crmBad)
SELECT a.PageURL, a.c, b.c, lgood.c, lneutral.c, lbad.c from ( select PageURL, count(PageURL) c from aem_eloqua_crm group by PageURL) AS a
LEFT JOIN  ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE EloquaContactId is not null group by PageURL) b
ON a.PageURL = b.PageURL
LEFT JOIN ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE goodLead IS TRUE group by PageURL) lgood
ON a.PageURL = lgood.PageURL
LEFT JOIN ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE neutralLead IS TRUE group by PageURL) lneutral
ON a.PageURL = lneutral.PageURL
LEFT JOIN ( select PageURL, count(PageURL) c from aem_eloqua_crm WHERE badLead IS TRUE group by PageURL) lbad
ON a.PageURL = lbad.PageURL;

---- 462996 records
---- (4 minutes)

15. DELETE FROM counts WHERE eloqua IS NULL;
---- 418642 records

16. Create summary views of row math and rank.

DROP VIEW IF EXISTS summary_counts_view;

CREATE VIEW summary_counts_view AS
SELECT PageURL, total as Total, eloqua as Eloqua, crmGood, crmBad, (eloqua - crmGood - crmBad) as Unknown, eloqua/Total as LeadPartition, crmGood/(crmGood+crmBad) AS kYield, SQRT((crmGood/(crmGood+crmBad))*(1-(crmGood/(crmGood+crmBad)))/(crmGood + crmBad)) as kYieldError, crmGood/(crmGood+crmBad) * (1-SQRT((crmGood/(crmGood+crmBad))*(1-(crmGood/(crmGood+crmBad)))/(crmGood + crmBad))) as kYieldModified FROM counts WHERE (crmGood + crmBad) > 10 and crmGood > -1 and crmBad > -1;

DROP VIEW IF EXISTS summary_counts_rank_view;

CREATE VIEW summary_counts_rank_view AS
SELECT row_number() OVER(ORDER BY y.Total DESC) AS pageRank, row_number() OVER(ORDER BY y.Traffic DESC) AS goodLeadRank, row_number() OVER(ORDER BY y.LeadPartition DESC) AS leadRank, ROUND(y.Unknown * kYieldModified, 0) AS opportunities, y.*, CAST(y.kYieldModified as decimal(65,30))*CAST(y.Traffic as decimal(65,30))/(CAST(y.goodCount as decimal(65,30))/CAST(y.Subtotal as decimal(65,30))) as GoodPart, (1-CAST(y.kYieldModified as decimal(65,30)))*CAST(y.Traffic as decimal(65,30))/(CAST(y.badCount as decimal(65,30))/CAST(y.Subtotal as decimal(65,30))) as BadPart FROM (
    SELECT *, CAST((CAST((crmGood + crmBad) as decimal(65,30))/917938) as decimal(65,30)) as Traffic FROM (SELECT PageURL, Total, Eloqua, crmGood, crmBad, Unknown, LeadPartition, kYield, kYieldError, kYieldModified, (SELECT sum(crmGood) from summary_counts_view) as goodCount, (SELECT sum(crmBad) from summary_counts_view) as badCount, (SELECT sum(crmGood+crmBad) from summary_counts_view) as Subtotal from summary_counts_view order by kYieldModified ASC) x
) y ORDER BY kYieldModified DESC;

---- Export to CSV
SELECT "pageRank", "goodLeadRank", "leadRank", "Opportunities", "PageURL", "Total", "Eloqua", "crmGood", "crmBad", "Unknown", "LeadPartition", "kYield", "kYieldError", "kYieldModified", "goodCount", "badCount", "Subtotal", "Traffic", "GoodPart", "BadPart"  UNION ALL
(
    SELECT * from summary_counts_rank_view
)
INTO OUTFILE '/Users/ikim/rockwell-scores-all.csv' FIELDS ENCLOSED BY '"' TERMINATED BY ',' ESCAPED BY '"' LINES TERMINATED BY '\n';

---- 2840 records


select sum(crmGood), sum(crmBad), sum(crmGood + crmBad), sum(Eloqua), sum(Total) from counts where (crmGood + crmBad) > 10 and crmGood > -1 and crmBad > -1;
----|       417830 |      500108 |                917938 |     8243158 |   26732686 |

select sum(crmGood), sum(crmBad), sum(crmGood + crmBad), sum(Eloqua), sum(Total), sum(Traffic), sum(GoodPart), sum(BadPart) from summary_counts_rank_view;
----|       417830 |      500108 |                917938 |     8243158 |   26732686 | 0.999999999999999999999999999978 | 0.985270325573349627839410765158 | 1.012306341561597344973203787902


17.
CREATE TABLE `url_map` (
  `id` int NOT NULL AUTO_INCREMENT,
  `PageURL` varchar(256) DEFAULT NULL,
  `count` int default 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `PageURL` (`PageURL`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

set autocommit=1;
INSERT INTO url_map (PageURL, count)
SELECT pageurl, count(pageurl) as c FROM aem_eloqua_crm GROUP BY pageurl ORDER BY c DESC;


18.
CREATE TABLE `aem_keywords` (
  `mcvisid` varchar(64) NOT NULL,
  `keywords` varchar(128) NOT NULL,
  PRIMARY KEY (`mcvisid`, `keywords`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

python importAEMKeywords.py

SELECT DISTINCT k.keywords, l.EmailAddress, l.opportunity from aem_keywords k, aem_map m, lead_map l where m.mcvisid = k.mcvisid and m.EloquaContactId = l.EloquaContactId order by l.EmailAddress;

19.
CREATE TABLE `lead_score` (
  `EmailAddress` varchar(128) NOT NULL,
  `pageViews` int DEFAULT '1',
  `sessionCount` int DEFAULT '1',
  `firstVisit` datetime NOT NULL,
  `lastVisit` datetime NOT NULL,
  `leadScore` decimal(65,30) DEFAULT NULL,
  PRIMARY KEY (`EmailAddress`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

SELECT EmailAddress, count(*) as visitCount, min(aec.DateTime) as minTime, max(aec.DateTime) as maxTime from aem_eloqua_crm aec WHERE aec.emailAddress IS NOT NULL GROUP BY EmailAddress ORDER BY EmailAddress;
---- (1 minute)
