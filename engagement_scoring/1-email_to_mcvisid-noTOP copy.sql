WITH TMP1 AS (
SELECT DISTINCT [EloquaContactId], [mcvisid]
FROM [Staging].[aem].[RawTraffic]
WHERE EloquaContactId <> ''
), 

TMP2 AS (
SELECT DISTINCT  TMP1.[mcvisid], elq.EloquaContactId, LOWER(elq.[EmailAddress]) AS "EmailAddress"
FROM [Staging].[elq].[Contact] AS elq
INNER JOIN TMP1 ON TMP1.EloquaContactId = elq.EloquaContactId
WHERE elq.EloquaContactId <> ''
)

SELECT *
FROM TMP2