WITH TMP1 AS (
SELECT DISTINCT [EloquaContactId], [mcvisid]
FROM [Staging].[aem].[RawTraffic]
WHERE EloquaContactId <> '' AND VisitStartDateTime > '2022-04-21'
), 

keepAEM AS (
SELECT DISTINCT LOWER(elq.[EmailAddress]) AS "EmailAddress", TMP1.[mcvisid]
FROM [Staging].[elq].[Contact] AS elq
INNER JOIN TMP1 ON TMP1.EloquaContactId = elq.EloquaContactId
WHERE elq.EloquaContactId <> '' AND elq.[EmailAddress] <> ''
)

SELECT 
      keepAEM.[EmailAddress] AS "ElqEmailAddress"
      ,[EloquaContactId]
      ,[EloquaGUID]
      ,AemRaw.[mcvisid]
      ,[SessionVisitorId]
      ,[VisitPageNumber]
      ,[VisitNumber]
      ,[NewVisit]
      ,[EventList]
      ,[DateTime_UTC]
      ,[PageURL]
      ,[VisitReferrer]
      ,[VisitReferrerType]
      ,[VisitorDomain]
      ,[External_Audience]
      ,[External_AudienceSegment]
      ,[External_Industry]
      ,[External_Website]
      ,[GeoCity]
      ,[GeoCountry]
      ,[GeoRegion]
      ,[PDFurl]
      ,[PDFpagecount]
      ,[BingeId]
      ,[BingeCriticalScore]
      ,[BingeScoredAssetPath]
      ,[BingeScoredAssetScore]
FROM [Staging].[aem].[RawTraffic] AS AemRaw
INNER JOIN keepAEM ON keepAEM.[mcvisid] = AemRaw.[mcvisid]
WHERE (VisitStartDateTime > '2022-04-21') AND (VisitStartDateTime < '2022-12-01') 
