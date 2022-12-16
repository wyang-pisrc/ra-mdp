SELECT mcvisid, SessionVisitorId, COUNT(*) as freq
FROM [Staging].[aem].[RawTraffic]
GROUP BY mcvisid, SessionVisitorId
ORDER BY freq