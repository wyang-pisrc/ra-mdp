SELECT DISTINCT [UploadedAt]
FROM [Staging].[aem].[RawTraffic]
WHERE UploadedAt > '2022-07-21'
ORDER BY UploadedAt DESC