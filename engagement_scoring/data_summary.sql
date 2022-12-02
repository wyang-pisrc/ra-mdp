SELECT [SessionVisitorId], [VisitStartDateTime], [UploadedAt], [BingeId], [BingeName], [BingeScoredAssetPath], [EloquaContactId]
FROM [Staging].[aem].[RawTraffic]
WHERE VisitStartDateTime > '2022-10-21' AND BingeId <> ''
ORDER BY VisitStartDateTime DESC