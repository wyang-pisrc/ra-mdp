SELECT [SessionVisitorId]
    --   ,[VisitStartDateTime]
      ,[VisitPageNumber]
      ,[VisitNumber]
      ,[NewVisit]
      ,[EventList]
    --   ,[ExcludeHit]
    --   ,[HitSource]
      ,[DateTime_UTC]
      ,[PageURL]
    --   ,[PageName]
      ,[VisitReferrer]
      ,[VisitReferrerType]
      ,[VisitorDomain]
    --   ,[IPaddress]
    --   ,[Language]
    --   ,[PageType]
    --   ,[ContentType]
    --   ,[UTM_Source]
    --   ,[UTM_Medium]
    --   ,[UTM_Campaign]
    --   ,[UTM_Content]
    --   ,[UTM_Term]
    --   ,[VideoName]
    --   ,[FileDownload]
    --   ,[FileType]
    --   ,[FileCategory]
    --   ,[GatedContentURL]
    --   ,[GatedContentFormType]
    --   ,[GatedContentFormName]
    --   ,[External_Id]
    --   ,[External_Company]
      ,[External_Audience]
      ,[External_AudienceSegment]
    --   ,[External_Address]
    --   ,[External_City]
    --   ,[External_StateProv]
    --   ,[External_PostalCode]
    --   ,[External_Country]
      ,[External_Industry]
    --   ,[External_SubIndustry]
    --   ,[External_SIC]
      ,[External_Website]
    --   ,[External_BusPhone]
    --   ,[External_NAICS]
      ,[EloquaContactId]
      ,[EloquaGUID]
      ,[mcvisid]
      ,[GeoCity]
      ,[GeoCountry]
    --   ,[GeoDMA]
      ,[GeoRegion]
    --   ,[GeoPostalCode]
    --   ,[Post_PostalCode]
    --   ,[Post_PageName]
    --   ,[Post_PageURL]
    --   ,[UploadedAt]
      ,[PDFurl]
    --   ,[PDFtitle]
      ,[PDFpagecount]
      ,[BingeId]
    --   ,[BingeName]
      ,[BingeCriticalScore]
    --   ,[BingeCampaignId]
    --   ,[BingeScoredAsset]
      ,[BingeScoredAssetPath]
      ,[BingeScoredAssetScore]
  FROM [Staging].[aem].[RawTraffic]
  WHERE (VisitStartDateTime > '2022-11-01') AND (VisitStartDateTime < '2022-12-01') 