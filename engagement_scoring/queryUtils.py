from datetime import date
import pandas as pd
from contentScoreShareUtils import email_cleanup

class Scoring_DataLoader:
    def __init__(self, engine, preload=False):
        self.today = str(date.today()).replace("-","")
        self.engine = engine
        self.preload = preload
        
        self.lead_filename = f"crm_Lead_all_{self.today}.csv.gz"
        self.elq_filename = f"elq_all_bridge-only_{self.today}.csv.gz"
        
    def loading(self, filename, query):
        if self.preload is True:
            print(f"Loading from stored gz file: {filename}")
            df = pd.read_csv(filename, compression="gzip")
        else: 
            print(f"Updating table into {filename}")
            df = pd.read_sql(query, self.engine)
            df.to_csv(filename, index=None, compression="gzip")
        return df
            
    def load_email_mcvisid(self):
        # read mcvisid_elqid_email_all into memory
        mcvisid_bridge_filename = f"mcvisid_elqid_email_all_{self.today}.csv.gz"
        email_mcvisid = self.loading(mcvisid_bridge_filename, self.get_email_mcvisid_query())
        email_mcvisid, drop_mcvisid = email_cleanup(email_mcvisid, "EmailAddress")
        valid_mcvisid = email_mcvisid["mcvisid"].drop_duplicates() ## all along the past, if mcvisid existed with corresponding elqid, then it is positive sigal
        return email_mcvisid, valid_mcvisid, drop_mcvisid

    
    def load_crm_lead(self):
        # read crm_lead table in to memory
        lead_filename = f"crm_Lead_all_{self.today}.csv.gz"
        _lead = self.loading(lead_filename, self.get_crm_lead_query())
        return _lead
    
    
    def save_elq_bridge(self):
        # store elq table
        elq_filename = f"elq_all_bridge-only_{self.today}.csv.gz"
        print(f"Updating table into {elq_filename}")
        df = pd.read_sql(self.get_elq_bridge_only_query(), self.engine)
        df.to_csv(elq_filename, index=None, compression="gzip")
        del df
        

    def get_crm_lead_query(self):
        s = f"""
            SELECT *
            FROM [Staging].[crm].[Lead]
            """
        return s
    
    def get_email_mcvisid_query(self):
        s = f"""
            WITH TMP1 AS (
            SELECT DISTINCT [EloquaContactId], [mcvisid]
            FROM [Staging].[aem].[RawTraffic]
            WHERE EloquaContactId <> '' AND VisitStartDateTime > '2022-04-01'
            ), 

            TMP2 AS (
            SELECT DISTINCT  TMP1.[mcvisid], elq.EloquaContactId, LOWER(elq.[EmailAddress]) AS "EmailAddress"
            FROM [Staging].[elq].[Contact] AS elq
            INNER JOIN TMP1 ON TMP1.EloquaContactId = elq.EloquaContactId
            WHERE elq.EloquaContactId <> ''
            )

            SELECT *
            FROM TMP2 
            """
        return s
    
    def get_elq_bridge_only_query(self):
        s = f"""    
            SELECT DISTINCT elq.[EloquaContactId], elq.[EmailAddress]
            FROM [Staging].[elq].[Contact] AS elq
            WHERE elq.[EloquaContactId]<>''
            """
        return s
    
        
    def generate_aemRaw_query(self, start_month, end_month, start_day, end_day):
        s = f"""
            SELECT 
            -- ,[SessionVisitorId]
            -- ,[VisitStartDateTime]
            -- ,[VisitPageNumber]
            -- ,[VisitNumber]
            -- ,[NewVisit]
            -- ,[EventList]
            --   ,[ExcludeHit]
            --   ,[HitSource]
            [DateTime_UTC]
            ,[PageURL]
            --   ,[PageName]
            ,[VisitReferrer]
            --    ,[VisitReferrerType]
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
            --   ,[External_Audience]
            --   ,[External_AudienceSegment]
            --   ,[External_Address]
            --   ,[External_City]
            --   ,[External_StateProv]
            --   ,[External_PostalCode]
            --   ,[External_Country]
            --   ,[External_Industry]
            --   ,[External_SubIndustry]
            --   ,[External_SIC]
            --   ,[External_Website]
            --   ,[External_BusPhone]
            --   ,[External_NAICS]
            --   ,[EloquaContactId]
            --   ,[EloquaGUID]
            ,[mcvisid]
            -- ,[GeoCity]
            ,[GeoCountry]
            --   ,[GeoDMA]
            --   ,[GeoRegion]
            --   ,[GeoPostalCode]
            --   ,[Post_PostalCode]
            --   ,[Post_PageName]
            --   ,[Post_PageURL]
            --   ,[UploadedAt]
            --   ,[PDFurl]
            --   ,[PDFtitle]
            --   ,[PDFpagecount]
            --   ,[BingeId]
            --   ,[BingeName]
            --   ,[BingeCriticalScore]
            --   ,[BingeCampaignId]
            --   ,[BingeScoredAsset]
            ,[BingeScoredAssetPath]
            -- ,[BingeScoredAssetScore]
            FROM [Staging].[aem].[RawTraffic]
            WHERE (VisitStartDateTime >= '2022-{start_month}-{start_day}') AND (VisitStartDateTime < '2022-{end_month}-{end_day}')
            """
        return s
