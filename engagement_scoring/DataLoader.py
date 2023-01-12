import re
import os
from datetime import date
import pandas as pd
from preprocessingUtils import mcvisid_label_assign, email_cleanup, search_params_parser
import glob

class Query_DataLoader:
    def __init__(self, engine, preload=False, data_export_folder="./data/", data_import_folder="./data/"):
        self.today = str(date.today()).replace("-","")
        self.engine = engine
        self.preload = preload
        self.data_export_folder = data_export_folder
        self.data_import_folder = data_import_folder
        
    def __str__(self):
        print("DataLoader Instance for query into DB and pre-save compressed data into local")
        
    def show_config(self):
        print("Today: ", self.today)
        print("Preload: ", self.preload)
        print("Data Folder: ", self.data_export_folder)
        
    def loading(self, filename, query, save_override=False):
        
        if (self.preload is False) |(save_override is True):
            print(f"Updating table into {filename}")
            df = pd.read_sql(query, self.engine)
            df.to_csv(self.data_export_folder + filename, index=None, compression="gzip")
        else:    
            filename = self.get_latest_filename(filename)
            print(f"Loading from stored gz file: {filename}")
            df = pd.read_csv(self.data_export_folder + filename, compression="gzip")
        
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
        if self.preload is False:
            print("Processing elq table for In Koo pipeline...")
            elq_filename = f"elq_all_bridge-only_{self.today}.csv.gz"
            self.loading(elq_filename, self.get_email_mcvisid_query(), save_override=True)
        else:
            print("Skip elq_bridge updating since preload is False ")

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
    
    def get_latest_filename(self, filename="mcvisid_elqid_email.csv.gz"):
        pattern = re.findall("([a-zA-z_-]+)_\d", filename)[0] + "*.csv.gz"
        local_files = sorted(map(os.path.basename, glob.glob(self.data_export_folder+pattern)))
        if filename not in local_files:
            filename = local_files[-1]
        return filename
    
    def load_updated_labels(self):
        ## label assign
        email_mcvisid, valid_mcvisid, drop_mcvisid = self.load_email_mcvisid()
        
        absolute_filename = self.data_export_folder + f"updated_labels_{self.today}.csv.gz"
        if self.preload:
            print("Loading processed mcvisid to updated_labels mapping table")
            filename = self.get_latest_filename(absolute_filename)
            updated_labels = pd.read_csv(self.data_export_folder + filename, compression="gzip")
        else:
            _lead = self.load_crm_lead()
            updated_labels = mcvisid_label_assign(_lead, email_mcvisid)
            updated_labels.to_csv(absolute_filename, compression="gzip", index=None)
            print(f"     Finished and stored into {absolute_filename}")
        
        return updated_labels, valid_mcvisid, drop_mcvisid

    def load_mcvisid_keyword(self):
        pass
        # search_params_parser()
        
        
    def load_mcvisid_search_tabs(self):
        pass
        # search_params_parser()
        
    def load_mcvisid_job_title(self):
        pass
    
    def load_mcvisid_industry(self):
        pass
    
    
# search_keys = aem_raw["VisitReferrer"].dropna().apply(lambda x: search_params_parser(x, "keyword"))
# activate_tabs = aem_raw["VisitReferrer"].dropna().apply(lambda x: search_params_parser(x, "activeTab"))