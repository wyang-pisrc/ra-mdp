import re
import os
from datetime import date
import pandas as pd
from preprocessingUtils import mcvisid_crmlead_label_assign, email_cleanup, search_params_parser, mcvisid_elqcontact_jobLevel_assign, mcvisid_elqcontact_label_assign
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
            
    def load_mcvisid_elqid_email(self):
        # read mcvisid_elqid_email_all into memory
        mcvisid_bridge_filename = f"mcvisid_elqid_email_all_{self.today}.csv.gz"        
        mcvisid_elqid_email = self.loading(mcvisid_bridge_filename, self.get_mcvisid_elqid_email_query())
        mcvisid_elqid_email, drop_mcvisid = email_cleanup(mcvisid_elqid_email, "EmailAddress")
        valid_mcvisid = mcvisid_elqid_email["mcvisid"].drop_duplicates() ## all along the past, if mcvisid existed with corresponding elqid, then it is positive sigal
        return mcvisid_elqid_email, valid_mcvisid, drop_mcvisid

    
    def load_crm_lead(self):
        # read crm_lead table in to memory
        lead_filename = f"crm_Lead_all_{self.today}.csv.gz"
        _lead = self.loading(lead_filename, self.get_crm_lead_query())
        return _lead
    

    def load_elq_contact(self):
        # read crm_lead table in to memory
        elq_filename = f"elq_Contact_IndustryJobs_{self.today}.csv.gz"
        _elq = self.loading(elq_filename, self.get_elq_contact_query())
        return _elq
    
    
    def save_elq_bridge(self):
        # store elq table
        if self.preload is False:
            print("Processing elq table for In Koo pipeline...")
            elq_filename = f"elq_all_bridge-only_{self.today}.csv.gz"
            self.loading(elq_filename, self.get_elq_contact_query_Inkoo(), save_override=True)
        else:
            print("Skip elq_bridge updating since preload is False ")

    def get_crm_lead_query(self):
        s = f"""
            SELECT *
            FROM [Staging].[crm].[Lead]
            """
        return s
    
    def get_elq_contact_query(self):
        s = f"""
            SELECT DISTINCT elq.[EloquaContactId], elq.[EmailAddress], elq.[Company], elq.[Industry], elq.[JobFunction], elq.[JobLevel], elq.[JobTitle]
            FROM [Staging].[elq].[Contact] AS elq
            WHERE elq.[EloquaContactId]<>''
            """
        return s
    
    def get_mcvisid_elqid_email_query(self):
        s = f"""
            WITH TMP1 AS (
            SELECT DISTINCT [EloquaContactId], [mcvisid]
            FROM [Staging].[aem].[RawTraffic]
            WHERE EloquaContactId <> '' AND VisitStartDateTime > '2022-04-01'
            ), 

            TMP2 AS (
            SELECT DISTINCT TMP1.[mcvisid], elq.EloquaContactId, elq.[EmailAddress]
            FROM [Staging].[elq].[Contact] AS elq
            INNER JOIN TMP1 ON TMP1.EloquaContactId = elq.EloquaContactId
            WHERE elq.EloquaContactId <> ''
            )

            SELECT *
            FROM TMP2 
            """
        return s
    
    def get_elq_contact_query_Inkoo(self):
        s = f"""    
            SELECT DISTINCT elq.[EloquaContactId], elq.[EmailAddress], elq.[Company], elq.[Industry], elq.[JobFunction], elq.[JobLevel], elq.[JobTitle]
            FROM [Staging].[elq].[Contact] AS elq
            WHERE elq.[EloquaContactId]<>''
            """
        return s
    
        
    def generate_aemRaw_query(self, current_year, start_month, end_month, start_day, end_day):
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
            WHERE (VisitStartDateTime >= '{current_year}-{start_month}-{start_day}') AND (VisitStartDateTime < '{current_year}-{end_month}-{end_day}')
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
        mcvisid_elqid_email, valid_mcvisid, drop_mcvisid = self.load_mcvisid_elqid_email()
        print(f"    mcvisid_elqid_email: {mcvisid_elqid_email.shape}, valid_mcvisid: {valid_mcvisid.shape}, drop_mcvisid: {drop_mcvisid.shape}")
        
        absolute_filename = self.data_export_folder + f"updated_labels_{self.today}.csv.gz"
        if self.preload:
            filename = self.get_latest_filename(absolute_filename)
            print("Loading processed mcvisid to updated_labels mapping table: ", filename)
            updated_labels = pd.read_csv(self.data_export_folder + filename, compression="gzip")
        else:
            ######################################################
            ######### extending more labels in here
            ######################################################
            _lead = self.load_crm_lead()
            updated_labels_lead = mcvisid_crmlead_label_assign(_lead, mcvisid_elqid_email)
            
            
            ## hard coded version for labels in elq 
            _elq = self.load_elq_contact()
            source_map = self.load_JobLevel_SourceMap()
            standardized_map = self.load_Standardized_JobLevel_Map()
            code_mapper = {k: standardized_map[v] for k,v in source_map.items()}
            print(code_mapper)
            updated_labels_elq_jobLevel = mcvisid_elqcontact_jobLevel_assign(_elq, mcvisid_elqid_email, code_mapper)
            
            ## generalized version for labels in elq 
            source_map = self.load_eloquaIndustry_SourceMap()
            standardized_map = self.load_Standardized_eloquaIndustryMap()
            code_mapper = {k: standardized_map[v] for k,v in source_map.items()}
            print(code_mapper)
            updated_labels_elq_industry = mcvisid_elqcontact_label_assign(_elq, mcvisid_elqid_email, code_mapper, source_mapping_filename = "eloquaIndustryMap_manual_v1.2_Wei_updated", key="Industry")

            updated_labels = updated_labels_lead.merge(updated_labels_elq_jobLevel, how="left").merge(updated_labels_elq_industry)
            
            ### fill nan with 0
            updated_labels.fillna(0, inplace=True)
            updated_labels.to_csv(absolute_filename, compression="gzip", index=None)
            print(f"     Finished and stored into {absolute_filename}")
        
        return updated_labels, valid_mcvisid, drop_mcvisid

    def load_mcvisid_keyword(self):
        pass
        # search_params_parser()
        
        
    def load_mcvisid_search_tabs(self):
        pass
        # search_params_parser()
            
    # search_keys = aem_raw["VisitReferrer"].dropna().apply(lambda x: search_params_parser(x, "keyword"))
    # activate_tabs = aem_raw["VisitReferrer"].dropna().apply(lambda x: search_params_parser(x, "activeTab"))


    def load_JobLevel_SourceMap(self):
            
        sourceMap = {
        "role-Csuite": "role-Csuite",
        "role-Manager": "role-Manager",
        "role-Engineer": "role-Engineer",
        "role-Marketing":  "role-Marketing",
        "role-Unknown": "role-Unknown",
        "role-Other":  "role-Other",
        }
        
        return sourceMap


    def load_Standardized_JobLevel_Map(self):
        
        Standardized_JobLevel_Map = {
        "role-Csuite": 4,
        "role-Manager": 3,
        "role-Engineer": 2,
        "role-Marketing": -1,
        "role-Unknown": 0,
        "role-Other": -1,
        }
        return Standardized_JobLevel_Map


    def load_eloquaIndustry_SourceMap(self):
        sourceMap = {'Aerospace': 'industry-Aerospace',
        'Infrastructure': 'industry-Infrastructure',
        'Automotive & Tire': 'industry-Automotive_Tire',
        'Cement': 'industry-Cement',
        'Chemical': 'industry-Chemical',
        'Entertainment': 'industry-Entertainment',
        'Fibers & Textiles': 'industry-Fibers_Textiles',
        'Food & Beverage': 'industry-Food_Beverage',
        'Glass': 'industry-Glass',
        'HVAC': 'industry-HVAC',
        'Household & Personal,Care': 'industry-Household_Personal_Care',
        'Life Sciences': 'industry-Life_Sciences',
        'Marine': 'industry-Marine',
        'Metals': 'industry-Metals',
        'Mining': 'industry-Mining',
        'Oil & Gas': 'industry-Oil_Gas',
        'Power Generation': 'industry-Power_Generation',
        'Print & Publishing': 'industry-Print_Publishing',
        'Pulp & Paper': 'industry-Pulp_Paper',
        'Semiconductor': 'industry-Semiconductor',
        'Whs EComm & Dist': 'industry-Whs_EComm_Dist',
        'Waste Management': 'industry-Waste_Management',
        'Water Wastewater': 'industry-Water_Wastewater',
        'Other': 'industry-Other'} 
        # label_map = {k: ("industry-" + "_".join(re.findall("\w+", k))) for k,v in Standardized_eloquaIndustryMap.items()}
        return sourceMap
        
        
    def load_Standardized_eloquaIndustryMap(self):

        Standardized_eloquaIndustryMap = {'industry-Aerospace': 1,
        'industry-Infrastructure': 13,
        'industry-Automotive_Tire': 21,
        'industry-Cement': 3,
        'industry-Chemical': 4,
        'industry-Entertainment': 5,
        'industry-Fibers_Textiles': 6,
        'industry-Food_Beverage': 7,
        'industry-Glass': 8,
        'industry-HVAC': 9,
        'industry-Household_Personal_Care': 10,
        'industry-Life_Sciences': 11,
        'industry-Marine': 12,
        'industry-Metals': 14,
        'industry-Mining': 15,
        'industry-Oil_Gas': 16,
        'industry-Power_Generation': 22,
        'industry-Print_Publishing': 18,
        'industry-Pulp_Paper': 19,
        'industry-Semiconductor': 20,
        'industry-Whs_EComm_Dist': 23,
        'industry-Waste_Management': 24,
        'industry-Water_Wastewater': 25,
        'industry-Other': -1}
        # if not self.is_unique_mapper(Standardized_eloquaIndustryMap):
        #     raise "this mapper is not defined correctly"

        return Standardized_eloquaIndustryMap
    
    

    def is_unique_mapper(self, mapper):
        if len(mapper.values()) == len(set(mapper.values())):
            return True
        else:
            return False
        
