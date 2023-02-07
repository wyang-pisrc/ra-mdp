from sklearn.preprocessing import LabelEncoder
import numpy as np
import pandas as pd
from datetime import date
import os

class Analyzer:
    def __init__(self, agg_key, snapshots_files, export_folder="./"):
        self.agg_key = agg_key
        self.snapshots_files = [snapshots_files] if isinstance(snapshots_files, str) else snapshots_files            
        self.today = str(date.today()).replace("-","")
        self.panel_snapshot = None
        self.export_folder = export_folder
        
    def load_accumulated_snapshot(self, filter_columns=["lead-Good", "lead-Bad"],  min_count=-1):
        dts = pd.DataFrame()
        for file in self.snapshots_files:
            print(f"loading {file}")
            dts = pd.concat([dts, pd.read_csv(file)], axis=0)
        result = dts.groupby(self.agg_key).sum()
        self.panel_snapshot = result[result[filter_columns].sum(axis=1) > min_count].reset_index()
        return self.panel_snapshot
    
    def save_accumulated_snapshot(self):
        if self.panel_snapshot is None:
            self.load_accumulated_snapshot()
        start = os.path.basename(self.snapshots_files[0]).split("_")[1].split("-")[0]
        end = os.path.basename(self.snapshots_files[-1]).split("_")[1].split("-")[-1]
        report_filename = f"overall_report_{start}-{end}.csv"
        self.panel_snapshot.to_csv(self.export_folder + report_filename, index=None)
        print(f"Report stored: {self.export_folder + report_filename}")
    

    @staticmethod
    def panel_summary(analysis_data, exclude_products=False):
        """
        Currently hard code the mapping and selected columns in here
        """
        calc_rules = {
            "eloqua": lambda x: x["existElqContactID"] == 1,
            "anonymousVistor": lambda x: x["label_lead"] != x["label_lead"], # NaN
            "total": lambda x: 1,

            "lead-Good": lambda x: x["label_lead"] == 1,
            "lead-Unknown": lambda x: x["label_lead"] == 0,
            "lead-Bad": lambda x: x["label_lead"] == -1,

            "role-Csuite": lambda x: x["label_jobLevel"]  == 4,
            "role-Manager": lambda x: x["label_jobLevel"]  == 3,
            "role-Engineer": lambda x: x["label_jobLevel"]  == 2,
            "role-Marketing": lambda x: x["label_jobLevel"]  == -1,
            "role-Unknown": lambda x: x["label_jobLevel"]  == 0,
            "role-Other": lambda x: x["label_jobLevel"]  == -1,
            
            'industry-Aerospace': lambda x: x["label_Industry"] == 1,
            'industry-Infrastructure': lambda x: x["label_Industry"] == 13,
            'industry-Automotive_Tire': lambda x: x["label_Industry"] == 21,
            'industry-Cement': lambda x: x["label_Industry"] == 3,
            'industry-Chemical': lambda x: x["label_Industry"] == 4,
            'industry-Entertainment': lambda x: x["label_Industry"] == 5,
            'industry-Fibers_Textiles': lambda x: x["label_Industry"] == 6,
            'industry-Food_Beverage': lambda x: x["label_Industry"] == 7,
            'industry-Glass': lambda x: x["label_Industry"] == 8,
            'industry-HVAC': lambda x: x["label_Industry"] == 9,
            'industry-Household_Personal_Care': lambda x: x["label_Industry"] == 10,
            'industry-Life_Sciences': lambda x: x["label_Industry"] == 11,
            'industry-Marine': lambda x: x["label_Industry"] == 12,
            'industry-Metals': lambda x: x["label_Industry"] == 14,
            'industry-Mining': lambda x: x["label_Industry"] == 15,
            'industry-Oil_Gas': lambda x: x["label_Industry"] == 16,
            'industry-Power_Generation': lambda x: x["label_Industry"] == 22,
            'industry-Print_Publishing': lambda x: x["label_Industry"] == 18,
            'industry-Pulp_Paper': lambda x: x["label_Industry"] == 19,
            'industry-Semiconductor': lambda x: x["label_Industry"] == 20,
            'industry-Whs_EComm_Dist': lambda x: x["label_Industry"] == 23,
            'industry-Waste_Management': lambda x: x["label_Industry"] == 24,
            'industry-Water_Wastewater': lambda x: x["label_Industry"] == 25,
            'industry-Other':  lambda x: x["label_Industry"] ==-1,

        }
        analysis_data = analysis_data.assign(**calc_rules)
        panel = analysis_data.groupby("clean_PageURL")[["eloqua", "anonymousVistor", "total",
                                                        "lead-Good", "lead-Unknown", "lead-Bad", 
                                                        "role-Csuite", "role-Manager", "role-Engineer", "role-Marketing", "role-Unknown", "role-Other",
                                                        'industry-Aerospace', 'industry-Infrastructure', 'industry-Automotive_Tire', 'industry-Cement', 'industry-Chemical', 'industry-Entertainment', 'industry-Fibers_Textiles', 'industry-Food_Beverage', 'industry-Glass', 'industry-HVAC', 'industry-Household_Personal_Care', 'industry-Life_Sciences', 'industry-Marine', 'industry-Metals', 'industry-Mining', 'industry-Oil_Gas', 'industry-Power_Generation', 'industry-Print_Publishing', 'industry-Pulp_Paper', 'industry-Semiconductor', 'industry-Whs_EComm_Dist', 'industry-Waste_Management', 'industry-Water_Wastewater', 'industry-Other',
                                                    ]].sum()

        if exclude_products:
            panel = panel[~panel.index.str.contains("products")]
        return panel


    @staticmethod
    def calc_naive_bayesian_metrics(panel, target_columns, kYieldModified_only=True):

        Subtotal = panel[target_columns].sum().sum()
        traffic = panel[target_columns].sum(axis=1)/Subtotal # it could be shared through all types of label
        labelProportion = panel[target_columns].sum(axis=0)/Subtotal # [probGoodLead, probBadLead]

        kYieldModifieds = panel[target_columns].apply(lambda x: Analyzer.calc_kYieldModified(x, var_adjusted=True), axis=1)
        kYieldModifieds = pd.DataFrame(kYieldModifieds.tolist(), columns=target_columns, index=kYieldModifieds.index) # expand into columns
    
        if kYieldModified_only:
            metrics = kYieldModifieds
        else: 
            metrics = kYieldModifieds.multiply(traffic, axis="rows").div(labelProportion, axis=1) # bayesian_numerators
        return metrics, traffic, labelProportion
    
    @staticmethod
    def calc_kYieldModified(row, var_adjusted=True):
        """
        @row: all target columns in a specific type of labels e.g. 
        - IsLead: ['lead-Good', 'lead-Bad']
        - JobLevel: ['role-Csuite', 'role-Manager', 'role-Engineer', 'role-Marketing', 'role-Unknown', 'role-Other']
        - Industries: [xxx]
        """
        x = row if isinstance(row, (np.ndarray, np.generic)) else np.array(row)
        kYield = x / (1 if x.sum() == 0 else x.sum()) # incase zero division
        variance_weight = 1 - np.var(kYield) if var_adjusted else 1
        kYieldModified = kYield * variance_weight
        return kYieldModified


    @staticmethod
    def calc_metrics(panel, key_column='clean_PageURL', target_columns=None):
        """
        should exclude unknown label by default
        """
        
        if panel.index.name != key_column:
            assert key_column in panel.columns
            panel.set_index(keys=key_column, inplace=True)
            
        lead_metrics, lead_traffic, lead_labelProportion = Analyzer.calc_naive_bayesian_metrics(panel, target_columns=['lead-Good', 'lead-Bad'],  kYieldModified_only=True)
        role_metrics, role_traffic, role_labelProportion = Analyzer.calc_naive_bayesian_metrics(panel, target_columns=["role-Csuite", "role-Manager", "role-Engineer", "role-Other"],  kYieldModified_only=True)
        industry_metrics, industry_traffic, industry_labelProportion = Analyzer.calc_naive_bayesian_metrics(panel, target_columns=['industry-Aerospace', 'industry-Infrastructure', 'industry-Automotive_Tire', 'industry-Cement', 'industry-Chemical', 'industry-Entertainment', 'industry-Fibers_Textiles', 'industry-Food_Beverage', 'industry-Glass', 'industry-HVAC', 'industry-Household_Personal_Care', 'industry-Life_Sciences', 'industry-Marine', 'industry-Metals', 'industry-Mining', 'industry-Oil_Gas', 'industry-Power_Generation', 'industry-Print_Publishing', 'industry-Pulp_Paper', 'industry-Semiconductor', 'industry-Whs_EComm_Dist', 'industry-Waste_Management', 'industry-Water_Wastewater', 'industry-Other'],  kYieldModified_only=True)
        
        
        
        panel["traffic"] = lead_traffic
        panel["eloquaPartition"] = panel["eloqua"]/panel["total"]
        
        panel_report = panel 
        
        labelProportion = {}
        labelProportion.update(lead_labelProportion)
        labelProportion.update(role_labelProportion)
        labelProportion.update(industry_labelProportion)

        bayesian_metrics = pd.concat([lead_metrics, role_metrics, industry_metrics], axis=1)
        return panel_report, bayesian_metrics, labelProportion

    @staticmethod
    def mcvisid_probs(request_list, le, panel_report, bayesian_metrics, labelProportion):
        """
        should exclude unknown label by default
        """
        
    
        conditional_parts = bayesian_metrics.multiply(panel_report["traffic"], axis="rows").div(labelProportion, axis=1) 
        conditional_parts = conditional_parts[conditional_parts.index.isin(le.classes_)]
        conditional_parts.index = le.transform(conditional_parts.index)

        def mcvisid_prob(request_list, conditional_parts, select_cols = ["lead-Good", "lead-Bad"]):
            conditional_parts_mapper = conditional_parts[select_cols]
            conditional_parts_mapper = np.log(conditional_parts_mapper + 1e-321) # take log and aviod zero
            ratio1 = request_list["page_code"].apply(lambda x: conditional_parts_mapper.iloc[x].sum())
            const = np.array([labelProportion[col] for col in select_cols])
            nominators = np.exp(ratio1 + const)
            probs = nominators.div(nominators.sum(axis=1), axis=0)
            probs.index = request_list["mcvisid"]
            return probs

        mcvisid_prob1 = mcvisid_prob(request_list, conditional_parts, select_cols=["lead-Good", "lead-Bad"])
        mcvisid_prob2 = mcvisid_prob(request_list, conditional_parts, select_cols=["role-Csuite", "role-Manager", "role-Engineer", "role-Other"])
        mcvisid_prob3 = mcvisid_prob(request_list, conditional_parts, select_cols=['industry-Aerospace', 'industry-Infrastructure', 'industry-Automotive_Tire', 'industry-Cement', 'industry-Chemical', 'industry-Entertainment', 'industry-Fibers_Textiles', 'industry-Food_Beverage', 'industry-Glass', 'industry-HVAC', 'industry-Household_Personal_Care', 'industry-Life_Sciences', 'industry-Marine', 'industry-Metals', 'industry-Mining', 'industry-Oil_Gas', 'industry-Power_Generation', 'industry-Print_Publishing', 'industry-Pulp_Paper', 'industry-Semiconductor', 'industry-Whs_EComm_Dist', 'industry-Waste_Management', 'industry-Water_Wastewater', 'industry-Other'])
        
        result = pd.concat([request_list.set_index("mcvisid"), mcvisid_prob1, mcvisid_prob2, mcvisid_prob3], axis=1)
        return result