import numpy as np
import pandas as pd
from datetime import date
import os

class Analyzer:
    def __init__(self, agg_key, snapshots_files, min_count=-1, export_folder="./"):
        self.agg_key = agg_key
        self.min_count = min_count
        self.snapshots_files = [snapshots_files] if isinstance(snapshots_files, str) else snapshots_files            
        self.today = str(date.today()).replace("-","")
        self.panel_snapshot = None
        self.export_folder = export_folder
        
    def load_accumulated_snapshot(self):
        dts = pd.DataFrame()
        for file in self.snapshots_files:
            print(f"loading {file}")
            dts = pd.concat([dts, pd.read_csv(file)], axis=0)
        result = dts.groupby(self.agg_key).sum()
        self.panel_snapshot = result[result["crmGood"] + result["crmBad"] > self.min_count].reset_index()
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
    def kYieldError(crmGood, crmBad):
        return np.sqrt((crmGood/(crmGood+crmBad))*(1-(crmGood/(crmGood+crmBad)))/(crmGood + crmBad))

    @staticmethod
    def kYieldModified(crmGood, crmBad):
        return crmGood/(crmGood+crmBad) * (1-np.sqrt((crmGood/(crmGood+crmBad))*(1-(crmGood/(crmGood+crmBad)))/(crmGood + crmBad)))

    @staticmethod
    def panel_summary(analysis_data, exclude_products=False):
        calc_rules = {
            "crmGood": lambda x: x["opportunity"] > 0,
            "Unknown": lambda x: x["opportunity"] == 0,
    #         "crmNeutral": lambda x: x["opportunity"] == 0,
            "crmBad": lambda x: x["opportunity"] < 0,
            "eloqua": lambda x: x["existElqContactID"] > 0,
            "anonymousVistor": lambda x: x["opportunity"] != x["opportunity"], # NaN
            "total": lambda x: 1,
        }
        analysis_data = analysis_data.assign(**calc_rules)
        panel = analysis_data.groupby("clean_PageURL")[["crmGood", "Unknown", "crmBad","eloqua", "anonymousVistor", "total"]].sum()

        if exclude_products:
            panel = panel[~panel.index.str.contains("products")]
        return panel

    @staticmethod
    def calc_metrics(panel):
        goodCount = panel[['crmGood']].sum().sum()
        badCount = panel[['crmBad']].sum().sum()
        Subtotal = goodCount + badCount
        probGoodLead = goodCount/Subtotal
        probBadLead = badCount/Subtotal
        assert probGoodLead + probBadLead == 1.0

        metrics_rules = {
            "kYield": lambda x: x["crmGood"]/Subtotal,
            "LeadPartition" : lambda x: x["eloqua"]/x["total"],
            "kYieldError": lambda x: Analyzer.kYieldError(x["crmGood"], x["crmBad"]),
            "kYieldModified": lambda x: Analyzer.kYieldModified(x["crmGood"], x["crmBad"]),
            "Traffic": lambda x: (x["crmGood"] + x["crmBad"])/Subtotal,
        }

        panel_step1 = panel.assign(**metrics_rules)

        integrated_metrics_rules = {
            "pageRank": panel_step1["total"].rank(method="max").astype(int),
            "goodLeadRank": panel_step1["Traffic"].rank(method="max").astype(int),
            "leadRank": panel_step1["LeadPartition"].rank(method="max").astype(int),
            "opportunities": panel_step1["Unknown"] * panel_step1["kYieldModified"],
            "GoodPart": lambda x: panel_step1["kYieldModified"] * panel_step1["Traffic"] / probGoodLead, # numerator of naive bayesian
            "BadPart": lambda x: (1-panel_step1["kYieldModified"]) * panel_step1["Traffic"] / probBadLead, # numerator of naive bayesian
        }
        
        panel_step2 = panel_step1.assign(**integrated_metrics_rules).sort_values(by="kYieldModified", ascending=False)
        
        return panel_step2
