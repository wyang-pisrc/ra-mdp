import pandas as pd

from Analyzer import Analyzer
import configparser
from glob import glob
from decimal import getcontext, Decimal
import json

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
    
if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config.read('config.txt')
    snapshots_files = sorted(glob(config["snapshot-export"]["path"]+"*.csv"))
    key = config["analyzer"]["key"]

    print("Start aggregating and analyzing...")
    pageAnalyzer = Analyzer(key, snapshots_files, config["analyzer"].getint("min_count"), config["report-export"]["path"])
    panel_snapshot = pageAnalyzer.load_accumulated_snapshot()
    pageAnalyzer.save_accumulated_snapshot() # store the cumulative result
    report = Analyzer.calc_metrics(panel_snapshot)
    
    # obtain export required metrics
    probGoodLead = (report[["crmGood", "crmBad"]].sum(axis=0)/report[["crmGood", "crmBad"]].sum().sum())["crmGood"]
    b = report[[key] + ["kYieldModified", "Traffic"]].set_index(key)
    getcontext().prec = config["analyzer"].getint("precision")
    b = b.applymap(Decimal)
    url_scores = b.T.to_dict("list")
    
    # export json data
    json_export = {"probGoodLead": Decimal(probGoodLead), "pathMetrics": url_scores}
    with open("url_scores.json", 'wt', encoding='UTF-8') as f:
        json.dump(json_export, f, indent=4, cls=DecimalEncoder)

