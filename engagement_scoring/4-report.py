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
    min_count = config["analyzer"].getint("min_count")

    print("Start aggregating and analyzing...")
    pageAnalyzer = Analyzer(key, snapshots_files, config["report-export"]["path"])
    panel_snapshot = pageAnalyzer.load_accumulated_snapshot(filter_columns=["lead-Good", "lead-Bad"], min_count=min_count)
    pageAnalyzer.save_accumulated_snapshot() # store the cumulative result
    panel_report, bayesian_metrics, labelProportion = Analyzer.calc_metrics(panel_snapshot, key_column=key)
    
    # obtain export required metrics
    # getcontext().prec = 30
    # Pandas default precision
    
    labelProportion = {k: Decimal(v) for k, v in labelProportion.items()}
    pathMetrics = pd.concat([bayesian_metrics, panel_report["traffic"]], axis=1)
    columnSchema = list(pathMetrics.columns)

    pathMetrics_dense = pathMetrics.applymap(Decimal).T.to_dict("list")

    json_export = {
        "labelProportion": labelProportion,
        "columnSchema": columnSchema,
        "pathMetrics": pathMetrics_dense
    }

    with open("page_scores.json", 'wt', encoding='UTF-8') as f:
        json.dump(json_export, f, indent=4, cls=DecimalEncoder)

