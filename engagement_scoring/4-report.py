import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
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
    
    labelProportion_Decimal = {k: Decimal(v) for k, v in labelProportion.items()}
    
    # pathMetrics = pd.concat([np.log(bayesian_metrics), panel_report["traffic"]], axis=1) # log version 
    pathMetrics = pd.concat([bayesian_metrics, panel_report["traffic"]], axis=1)
    columnSchema = list(pathMetrics.columns)

    pathMetrics_Decimal = pathMetrics.applymap(Decimal).T.to_dict("list")
    
    json_export = {
        "labelProportion": labelProportion_Decimal,
        "columnSchema": columnSchema,
        "pathMetrics": pathMetrics_Decimal
    }


    ################################
    # page_score json
    ################################
    with open("page_scores.json", 'wt', encoding='UTF-8') as f:
        json.dump(json_export, f, indent=4, cls=DecimalEncoder)
        
    print("Finished data analyzing and store the json data")
    
    ################################
    # valid url list
    ################################ 
    le = LabelEncoder()
    encode_urls = le.fit_transform(bayesian_metrics.index)
    np.save('./report/encode_urls.npy', le.classes_)
    print("largest code:", max(encode_urls))
    
    
    ################################
    # mcvisid report list
    ################################
    test_sample_size = None
    files = sorted(glob("./data/aemRaw_keyColumns_2022*"))[-6::]
    dfs = pd.DataFrame()
    for file in files:
        print("loading: ", file)
        df = pd.read_csv(file, compression="gzip", usecols=['mcvisid', 'clean_PageURL'], nrows=test_sample_size)
        dfs = pd.concat([dfs, df], axis=0)
    print("Finish loading")
    
    print("Aggregating user request list")
    filter_dfs = dfs[dfs["clean_PageURL"].isin(le.classes_)]
    filter_dfs.loc[:, "page_code"] = le.transform(filter_dfs["clean_PageURL"].tolist())
    request_list = filter_dfs[["mcvisid", "page_code"]].groupby("mcvisid").apply(lambda x: x["page_code"].tolist()).rename("page_code").reset_index()
    request_list.to_csv("./report/mcvisid_request_list.csv")
    
    print("Processing probability report")
    preload = False
    if preload:
        request_list = pd.read_csv("./report/mcvisid_request_list.csv")
        request_list["page_code"] = request_list["page_code"].apply(eval)
    request_list_prob = Analyzer.mcvisid_probs(request_list, le, panel_report, bayesian_metrics, labelProportion)
    request_list_prob.to_csv("./report/mcvisid_request_list_probs-server.csv")
    # request_list_prob.to_excel("./report/mcvisid_request_list_probs.xlsx")
    request_list_prob["request_count"] = request_list_prob["page_code"].apply(len)
    target_list = request_list_prob[(request_list_prob["request_count"]>5) & (request_list_prob["request_count"]<500)]
    target_list.to_excel("./report/mcvisid_request_list_probs_target.xlsx")
    
    ################################
    # validation - merge mcvisid label to compare
    ################################
    
