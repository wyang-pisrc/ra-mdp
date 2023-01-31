
from fuzzywuzzy import fuzz
import re 
import pandas as pd

eloquaIndustryMap = {
    "Aerospace - Mfg": "Aerospace",
    "Airports": "Infrastructure",
    "Automotive": "Automotive & Tire",
    "Cement & Aggregate": "Cement",
    "Chemicals & Plastics": "Chemical",
    "Entertainment": "Entertainment",
    "Fibers & Textiles": "Fibers & Textiles",
    "Food & Beverage": "Food & Beverage",
    "Glass": "Glass",
    "HVAC": "HVAC",
    "Household & Personal Care": "Household & Personal,Care",
    "Life Sciences": "Life Sciences",
    "Marine": "Marine",
    "Mass Transit": "Infrastructure",
    "Metals": "Metals",
    "Mining": "Mining",
    "Oil & Gas": "Oil & Gas",
    "Renewable Energy": "Power Generation",
    "Printing & Publishing": "Print & Publishing",
    "Pulp & Paper": "Pulp & Paper",
    "Semiconductor & Electronics": "Semiconductor",
    "Tire & Rubber": "Automotive & Tire",
    "Traditional Power": "Power Generation",
    "Whs EComm & Dist": "Whs EComm & Dist",
    "Waste Management": "Waste Management",
    "Water & Wastewater": "Water Wastewater",
    "Other": "Other",
    ## etc unrelated label updated by wei
    # "Mineria": "Mining",
    # "Government": "Government",
    # "power": "Power Generation",
    # "Cement": "Cement",
    # "Media": "Media",
    # "Grocery": "Grocery"
}
print({v:i for i, (k, v) in enumerate(eloquaIndustryMap.items())})

def fuzzy_preprocessing(x):
    x = " ".join(re.findall("\w+", x)).lower()
    x = x.replace("ing", "")
    return x
    
def fuzzy_match(x, normalized_mapping):
    scores = []
    x = fuzzy_preprocessing(x)
    items = sorted(normalized_mapping.items())
    for key, item in items:
        scores.append(fuzz.token_sort_ratio(x, key.replace("ing", "")))

    max_score = max(scores)
    matched_item, output_item = items[scores.index(max_score)]
    return pd.Series([x, matched_item, output_item, max_score], index=["preprocessing_item", "matched_item", "output_item", "max_score"])

def store_map_manual(mapping_table, df, filename="eloquaIndustryMap_manual"):
    fuzzy_df = df.apply(lambda x: fuzzy_match(x, mapping_table))
    manual_checking = pd.concat([df, fuzzy_df], axis=1).sort_values("max_score")
    manual_checking.to_excel(filename, index=None)

VERSION = "v2.0"
_elq = pd.read_csv("./data/elq_all_bridge-only_20230117.csv.gz", compression="gzip")
_elq["JobLevel"].dropna().drop_duplicates().str.strip().sort_values().to_excel(f"jobLevel_manual_{VERSION}.xlsx", index=None)

industries = _elq["Industry"].dropna().drop_duplicates().str.strip()
store_map_manual(filename=f"eloquaIndustryMap_manual_{VERSION}.xlsx", mapping_table=eloquaIndustryMap, df=industries)
