import urllib.parse
import re
import pandas as pd
import numpy as np

def email_cleanup(table, key="emailaddress1", exclude_test_email=True, drop_pattern=["rockwellautomation","@pisrc.com","@bounteous.com","@ra.rockwell.com","demandbaseexport"]):
    """
    Drop pattern should be None to keep
    
    """
    if key not in table.columns:
        raise "key is not in table.columns"
    drop_pattern = "|".join(drop_pattern)
    drop_rows = None
    table = table.dropna(subset=[key]) # remove nan
    table.loc[:, key] = table[key].str.replace("'", "").str.lower() # lower case, clean single quote
    if exclude_test_email:
        drop_rows = table[(table[key].str.contains(drop_pattern))] # remove rockwell related email
        table = table[(~table[key].str.contains(drop_pattern))] # remove rockwell related email
    return table, drop_rows


def url_filter(url_maps):
    url_maps = url_maps[(url_maps["PageURL"].str.startswith("https://www.rockwellautomation"))
         & (~url_maps["PageURL"].str.contains("\.gif$|\.js$|file://|\/adfs\/|change\-password"))
        ]
    return url_maps

def url_clean(x):
    x = urllib.parse.unquote(x)
    
    # remove special
    x = re.sub(',|;|\'|\"', "", x) # clean special symbol
    x = re.sub("#.*", "", x)  # clean tailing start with
    x = re.sub("https://www.rockwellautomation.(?:com.cn|com)", "/", x) # replace china area website URL
    
    # grouping different language
    x = re.sub("/(global|go)/", "/", x, flags=re.I) 
    # x = re.sub("/[a-z]{2}[-_][a-z]{2,3}$", "", x, flags=re.I)  # grouping homepage with different country code
    x = re.sub("/[a-z]{2}[-_][a-z]{2,3}/|/[a-z]{2}/", "/", x, flags=re.I)  # grouping any children pages with different country code
    
    # remove ending
    x = re.sub("/[a-z]{2}[-_][a-z]{2}[-_][a-z]{2}$", "", x, flags=re.I)  # grouping search page with different country code
    x = re.sub("(\.html)$", "", x, flags=re.I)  # remove html,
    x = re.sub("/+$", "", x)  # clean extra /
    
    # remove extra slice in the middle
    x = re.sub("/+", "/", x).strip()
    return x



def mcvisid_label_assign(_lead, email_mcvisid):
    neutral = 0
    positive = 1
    negative = -1
    print("Assigning labels to crm lead table")
    crm_lead, _ = email_cleanup(_lead, "emailaddress1")

    statecodename_positive_rules = {"Qualified": positive,
                                    "Disqualified": negative,
                                    "Open": neutral
                                    }
    
    statuscodename_positive_rules = {'Assigned to Distribution': positive,
                                    'External Processing': positive,
                                    'Already Active Opportunity': positive,
                                    'Qualified': positive,
                                    'Duplicate Lead': neutral,
                                    'Admin Only: Abandoned by Sales': neutral,
                                    'Does not meet campaign criteria': neutral,
                                    'Not Decision Maker': negative,
                                    'Not buying or influence location': negative,
                                    'No Interest': negative,
                                    'Insufficient information to contact': negative,
                                    'No buying intention': negative,
                                    'Unable to make contact (via phone,email)': negative,
                                    'Unable to make contact': negative,
                                    'No viable contact': negative,
                                    'Max Attempts': negative,
                                    'Competitor/Non RA distributor': negative,
                                    'Selling barrier to high': negative,
                                    'Unable to process': negative,
                                    'Credit hold or watch': negative,
                                    'Not Buying Location': negative,
                                    'No RA solution': negative,
                                    'Bad Contact Information': negative
                                    }
    crm_lead["statecode_signal"] = crm_lead["statecodename"].map(statecodename_positive_rules).fillna(neutral)
    crm_lead["statuscode_signal"] = crm_lead["statuscodename"].map(statuscodename_positive_rules).fillna(neutral)

    def label_assign_rules(x):
        signals = x[["statuscode_signal", "statecode_signal"]]
        pos_sum = (signals>0).sum().sum()
        neg_sum = (signals<0).sum().sum()
        label = None
        if pos_sum >0:
            label = positive
        elif (pos_sum == 0) & (neg_sum > 0):
            label = negative
        else:
            label = neutral
        return label


    ## signal aggregation as label
    email_updated_label = crm_lead.groupby("emailaddress1").apply(lambda x: label_assign_rules(x)) 
    email_updated_label = email_updated_label.reset_index()
    email_updated_label.columns = ["EmailAddress", "opportunity"]

    mcvisid_labels = email_mcvisid.merge(email_updated_label, on="EmailAddress", how="left")
    mcvisid_labels["opportunity"].fillna(neutral, inplace=True)
    # positive_mcvisid = mcvisid_labels[mcvisid_labels["opportunity"] == True]["mcvisid"]
    updated_labels = mcvisid_labels[["mcvisid","opportunity"]].drop_duplicates()
    updated_labels["opportunity"] = updated_labels["opportunity"].astype(int)
    return updated_labels



def identify_pos(user_journey, label_type = 2):
    if label_type == 1:
        pos_code_statuscode = [953810011.0, 3.0, 953810008.0]
        isPos = user_journey["statuscode"].isin(pos_code_statuscode).any()
    elif label_type == 2:
        pos_code_ra_leadstage = [6.0, 7.0, 8.0]
        isPos = user_journey["ra_leadstage"].isin(pos_code_ra_leadstage).any()
    return isPos

def preprocessing_stage(user_journey, stage=1, target_method=0, feature_method="sequential", sep="$"):
    if stage == 1:
        if target_method == 0:
            target_event = [125, 126]
            events = ",".join(user_journey["EventList"].drop_duplicates().tolist())
            y = any([True for e in target_event if "," + str(e) in events]) * 1
        elif target_method == 1:
            y = (~user_journey["EloquaContactId"].isnull()).any()
        else:
            raise "no such target_method"
            
    elif stage == 2:
        y = any(user_journey["label"]) * 1
    
    if feature_method == "bow":
        page_view = user_journey.groupby("BingeScoredAssetPath")["BingeScoredAssetPath"].size().to_dict() # dict vectorizer
    elif feature_method == "sequential":
        page_view = sep.join(user_journey.sort_values(by="DateTime_UTC")["BingeScoredAssetPath"].tolist())
    else:
        raise "no such feature_method"
    
    # init_scores = user_journey.groupby(["BingeScoredAssetPath"])[["BingeCriticalScore","BingeScoredAssetScore"]].mean() # not sure how to append as weighted matrix
#     asset_user_journey_seq = user_journey.sort_values(by="DateTime_UTC")["BingeScoredAssetPath"].tolist()
    return pd.Series([page_view, y], index=["features", "label"])



def add_timestamp_columns(stage1_raw):
    times_mapper = {
        "date": stage1_raw["DateTime_UTC"].str.slice(0, 10).str.replace("-","").astype(int),
        "hour": stage1_raw["DateTime_UTC"].str.slice(11, 13),
        "minute": stage1_raw["DateTime_UTC"].str.slice(14, 16)
    }
    stage1_raw = stage1_raw.assign(**times_mapper)
    return stage1_raw

def aem_raw_preprocessing(aem_raw, valid_mcvisid, drop_mcvisid):
    stage1_raw = aem_raw
    log = []
    
    row1 = stage1_raw.shape[0]
    log.append(row1)

    if drop_mcvisid is not None:
        stage1_raw = stage1_raw[~stage1_raw["mcvisid"].isin(drop_mcvisid["mcvisid"])] # drop irrelevant mcvisid
    row2 = stage1_raw.shape[0]
    log.append(row1 - row2)

    ## deduplicate with minute granularity
    stage1_raw = add_timestamp_columns(stage1_raw)    
    stage1_raw = stage1_raw.drop_duplicates(subset=["mcvisid","date", "hour", "minute", "PageURL"], keep="first")
    row3 = stage1_raw.shape[0]
    log.append(row2 - row3)
    log.append((row2 - row3)/row2)

    ## get clean PageURL and drop irrelevant PageURL - inner join to drop unmatched rows
    url_maps = url_filter(stage1_raw[["PageURL"]].drop_duplicates())
    url_maps["clean_PageURL"] = url_maps["PageURL"].apply(lambda x: url_clean(x))
    # s = url_maps[url_maps["PageURL"].str.contains("details")].sample(100)
    stage1_raw = stage1_raw.merge(url_maps, on="PageURL")
    row4 = stage1_raw.shape[0]
    log.append(row3 - row4)

    ## create labels
    stage1_raw["existElqContactID"] = stage1_raw["mcvisid"].isin(valid_mcvisid) # mark pos and neg mcvisid
    row5 = stage1_raw.shape[0]
    log.append(row5)
    log.append(stage1_raw["mcvisid"].unique().shape[0])
    log.append(stage1_raw["clean_PageURL"].unique().shape[0])
    
    return stage1_raw, log


def search_params_parser(x, tag="keyword"):
    tags = re.findall(f"{tag}=([^;]*)", x)
    x = []
    while "%" in tag :
        tag = urllib.parse.unquote(tag)
        x.append(tag)
    return x