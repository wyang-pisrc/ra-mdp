import pandas as pd
# pd.set_option('display.float_format',lambda x : '%.5f' % x)
import re
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import train_test_split

from sklearn.metrics import roc_curve, auc, precision_recall_curve, classification_report
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sqlalchemy import create_engine
import sqlalchemy
import pymysql
from urllib.parse import quote
from sqlalchemy.types import *

import matplotlib.pyplot as plt
import numpy as np
np.set_printoptions(suppress=True)

from collections import Counter
from sklearn import svm
from sklearn.model_selection import cross_validate
import os
from glob import glob
from tqdm import tqdm
tqdm.pandas()

import warnings
warnings.filterwarnings('ignore') 


def get_summary(table, field):
    print("nan ratio:", table[field].isnull().sum()/table.shape[0])
    print("nan rows: ", table[field].isnull().sum())
    print(table[field].fillna("nan").value_counts())
    
def email_cleanup(table, key="emailaddress1", drop_pattern="rockwell|pisrc|bounteous|test"):
    if key not in table.columns:
        raise "key is not in table.columns"
    table = table.dropna(subset=[key]) # remove nan
    table.loc[:, key] = table[key].str.replace("'", "").str.lower() # lower case, clean single quote
    drop_rows = table[(table[key].str.contains(drop_pattern))] # remove rockwell related email
    table = table[(~table[key].str.contains(drop_pattern))] # remove rockwell related email
    return table, drop_rows


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

def evaluate_metrics(y_test, y_pred, show_plots=False):
    fpr, tpr, thresholds = roc_curve(y_test, y_pred, pos_label=1)
    auc_score = auc(fpr, tpr)
    pre, rec, thresholds = precision_recall_curve(y_test, y_pred, pos_label=1)
    prc_score = auc(rec, pre)

    if show_plots:
        print(classification_report(y_test, y_pred>0.5))
        plt.subplot(1, 2, 1)
        plt.plot(fpr, tpr, color='darkorange', lw=2)
        plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('AUC')

        plt.subplot(1, 2, 2)
        plt.plot(rec, pre, color='darkorange', lw=2)
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.title('PRC')
        plt.tight_layout()

        plt.show()

    print("AUC: {:.2%}, PRC: {:.2%}".format(auc_score, prc_score))
    

def load_csv_batches(glob_pattern="./target_mcvisid/*.csv", num=2, keep="last"):
    """
    glob_pattern could be ./target_mcvisid/*.csv (filtered with target mcvisid) or ./aem_raw/*.csv (raw without any filter)
    """    
    files = sorted([file for file in glob(glob_pattern) if "aemRaw_keyColumns_2022" in file])
    num = -num if keep == "last" else num
    if num is not None:
        files = files[num::]
        print("loading files:", files)
    else:
        print(files)
        isConfirm = input("are u sure to load all of files in this folder (enter Y to confirm)")
        if isConfirm.lower() == "y":
            print("confirmed and loading...")
        else:
            return None

    df = pd.DataFrame()

    for file in files:
        current = pd.read_csv(file)
        df = pd.concat([df, current])
        print("current rows: ", df.shape[0])
    return df

def score_bar_plot(index, x, num_levels = 5, color_opts = ['lightskyblue', 'turquoise', 'orange', 'blue', 'red']):
    num_levels = 5
    color_opts = ['lightskyblue', 'turquoise', 'orange', 'blue', 'red']


    xx = pd.concat([index, x, pd.cut(x, num_levels)], axis=1)
    xx.columns = ["asset", "weight", "bins"]
    xx["bins"] = xx["bins"].map(lambda x: f"[{str(x.left)}, {str(x.right)}]")
    bin_dict = sorted(xx["bins"].unique().tolist())

    colors = dict(zip(bin_dict, color_opts[0:num_levels]))
    labels = dict(zip(bin_dict, range(1, len(bin_dict)+1)))


    xx["color"] = xx["bins"].apply(lambda x: colors[x] if x in colors else "NA")
    xx["auto_score"] = xx["bins"].apply(lambda x: labels[x] if x in colors else "NA")
    xx = xx.reset_index()
    plt.figure(figsize=(20,10))

    for idx, item in xx.groupby("bins"):
        if item.shape[0] ==0:
            continue
        plt.bar(x=item.index, height = item["weight"], color=item["color"], label=item["auto_score"].iloc[0])
    #     break

    for pair in bin_dict:
        upper_line = eval(pair)[1]
        plt.plot([0, xx.shape[0]],[upper_line, upper_line], )


    plt.xlabel("Asset index rows")
    plt.ylabel("Model Assign Weight")
    plt.title("Asset auto standardized score")
    plt.legend()
    plt.show()
    return xx

iso_language = [
    ('aa', 'Afar'),
    ('ab', 'Abkhazian'),
    ('af', 'Afrikaans'),
    ('ak', 'Akan'),
    ('sq', 'Albanian'),
    ('am', 'Amharic'),
    ('ar', 'Arabic'),
    ('an', 'Aragonese'),
    ('hy', 'Armenian'),
    ('as', 'Assamese'),
    ('av', 'Avaric'),
    ('ae', 'Avestan'),
    ('ay', 'Aymara'),
    ('az', 'Azerbaijani'),
    ('ba', 'Bashkir'),
    ('bm', 'Bambara'),
    ('eu', 'Basque'),
    ('be', 'Belarusian'),
    ('bn', 'Bengali'),
    ('bh', 'Bihari languages'),
    ('bi', 'Bislama'),
    ('bo', 'Tibetan'),
    ('bs', 'Bosnian'),
    ('br', 'Breton'),
    ('bg', 'Bulgarian'),
    ('my', 'Burmese'),
    ('ca', 'Catalan; Valencian'),
    ('cs', 'Czech'),
    ('ch', 'Chamorro'),
    ('ce', 'Chechen'),
    ('zh', 'Chinese'),
    ('cu', 'Church Slavic; Old Slavonic; Church Slavonic; Old Bulgarian; Old Church Slavonic'),
    ('cv', 'Chuvash'),
    ('kw', 'Cornish'),
    ('co', 'Corsican'),
    ('cr', 'Cree'),
    ('cy', 'Welsh'),
    ('cs', 'Czech'),
    ('da', 'Danish'),
    ('de', 'German'),
    ('dv', 'Divehi; Dhivehi; Maldivian'),
    ('nl', 'Dutch; Flemish'),
    ('dz', 'Dzongkha'),
    ('el', 'Greek, Modern (1453-)'),
    ('en', 'English'),
    ('eo', 'Esperanto'),
    ('et', 'Estonian'),
    ('eu', 'Basque'),
    ('ee', 'Ewe'),
    ('fo', 'Faroese'),
    ('fa', 'Persian'),
    ('fj', 'Fijian'),
    ('fi', 'Finnish'),
    ('fr', 'French'),
    ('fr', 'French'),
    ('fy', 'Western Frisian'),
    ('ff', 'Fulah'),
    ('Ga', 'Georgian'),
    ('de', 'German'),
    ('gd', 'Gaelic; Scottish Gaelic'),
    ('ga', 'Irish'),
    ('gl', 'Galician'),
    ('gv', 'Manx'),
    ('el', 'Greek, Modern (1453-)'),
    ('gn', 'Guarani'),
    ('gu', 'Gujarati'),
    ('ht', 'Haitian; Haitian Creole'),
    ('ha', 'Hausa'),
    ('he', 'Hebrew'),
    ('hz', 'Herero'),
    ('hi', 'Hindi'),
    ('ho', 'Hiri Motu'),
    ('hr', 'Croatian'),
    ('hu', 'Hungarian'),
    ('hy', 'Armenian'),
    ('ig', 'Igbo'),
    ('is', 'Icelandic'),
    ('io', 'Ido'),
    ('ii', 'Sichuan Yi; Nuosu'),
    ('iu', 'Inuktitut'),
    ('ie', 'Interlingue; Occidental'),
    ('ia', 'Interlingua (International Auxiliary Language Association)'),
    ('id', 'Indonesian'),
    ('ik', 'Inupiaq'),
    ('is', 'Icelandic'),
    ('it', 'Italian'),
    ('jv', 'Javanese'),
    ('ja', 'Japanese'),
    ('kl', 'Kalaallisut; Greenlandic'),
    ('kn', 'Kannada'),
    ('ks', 'Kashmiri'),
    ('ka', 'Georgian'),
    ('kr', 'Kanuri'),
    ('kk', 'Kazakh'),
    ('km', 'Central Khmer'),
    ('ki', 'Kikuyu; Gikuyu'),
    ('rw', 'Kinyarwanda'),
    ('ky', 'Kirghiz; Kyrgyz'),
    ('kv', 'Komi'),
    ('kg', 'Kongo'),
    ('ko', 'Korean'),
    ('kj', 'Kuanyama; Kwanyama'),
    ('ku', 'Kurdish'),
    ('lo', 'Lao'),
    ('la', 'Latin'),
    ('lv', 'Latvian'),
    ('li', 'Limburgan; Limburger; Limburgish'),
    ('ln', 'Lingala'),
    ('lt', 'Lithuanian'),
    ('lb', 'Luxembourgish; Letzeburgesch'),
    ('lu', 'Luba-Katanga'),
    ('lg', 'Ganda'),
    ('mk', 'Macedonian'),
    ('mh', 'Marshallese'),
    ('ml', 'Malayalam'),
    ('mi', 'Maori'),
    ('mr', 'Marathi'),
    ('ms', 'Malay'),
    ('Mi', 'Micmac'),
    ('mk', 'Macedonian'),
    ('mg', 'Malagasy'),
    ('mt', 'Maltese'),
    ('mn', 'Mongolian'),
    ('mi', 'Maori'),
    ('ms', 'Malay'),
    ('my', 'Burmese'),
    ('na', 'Nauru'),
    ('nv', 'Navajo; Navaho'),
    ('nr', 'Ndebele, South; South Ndebele'),
    ('nd', 'Ndebele, North; North Ndebele'),
    ('ng', 'Ndonga'),
    ('ne', 'Nepali'),
    ('nl', 'Dutch; Flemish'),
    ('nn', 'Norwegian Nynorsk; Nynorsk, Norwegian'),
    ('nb', 'Bokmål, Norwegian; Norwegian Bokmål'),
    ('no', 'Norwegian'),
    ('oc', 'Occitan (post 1500)'),
    ('oj', 'Ojibwa'),
    ('or', 'Oriya'),
    ('om', 'Oromo'),
    ('os', 'Ossetian; Ossetic'),
    ('pa', 'Panjabi; Punjabi'),
    ('fa', 'Persian'),
    ('pi', 'Pali'),
    ('pl', 'Polish'),
    ('pt', 'Portuguese'),
    ('ps', 'Pushto; Pashto'),
    ('qu', 'Quechua'),
    ('rm', 'Romansh'),
    ('ro', 'Romanian; Moldavian; Moldovan'),
    ('ro', 'Romanian; Moldavian; Moldovan'),
    ('rn', 'Rundi'),
    ('ru', 'Russian'),
    ('sg', 'Sango'),
    ('sa', 'Sanskrit'),
    ('si', 'Sinhala; Sinhalese'),
    ('sk', 'Slovak'),
    ('sk', 'Slovak'),
    ('sl', 'Slovenian'),
    ('se', 'Northern Sami'),
    ('sm', 'Samoan'),
    ('sn', 'Shona'),
    ('sd', 'Sindhi'),
    ('so', 'Somali'),
    ('st', 'Sotho, Southern'),
    ('es', 'Spanish; Castilian'),
    ('sq', 'Albanian'),
    ('sc', 'Sardinian'),
    ('sr', 'Serbian'),
    ('ss', 'Swati'),
    ('su', 'Sundanese'),
    ('sw', 'Swahili'),
    ('sv', 'Swedish'),
    ('ty', 'Tahitian'),
    ('ta', 'Tamil'),
    ('tt', 'Tatar'),
    ('te', 'Telugu'),
    ('tg', 'Tajik'),
    ('tl', 'Tagalog'),
    ('th', 'Thai'),
    ('bo', 'Tibetan'),
    ('ti', 'Tigrinya'),
    ('to', 'Tonga (Tonga Islands)'),
    ('tn', 'Tswana'),
    ('ts', 'Tsonga'),
    ('tk', 'Turkmen'),
    ('tr', 'Turkish'),
    ('tw', 'Twi'),
    ('ug', 'Uighur; Uyghur'),
    ('uk', 'Ukrainian'),
    ('ur', 'Urdu'),
    ('uz', 'Uzbek'),
    ('ve', 'Venda'),
    ('vi', 'Vietnamese'),
    ('vo', 'Volapük'),
    ('cy', 'Welsh'),
    ('wa', 'Walloon'),
    ('wo', 'Wolof'),
    ('xh', 'Xhosa'),
    ('yi', 'Yiddish'),
    ('yo', 'Yoruba'),
    ('za', 'Zhuang; Chuang'),
    ('zh', 'Chinese'),
    ('zu', 'Zulu')
]

