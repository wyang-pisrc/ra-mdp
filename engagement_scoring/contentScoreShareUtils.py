import pandas as pd
import re
from sklearn.metrics import roc_curve, auc, precision_recall_curve, classification_report
from glob import glob
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore') 

    
def datetime_interval_iterator(event_start_month=4, event_end_month=12, start_year=2022, end_year=2023):

    days = ["01", "15", "31"]
    day_pair = list(zip(days, days[1:]))
    month_max_days = {"02": "28", "04":"30", "06":"30","09":"30","11":"30"}
    
    for current_year in range(start_year, end_year+1):
        if start_year == end_year:
            months = [str(month).zfill(2) for month in range(event_start_month, event_end_month+2)] # +2 for iterator
        elif current_year == end_year:
            months = [str(month).zfill(2) for month in range(1, event_end_month+2)] # +2 for iterator
        else:
            months = [str(month).zfill(2) for month in range(event_start_month, 12+2)] # +2 for iterator
            
        month_pair = list(zip(months, months[1:]))
        for (_start_month, _end_month) in month_pair:
            span = 0
            for (start_day, end_day) in day_pair:
                span +=1
                if span in [1,2]:
                    start_month, end_month = _start_month, _start_month
                if span in [3,4]:
                    start_month, end_month = _end_month, _end_month

                if (end_month in month_max_days.keys()) & (end_day == "31"):
                    end_day = month_max_days[end_month]

                print(f"Range month: {current_year}{start_month}{start_day}-{current_year}{end_month}{end_day}")
                yield current_year, start_month, start_day, end_month, end_day



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

