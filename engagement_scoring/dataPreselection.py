import pandas as pd
from glob import glob
from contentScoreShareUtils import email_cleanup


email_mcvisid = pd.read_csv("email_mcvisid.csv")
email_mcvisid = email_cleanup(email_mcvisid, "EmailAddress").drop_duplicates()
files = sorted([file for file in glob("./aem_raw/*.csv") if "aemRaw_keyColumns_2022" in file])
stored_files = sorted([file for file in glob("./target_mcvisid/*.csv") if "aemRaw_keyColumns_2022" in file])

output_folder = "target_mcvisid"

info_list = []

for file in files:
    output_filename = file.replace("aem_raw", output_folder)
    if output_filename in stored_files:
        print(f"already processed this file {output_filename} in the target_mcvisid folder")
        continue
    current = pd.read_csv(file, nrows=None)
    keep_data = current.merge(email_mcvisid, how="inner", on="mcvisid")
    keep_data.to_csv(output_filename, index=None)
    info = [file, output_filename, current.shape[0], keep_data.shape[0]]
    info_list.append(info)
    print(f"{output_filename}, original rows {current.shape[0]} --> keep rows:  {keep_data.shape[0]}")
    
    # del keep_data
info_list_df = pd.DataFrame(info_list, columns = ["Raw_file", "Clean_file", "Original_rows", "Keep_rows"])
info_list_df.to_excel("export_info.xlsx")