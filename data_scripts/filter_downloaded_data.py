import virk
from utils import daterange
from datetime import timedelta, date
import pandas as pd

import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache
import os

import random

# temp imports
import pprint

seed = 191963
random.seed(seed)

# ALready downloaded data parameters
data_save_path = "../data/"
downloaded_company_specific_path = "../data/companies/"

# Paths to save data to:
savt_to_company_specific_path = "../data/filtered_data/"
save_to_compnay_name_base = "{0}_data.csv"

# Keep cache in case of crashing while working on it!
set_cache(FileCache("../cache_working_subset"))

subset_companies_filename = "companies_subset.csv"

# Subset size (We always add aditional that many companies!!)

fields_we_want = ("entity",
                  "start_date",
                  "end_date",
                  "context",
                  "AddressOfFinancialDistrictName",
                  "AddressOfFinancialPostCodeIdentifier",
                  "DateOfApprovalOfAnnualReport",
                  "IdentificationNumberCvrOfReportingEntity",
                  # Financial data
                  "ProfitLoss",
                  "GrossResult",
                  "GrossProfitLoss",
                  "Revenue",
                  "Assets"
                  # day_downloaded
                  )

fields_we_want_sum = (
    # Financial data
    "ProfitLoss",
    "GrossResult",
    "GrossProfitLoss",
    "Revenue",
    "Assets"
    # day_downloaded
)

downloaded_campanies = list()
if os.path.isfile(data_save_path + subset_companies_filename):
    with open(data_save_path + subset_companies_filename, "r") as inpput:
        inpput.readline()
        for line in inpput:
            line = line.strip()
            downloaded_campanies.append(int(line))
else:
    print("No file with path {0}".format(data_save_path + subset_companies_filename))


def load_data_and_filter(cvr, fields=fields_we_want):
    cvr_path = downloaded_company_specific_path + str(cvr) + "/"
    # all_files = os.listdir(cvr_path)
    all_files = [f for f in os.listdir(cvr_path) if os.path.isfile(os.path.join(cvr_path, f))]
    file_rows__ = list()
    for file in all_files:
        rep_data = pd.read_csv(file)
        day = file[-12:-4]
        print(day)
        columns__ = list()
        for atribute in fields:
            columns__.append(rep_data.get(atribute, pd.Series(index=rep_data.index, name=atribute)))
        end_data = pd.concat(columns__, axis=1)
        end_data["day_downloaded"] = day
        file_rows__.append(end_data)
    company_data = pd.concat(file_rows__, axis=0)
    with open(savt_to_company_specific_path + save_to_compnay_name_base.format(cvr), "w") as inp:
        company_data.to_csv(path_or_buf=inp, index=False)


def load_data_and_filter_sum(cvr, fields=fields_we_want_sum):
    cvr_path = downloaded_company_specific_path + str(cvr) + "/"
    # all_files = os.listdir(cvr_path)
    all_files = [f for f in os.listdir(cvr_path) if os.path.isfile(os.path.join(cvr_path, f))]
    file_rows__ = list()
    for file in all_files:
        rep_data = pd.read_csv(cvr_path+file)
        day = file[-10:-8]
        columns__ = [day]
        for atribute in fields:
            temp_column = rep_data.get(atribute, pd.Series(index=rep_data.index, name=atribute))
            try:
                to_Append_temp = temp_column.sum()/2
            except:
                to_Append_temp = 0
            columns__.append(to_Append_temp)
        file_rows__.append(columns__)
    file_rows__.sort()
    company_data = pd.DataFrame(file_rows__, columns=["Year"]+list(fields))
    with open(savt_to_company_specific_path + save_to_compnay_name_base.format(cvr), "w") as inp:
        company_data.to_csv(path_or_buf=inp, index=False)


i = 0
acc_percet = 20
percent_done_break = len(downloaded_campanies) // acc_percet
print(percent_done_break)
j = 0
for comp in downloaded_campanies:
    if i % percent_done_break == 0:
        print("{0}% Done".format(j * (100 // acc_percet)))
        j+=1
    load_data_and_filter_sum(comp)
    i += 1
