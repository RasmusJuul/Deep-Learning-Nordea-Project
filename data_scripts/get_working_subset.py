import virk
from utils import daterange
from datetime import timedelta, date
import pandas as pd

import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache
import os
from tqdm import tqdm

import random

# temp imports
import pprint

seed = 191963
random.seed(seed)

data_save_path = "../data/"
compnay_name_base = "{0}_data.csv"
company_specific_path = "../data/companies/"

# Keep cache in case of crashing while working on it!
set_cache(FileCache("../cache_working_subset"))

subset_companies_filename = "companies_subset.csv"


# Subset size (We always add aditional that many companies!!)
subset_size = 517

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

# Only run if you dont have a subset file base!!!
# with open(data_save_path+subset_companies_filename,"w") as out:
#     out.write("company \n")

# Check if there is already some data:
already_downloaded = set()
if os.path.isfile(data_save_path+subset_companies_filename):
    with open(data_save_path+subset_companies_filename,"r") as inpput:
        inpput.readline()
        for line in inpput:
            line = line.strip()
            already_downloaded.add(int(line))
else:
    with open(data_save_path + subset_companies_filename, "w") as out:
        out.write("company \n")


def filter_one_df(cvr,day,fields=fields_we_want):
    try:
        rep_data = virk.cvrdf(cvr, day=day)
        # Get data from fields if fields exist!
        all_atributes = list(rep_data.columns)
        columns__ = list()
        for atribute in fields:
            columns__.append(rep_data.get(atribute, pd.Series(index=rep_data.index, name=atribute)))
        end_data = pd.concat(columns__,axis=1)
        end_data["day_downloaded"] = day
        return end_data
    except:
        print("Company {0} for day {1} is invalid".format(cvr,day))
        return


def download_filter_save(cvr,days,fields=fields_we_want,extend=True):
    all_frames = list()
    for i in list(days):
        all_frames.append(filter_one_df(cvr=cvr,
                                        day=i,
                                        fields=fields))
    all_entries = pd.concat(all_frames, axis=0)
    all_entries.to_csv(company_specific_path+compnay_name_base.format(cvr), index=False)
    if extend:
        with open(data_save_path+subset_companies_filename,"a") as out:
            out.write(str(cvr)+"\n")
    return


def download_save(cvr,days,extend=True):
    dir_path = company_specific_path+str(cvr)
    os.mkdir(dir_path)
    for day in list(days):
        with open(dir_path+"/{0}_{1}.csv".format(cvr, day), "w") as inp:
            virk.cvrdf(cvr, day=day).to_csv(path_or_buf=inp, index=False)
    if extend:
        with open(data_save_path+subset_companies_filename,"a") as out:
            out.write(str(cvr)+"\n")
    return
# Create dict of all companies as keys and report dates as its values
# With at leats more_than_n_reports reports
all_valid_companies = dict()
more_than_n_reports = 4

print("Open csv")
with open(data_save_path+"cvr_all_time.csv","r") as iinp:
    iinp.readline()
    for line in iinp:
        line = line.strip().split(",")
        company = int(line[0])
        if len(line)-1 >= more_than_n_reports:
            all_valid_companies[company] = tuple(line[1:])

print("We have all companies")
companies_all_list = list(all_valid_companies.keys())

# Inplace random shuffle (note thet we are using seed so not real random)
random.shuffle(companies_all_list)

i = 0
i = int(input())

# pprint.pprint(already_downloaded)

# subset of companies
for _ in tqdm(range(subset_size), unit="Company"):
    com = companies_all_list[i]
    while com in already_downloaded:
        i+=1
        com = companies_all_list[i]
    # print(com)
    if com not in already_downloaded:
        try:
            download_save(com,all_valid_companies[com])
            already_downloaded.add(com)
        except:
            i+=1


