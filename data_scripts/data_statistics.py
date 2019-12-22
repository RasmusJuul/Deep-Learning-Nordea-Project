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

# READ ONLY !!

# ALready downloaded data parameters
data_save_path = "../data/"
downloaded_company_specific_path = "../data/companies/"

# Paths to save data to:
savt_to_company_specific_path = "../data/filtered_data_grouped/"
save_to_compnay_name_base = "{0}_data.csv"

# Keep cache in case of crashing while working on it!
set_cache(FileCache("../cache_working_subset"))

subset_companies_filename = "companies_subset.csv"

def get_company_stats(cvr):

    return 0

def analyze_data_and_print(stats):

    return


if __name__ == '__main__':
    # Get downloaded data
    downloaded_campanies = list()
    if os.path.isfile(data_save_path + subset_companies_filename):
        with open(data_save_path + subset_companies_filename, "r") as inpput:
            inpput.readline()
            for line in inpput:
                line = line.strip()
                downloaded_campanies.append(int(line))
    else:
        print("No file with path {0}".format(data_save_path + subset_companies_filename))

    # iterate over companies and and save statistics
    print("Itearting over companies and getting statistic data:")
    statistic_data = list()
    failed = 0
    failed_company_list = list()
    for comp in tqdm(downloaded_campanies, unit='companies'):
        try:
            statistic_data.append(get_company_stats(comp))
        except:
            failed += 1
            failed_company_list.append(comp)

    print("Failed: ", failed)
    print(",".join(map(str,failed_company_list)))

    print("Analyzing data")

    analyze_data_and_print(statistic_data)