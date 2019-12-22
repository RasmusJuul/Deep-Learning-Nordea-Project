import virk
from utils import daterange
from datetime import timedelta, date
import os
import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache

# temp imports
import pprint

from tqdm import tqdm

data_save_path = "../data/"
data_year_specific_file = "cvr_to_dates_{0}.csv"
data_year_file_all = "cvr_all_time.csv"


downloaded_company_specific_path = "../data/companies/"

# Paths to save data to:
savt_to_company_specific_path = "../data/filtered_data_grouped/"
save_to_compnay_name_base = "{0}_data.csv"

# Keep cache in case of crashing while working on it!
set_cache(FileCache("../cache_working_subset"))

subset_companies_filename = "companies_subset.csv"

# Keep cache in case of crashing while working on it!
# set_cache(FileCache("../cache_temp"))

# Years interested in

downloaded_campanies_dict = dict()
if os.path.isfile(data_save_path + data_year_file_all):
    with open(data_save_path + data_year_file_all, "r") as inpput:
        inpput.readline()
        for line in inpput:
            line = line.strip().split(",")
            company = int(line[0])
            downloaded_campanies_dict[company] = tuple(line[1:])
else:
    print("No file with path {0}".format(data_save_path + data_year_file_all))


update_for_year = [2019]

downloading_to_check = 0
empty_data = 0
fails = 0
fails_list = list()
print("Stripping years")
for year in update_for_year:
    print("Checking year {0}".format(year))
    with open(data_save_path + "cvr_to_dates_{0}.csv".format(year), "r") as inp:
        i = 0
        inp.readline()
        for line in inp:
            if i%200 == 0:
                print(i)
                print("Downloaded to check so far: ",downloading_to_check)
            i+=1
            line = line.strip().split(",")
            if line[0] != "None":
                company = int(line[0])
                if len(line) > 2:
                    # We may have some ampty data
                    for date_rep in line[1:]:
                        # print(date_rep)
                        try:
                            rep_data = virk.cvrdf(company, day=date_rep)
                            downloading_to_check += 1
                            if rep_data.shape[0] != 0:
                                # There is some data
                                if company in downloaded_campanies_dict:
                                    downloaded_campanies_dict[company] += (date_rep,)
                                else:
                                    downloaded_campanies_dict[company] = (date_rep,)
                            else:
                                # print("Empty data")
                                empty_data += 1
                        except:
                            fails += 1
                            fails_list.append((company,date_rep))
                            # print("Fail: ", company, date_rep)
                            pass
                        # print(rep_data.shape)
                else:
                    # Only one entry this year
                    if company in downloaded_campanies_dict:
                        downloaded_campanies_dict[company] += (line[1],)
                    else:
                        downloaded_campanies_dict[company] = (line[1],)

report_num_stats = dict()

print("Started statistics")
oout_file = ""
for company_cvr in tqdm(downloaded_campanies_dict.keys(), unit='companies'):
    oout_file += (",".join([str(company_cvr)] + list(map(str, [*downloaded_campanies_dict[company_cvr]]))) + "\n")
    if len(downloaded_campanies_dict[company_cvr]) not in report_num_stats:
        report_num_stats[len(downloaded_campanies_dict[company_cvr])] = 1
    else:
        report_num_stats[len(downloaded_campanies_dict[company_cvr])] += 1

print(empty_data)

print("Generating file")
with open(data_save_path+"cvr_all_time.csv","w") as oout:
    oout.write("cvr,dates_list \n")
    oout.write(oout_file)

print("Reports_statistic")
pprint.pprint(report_num_stats)

