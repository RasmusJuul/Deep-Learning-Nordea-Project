import virk
from utils import daterange
from datetime import timedelta, date

import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache

# temp imports
import pprint

from tqdm import tqdm

data_save_path = "../data/"

# Keep cache in case of crashing while working on it!
# set_cache(FileCache("../cache_temp"))

# Years interested in
years = [2012,2013,2014,2015,2016,2017,2018,2019]

cvr_nums_all = dict()
empty_data = 0
fails = 0
fails_list = list()
print("Stripping years")
for year in years:
    with open(data_save_path + "cvr_to_dates_{0}.csv".format(year), "r") as inp:
        print("Opened file")
        i = 0
        inp.readline()
        for line in inp:
            if i%100 == 0:
                print(i)
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
                            if rep_data.shape[0] != 0:
                                # There is some data
                                if company in cvr_nums_all:
                                    cvr_nums_all[company] += (date_rep,)
                                else:
                                    cvr_nums_all[company] = (date_rep,)
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
                    if company in cvr_nums_all:
                        cvr_nums_all[company] += (line[1],)
                    else:
                        cvr_nums_all[company] = (line[1],)

report_num_stats = dict()

print("Started statistics")
oout_file = ""
for company_cvr in tqdm(cvr_nums_all.keys(), unit='companies'):
    oout_file += (",".join([str(company_cvr)] + list(map(str, [*cvr_nums_all[company_cvr]]))) + "\n")
    if len(cvr_nums_all[company_cvr]) not in report_num_stats:
        report_num_stats[len(cvr_nums_all[company_cvr])] = 1
    else:
        report_num_stats[len(cvr_nums_all[company_cvr])] += 1

print(empty_data)

print("Generating file")
with open(data_save_path+"cvr_all_time.csv","w") as oout:
    oout.write("cvr,dates_list \n")
    oout.write(oout_file)

print("Reports_statistic")
pprint.pprint(report_num_stats)

