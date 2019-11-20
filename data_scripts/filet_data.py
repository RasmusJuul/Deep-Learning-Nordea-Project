import virk
from utils import daterange
from datetime import timedelta, date

import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache

# temp imports
import pprint

data_save_path = "../data/"

# Keep cache in case of crashing while working on it!
set_cache(FileCache("../cache_temp"))

# Years interested in
years = [2012,2013,2014,2015,2016,2017,2018,2019]

cvr_nums_all = dict()

for year in years:
    with open(data_save_path + "cvr_to_dates_{0}.csv".format(year), "r") as inp:
        inp.readline()
        for line in inp:
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
                                print("Empty data")
                        except:
                            print("Fail: ", company, date_rep)
                            pass
                        # print(rep_data.shape)
                else:
                    # Only one entry this year
                    if company in cvr_nums_all:
                        cvr_nums_all[company] += (line[1],)
                    else:
                        cvr_nums_all[company] = (line[1],)

with open(data_save_path+"cvr_all_time.csv","w") as oout:
    oout.write("cvr,dates_list \n")
    for company_cvr in cvr_nums_all.keys():
        oout.write(",".join([str(company_cvr)] + list(map(str, [*cvr_nums_all[company_cvr]]))) + "\n")
