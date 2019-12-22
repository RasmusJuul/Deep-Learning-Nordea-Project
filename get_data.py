import virk
from utils import daterange
from datetime import timedelta, date

import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache

# temp imports
import pprint

data_save_path = "data/"

# Keep cache in case of crashing while working on it!
# set_cache(FileCache("cache"))

# Years interested in
# years = [2012,2013,2014,2015,2016,2017,2018,2019]
years = [2019]

for year in years:
    # First get unique CVR numbers
    # between start and end date:
    # Format of date in date(year,month,day)
    st_date = date(year, 1, 1)
    en_date = date(year, 12, 30)

    print(year)

    # Set of all CVR numbers and dates we aquired them as a tuple
    all_company_cvr = dict()

    for single_date in daterange(st_date, en_date):
        cur_date = single_date.strftime("%Y%m%d")

        if cur_date[-2:] == "01":
            print(cur_date)

        register_df = virk.register(day=cur_date)
        cvr_nums = register_df["cvrNummer"].unique()

        for temp_cvr in cvr_nums:
            if temp_cvr in all_company_cvr:
                all_company_cvr[temp_cvr] += (cur_date,)
            else:
                all_company_cvr[temp_cvr] = (cur_date,)

    with open(data_save_path+"cvr_to_dates_{0}.csv".format(year),"w") as outout:
        outout.write("cvr,dates \n")
        for company_cvr in all_company_cvr.keys():
            outout.write(",".join([str(company_cvr)] + list(map(str,[*all_company_cvr[company_cvr]])))+"\n")

    dist = dict()
    for i in all_company_cvr:
        num = len(all_company_cvr[i])
        if num in dist:
            dist[num] += 1
        else:
            dist[num] = 1

    print("{0} year statistic".format(year))
    pprint.pprint(dist)
    print("Number of companies: ",len(all_company_cvr.keys()))