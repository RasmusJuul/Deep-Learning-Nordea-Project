import virk
from utils import daterange
from datetime import timedelta, date

import liquer.ext.basic
import liquer.ext.lq_pandas
from liquer.cache import FileCache, set_cache

# temp imports
import pprint


# Keep cache in case of crashing while working on it!
set_cache(FileCache("cache"))

# First get unique CVR numbers
# between start and end date:
# Format of date in date(year,month,day)
st_date = date(2019, 1, 1)
en_date = date(2019, 11, 18)

# Set of all CVR numbers and dates we aquired them as a tuple
all_company_cvr = dict()

for single_date in daterange(st_date, en_date):
    cur_date = single_date.strftime("%Y%m%d")

    print(cur_date)

    register_df = virk.register(day=cur_date)
    cvr_nums = register_df["cvrNummer"].unique()

    for temp_cvr in cvr_nums:
        if temp_cvr in all_company_cvr:
            all_company_cvr[temp_cvr] += (cur_date,)
        else:
            all_company_cvr[temp_cvr] = (cur_date,)

pprint.pprint(all_company_cvr)

len(all_company_cvr.keys())