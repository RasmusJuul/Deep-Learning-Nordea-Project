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

# ALready downloaded data parameters
data_save_path = "../data/"
downloaded_company_specific_path = "../data/companies/"

# Paths to save data to:
savt_to_company_specific_path = "../data/filtered_data_grouped/"
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
    "Assets"  ## can slo be: CurrentAssets
    # day_downloaded
)

fields_we_want_first = (
    # Financial data
    "ProfitLoss",
    # "GrossResult",
    "GrossProfitLoss",
    # "Revenue",
    "Assets",  # can also be: CurrentAssets
    "AverageNumberOfEmployees",
    "CurrentAssets",
    "Equity",
    # "Inventories",
    "AddressOfReportingEntityPostCodeIdentifier"
    # day_downloaded
)

fields_we_want_first_group = [
    ["ProfitLoss"],
    ["GrossResult", "GrossProfitLoss"],
    ["Assets", "CurrentAssets"],
    ["AverageNumberOfEmployees"],
    ["Equity"],
    ["AddressOfReportingEntityPostCodeIdentifier"]
]

fields_others_are_using = (
    # PS
    "Revenue",
    "DepreciationAmortisationExpenseAndImpairmentLossesOfPropertyPlantAndEquipmentAndIntangibleAssetsRecognisedInProfitOrLoss",
    "EmployeeBenefitsExpense",
    "ExternalExpenses",
    "GrossProfitLoss",
    "GrossResult",
    "ImpairmentOfFinancialAssets",
    "OtherFinanceIncome",
    "ProfitLoss",
    "ProfitLossFromOrdinaryActivitiesBeforeTax",
    "ProfitLossFromOrdinaryOperatingActivities",
    "Provisions",
    "ProvisionsForDeferredTax",
    "ShorttermTaxPayables",
    "TaxExpense"
    # BS
    "Assets",
    "CashAndCashEquivalents",
    "ContributedCapital",
    "CurrentAssets",
    "Equity",
    "LiabilitiesAndEquity",
    "LiabilitiesOtherThanProvisions",
    "LongtermInvestmentsAndReceivables",
    "NoncurrentAssets",
    "OtherLongtermInvestments",
    "OtherShorttermReceivables",
    "ProposedDividend",
    "ProposedDividendRecognisedInEquity",
    "RetainedEarnings",
    "ShorttermLiabilitiesOtherThanProvisions",
    "ShorttermReceivables",
    "CurrentDeferredTaxAssets",
    "FixturesFittingsToolsAndEquipment",
    "OtherShorttermPayables",
    "PropertyPlantAndEquipment",
    "ShorttermEquityLoan",
    "ShorttermPayablesToAssociates",
    "ShorttermTradePayables"
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
        rep_data = pd.read_csv(cvr_path + file)
        day = file[-10:-8]
        columns__ = [day]
        for atribute in fields:
            temp_column = rep_data.get(atribute, pd.Series(index=rep_data.index, name=atribute))
            try:
                to_Append_temp = temp_column.sum() / 2
            except:
                to_Append_temp = 0
            columns__.append(to_Append_temp)
        file_rows__.append(columns__)
    file_rows__.sort()
    company_data = pd.DataFrame(file_rows__, columns=["Year"] + list(fields))
    with open(savt_to_company_specific_path + save_to_compnay_name_base.format(cvr), "w") as inp:
        company_data.to_csv(path_or_buf=inp, index=False)


def load_data_and_filter_first_num(cvr, fields=fields_we_want_first):
    cvr_path = downloaded_company_specific_path + str(cvr) + "/"
    # all_files = os.listdir(cvr_path)
    all_files = [f for f in os.listdir(cvr_path) if os.path.isfile(os.path.join(cvr_path, f))]
    file_rows__ = list()
    for file in all_files:
        rep_data = pd.read_csv(cvr_path + file)
        day = file[-10:-8]
        start_dates = list(rep_data.get("start_date", pd.Series(index=rep_data.index, name="start_date")))
        end_dates = list(rep_data.get("end_date", pd.Series(index=rep_data.index, name="end_date")))
        unique_start_dates = sorted(set([i for i, j in zip(start_dates, end_dates) if i != j]))
        # unique_end_dates = sorted(set([j for i, j in zip(start_dates, end_dates) if i != j]))
        columns__ = [[day, unique_start_dates[i]] for i in range(len(unique_start_dates))]
        for atribute in fields:
            already_seen = [False for _ in range(len(unique_start_dates))]
            temp_vals = [0. for _ in range(len(unique_start_dates))]
            temp_column = rep_data.get(atribute, pd.Series(index=rep_data.index, name=atribute))
            index_non_first = temp_column.first_valid_index()
            if index_non_first is not None:
                for index, indikator in temp_column.notnull().iteritems():
                    if indikator:
                        if start_dates[index] in unique_start_dates:
                            if not already_seen[unique_start_dates.index(start_dates[index])]:
                                already_seen[unique_start_dates.index(start_dates[index])] = True
                                temp_vals[unique_start_dates.index(start_dates[index])] = temp_column[index]

                    else:
                        # Not a number!!
                        pass
            for i, tval in enumerate(temp_vals):
                columns__[i].append(tval)
            asdwa = 7
        for col__ in columns__:
            file_rows__.append(col__)
    file_rows__.sort()
    company_data = pd.DataFrame(file_rows__, columns=["Year", "Start_date"] + list(fields))
    with open(savt_to_company_specific_path + save_to_compnay_name_base.format(cvr), "w") as inp:
        company_data.to_csv(path_or_buf=inp, index=False)


def load_data_and_filter_group(cvr, fields_group=fields_we_want_first_group):
    cvr_path = downloaded_company_specific_path + str(cvr) + "/"
    # all_files = os.listdir(cvr_path)
    all_files = [f for f in os.listdir(cvr_path) if os.path.isfile(os.path.join(cvr_path, f))]
    file_rows__ = list()
    for file in all_files:
        rep_data = pd.read_csv(cvr_path + file)
        day = file[-10:-8]
        start_dates = list(rep_data.get("start_date", pd.Series(index=rep_data.index, name="start_date")))
        end_dates = list(rep_data.get("end_date", pd.Series(index=rep_data.index, name="end_date")))
        unique_start_dates = sorted(set([i for i, j in zip(start_dates, end_dates) if i != j]))

        # print("More than two years !!!: ",cvr)

        # unique_end_dates = sorted(set([j for i, j in zip(start_dates, end_dates) if i != j]))
        columns__ = [[day, unique_start_dates[i]] for i in range(len(unique_start_dates))]
        for atributes in fields_group:
            atribute_group_seen = False
            already_seen = [False for _ in range(len(unique_start_dates))]
            temp_vals = [0. for _ in range(len(unique_start_dates))]
            for atribute in atributes:
                temp_column = rep_data.get(atribute, pd.Series(index=rep_data.index, name=atribute))
                index_non_first = temp_column.first_valid_index()
                if index_non_first is not None and atribute_group_seen is False:
                    atribute_group_seen = True
                    for index, indikator in temp_column.notnull().iteritems():
                        if indikator:
                            if start_dates[index] in unique_start_dates:
                                if not already_seen[unique_start_dates.index(start_dates[index])]:
                                    already_seen[unique_start_dates.index(start_dates[index])] = True
                                    temp_vals[unique_start_dates.index(start_dates[index])] = temp_column[index]

                        else:
                            # Not a number!!
                            pass
            for i, tval in enumerate(temp_vals):
                columns__[i].append(tval)
        for col__ in columns__:
            file_rows__.append(col__)
    file_rows__.sort(reverse=True)
    file_rows___ = list()
    # Filter out "repeating" rows
    seen_years = set()
    for row in file_rows__:
        if row[0] not in seen_years:
            seen_years.add(row[0])
            file_rows___.append(row)
    file_rows___.append(file_rows__[-1])
    file_rows___.sort()
    company_data = pd.DataFrame(file_rows___, columns=["Year", "Start_date"] + [i[0] for i in fields_group])
    with open(savt_to_company_specific_path + save_to_compnay_name_base.format(cvr), "w") as inp:
        company_data.to_csv(path_or_buf=inp, index=False)


i = 0
acc_percet = 20
percent_done_break = len(downloaded_campanies) // acc_percet
# print(percent_done_break)
j = 0
failed = 0
failed_company_list = list()

print("Filtering the data:")
for comp in tqdm(downloaded_campanies, unit='companies'):
    try:
        load_data_and_filter_group(comp)
    except:
        failed += 1
        failed_company_list.append(comp)

print("Failed: ", failed)
pprint.pprint(failed_company_list)
