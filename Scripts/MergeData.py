# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 15:27:20 2019

@author: Bruger
"""

import numpy as np
import pandas as pd
import os
from tqdm import tqdm

#companies = pd.read_csv("../Deep-Learning-Nordea-Project-master/data/companies_subset.csv")
ls = os.listdir("../data/filtered_data_grouped_23k")
#data = pd.read_csv("../data/filtered_data_grouped/{}".format(ls[2]))
train = []
target = []
years = 4
for j in tqdm(range(len(ls)), unit='companies'):
    data = pd.read_csv("../data/filtered_data_grouped_23k/{}".format(ls[j]))
    
    data = data.sort_values(by=['Year'])
    
    n = len(data.index)
    for i in range(n-years):
        train.append(data.values[i:i+years])
        target.append(data.values[i+years])


train_ = np.array(train)
target_ = np.array(target)

np.save("data_train_{}".format(years),train_)
np.save("data_target_{}".format(years),target_)