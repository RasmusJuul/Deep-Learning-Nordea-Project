# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 15:29:44 2019

@author: rasmu
"""

#from virk import *
#import liquer.ext.basic
#import liquer.ext.lq_pandas
#from liquer.cache import FileCache, set_cache
#set_cache(FileCache("cache"))

register_df = register()

temp = cvrdf(register_df['cvrNummer'].values[1])
compInfo = temp.iloc[temp['GrossResult'].dropna().index]

CoI = ['entity','GrossResult','ProfitLoss','ProfitLossFromOrdinaryOperatingActivities'] #'Revenue' '_score'
temp2 = compInfo[CoI]