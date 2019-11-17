# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 15:29:44 2019

@author: rasmu
"""
import numpy as np
try:
    from virk import *
    import liquer.ext.basic
    import liquer.ext.lq_pandas
    from liquer.cache import FileCache, set_cache
    set_cache(FileCache("cache"))
except:
    print('already loaded')


register_df = register()
idx = ['entity', 'GrossResult','GrossProfitLoss','ProfitLoss','Revenue','Assets']
compInfo = np.empty((register_df.shape[0],len(idx)))

for (i,cvr_nr) in enumerate(register_df['cvrNummer']):
    temp = cvrdf(cvr_nr)
    for (j,entry) in enumerate(idx):
        try:
            temp2 = temp[entry]
            compInfo[i,j] = np.mean(temp[entry].dropna().values.astype('float'))
        except:
            compInfo[i,j] = 0

# Attemt at getting data of one company from different years (not working)
#cvr_nr = register_df['cvrNummer'][0]
#date = register_df['regnskabsperiode_slutDato'][0].replace('-','')
#date2 = str(int(date[:4])-1)+date[4:]
#temp1 = cvrdf(register_df['cvrNummer'],day=date)
#temp2 = cvrdf(register_df['cvrNummer'],day=date2)