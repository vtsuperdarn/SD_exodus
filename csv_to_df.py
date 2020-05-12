"""
Written by Muhammad on 24/02/2019
"""

import datetime as dt
import logging
import numpy as np
import pandas as pd
import sys
sys.path.append("./")
from csv_to_dict import csv_to_dict

# input parameters
stime = None
etime = None
#stime = dt.datetime(2012,12,31)
#etime = dt.datetime(2012,12,31, 1, 0)

csv_sep = "|"    # Delimiter to use

# Convert dmap format to csv
fdir = "./data/tmp/"
#fname = fdir + "20130109.180000.20130110.180000.bks.fitacf.csv"
#fname = fdir + "20130110.180000.20130111.180000.fhe.fitacf.csv"
#fname = fdir + "20141102.000000.20141103.000000.bks.fitacf.csv"
#fname = fdir + "20121231.000000.20130101.000000.fhe.fitacf.csv"
#fname = fdir + "20121204.000000.20121205.000000.bks.fitacf.csv"
fname = fdir + "20121205.000000.20121206.000000.bks.fitacf.csv"
fname_out = fname[:-4] + ".vel" +".csv"

data_dict = csv_to_dict(fname, stime=stime, etime=etime, sep=csv_sep)

# Store selected parameters into a data frame
params = ["time", "v", "slist", "bmnum", "bmazm", "gflg", "rsep", "frang"]

# Flatten the data
dct = {}
dct["UT_Time"] = [x for i in range(len(data_dict["time"]))\
                  for x in [data_dict["time"][i]]*len(data_dict["v"][i])]
dct["LOS_Velocity"] = [x for i in range(len(data_dict["time"]))\
                  for x in data_dict["v"][i]]
dct["Range_Gate"] = [x for i in range(len(data_dict["time"]))\
                     for x in data_dict["slist"][i]]
dct["Beam_Number"] = [x for i in range(len(data_dict["time"]))\
                      for x in [data_dict["bmnum"][i]]*len(data_dict["v"][i])]
dct["Beam_Azimuth"] = [x for i in range(len(data_dict["time"]))\
                       for x in [data_dict["bmazm"][i]]*len(data_dict["v"][i])]
dct["Ground_Scatter_Flag"] = [x for i in range(len(data_dict["time"]))\
                              for x in data_dict["gflg"][i]]

df = pd.DataFrame(data=dct)
df.set_index("UT_Time", inplace=True)
df.to_csv(fname_out, index_label="UT_Time")



