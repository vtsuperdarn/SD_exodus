"""
Written by Muhammad on 09/02/2018
"""

import datetime as dt
import logging
import numpy as np
import pandas as pd
import ast

def csv_to_dict(fname, stime=None, etime=None, sep="|"):

    """Reads data from a csv file and returns a dictionary.

    Parameter
    ---------
    fname : str
        Full path of a csv file.
    stime : Optional[datetime.datetime]
        The start time of interest
    etime : Optional[datetime.datetime]
        The end time of interest.
        If set to None, reads data to the end of a day
    sep : str
        Delimiter to use

    Returns
    -------
    data_dict : dict
        A dictionary object that holds the data

    """

    # Load to a pandas dataframe
    print("Loading csv file to pandas dataframe")
    date_parser = lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    df = pd.read_csv(fname, sep=sep, na_values="None",
                     parse_dates=['time'],
                     date_parser=date_parser)
    
    if stime is not None:
        df = df.loc[df.time >= stime, :]
    if etime is not  None:
        df = df.loc[df.time <= etime, :]

    # Convert to a dict
    print("Converting pandas dataframe to dict")
    data_dict = df.to_dict(orient="list")

    # Convert a string representation of list to a list 
    prm_keys = ["ptab", "ltab"]
    fit_keys = ["elv", "gflg", "nlag", "p_l", "p_l_e", "p_s",
                "p_s_e", "phi0", "phi0_e", "pwr0", "qflg", "slist", "v",
                "v_e", "w_l", "w_l_e", "w_s", "w_s_e"]

    keys_list = prm_keys + fit_keys
    print("Converting string representation of lists to normal lists")
    for ky in keys_list:
        data_dict[ky] = [ast.literal_eval(x) for x in data_dict[ky]]

    return data_dict

# run the code
def main():

    # Set the logging level
    logging.getLogger().setLevel(logging.WARNING)

    # input parameters
    stime = None
    etime = None
    #stime = dt.datetime(2012,12,31)
    #etime = dt.datetime(2012,12,31, 1, 0)

    csv_sep = "|"    # Delimiter to use

    # Convert dmap format to csv
    fname = "./data/tmp/20121231.000000.20130101.000000.fhe.fitacf.csv"
    data_dict = csv_to_dict(fname, stime=stime, etime=etime, sep=csv_sep)

    return data_dict

if __name__ == "__main__":
    data_dict = main()

