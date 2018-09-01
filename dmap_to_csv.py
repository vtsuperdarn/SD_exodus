"""
Written by Muhammad on 09/01/2018
"""

import datetime as dt
from davitpy.pydarn.sdio.fetchUtils import fetch_local_files
from davitpy.pydarn.sdio import radDataOpen, radDataReadRec
from davitpy.pydarn.sdio import radDataPtr
import logging
import os
import string
import numpy as np

def fetch_concat(ctr_date, localdirfmt, localdict, tmpdir,
                 fnamefmt, remove_extra_file=True):

    """ fetches files for a single day given by ctr_date,
    then unzips and concatenates them into a single file.
    
    Parameters
    ----------
    ctr_date : datetime.datetime
        a full day for which data are to be read. 
    localdirfmt : str
        string defining the local directory structure
    localdict : dict
        Contains keys for non-time related information
        fnamefmt (eg localdict={'ftype':'fitacf','radar':'sas','channel':'a'})  
    tmpdir : str
        Temporary directory in which to store uncompressed files.	
    fnamefmt : str or list
        Optional string or list of file name formats
        (eg fnamefmt = ['{date}.{hour}......{radar}.{channel}.{ftype}', \
            '{date}.{channel}.{radar}.{ftype}'] 
        or fnamefmt = '{date}.{hour}......{radar}.{ftype}')	
    remove_extra_file : bool
        If set to True, files other than the concatenated file will be removed
    
    Returns
    -------
    str
	full path of the contatenated filename.
    
    """
  
    # construct stime and etime for one-day worthy of data
    stime = ctr_date
    etime = ctr_date + dt.timedelta(days=1)

    # Cteate a tmpdir folder if not exist
    if not os.path.exists(tmpdir):
        os.makedirs(tmpdir)

    # extract info from the localdict argument
    radcode = localdict["radar"]
    ftype = localdict["ftype"]
    channel = localdict["channel"]

    # fetch the data for a given day
    file_list = fetch_local_files(stime, etime, localdirfmt, localdict, tmpdir, fnamefmt)

    # check if we have found files
    if len(file_list) != 0:
        # concatenate the files into a single file
        print('Concatenating all the files in to one')

        # choose a temp file name with time span info for cacheing
        if (channel is None) or (channel == "."):
            fname = '%s%s.%s.%s.%s.%s.%s' % \
                      (tmpdir, stime.strftime("%Y%m%d"),
                       stime.strftime("%H%M%S"),
                       etime.strftime("%Y%m%d"),
                       etime.strftime("%H%M%S"), radcode, ftype)
        else:
            fname = '%s%s.%s.%s.%s.%s.%s.%s' % \
                       (tmpdir, stime.strftime("%Y%m%d"),
                       stime.strftime("%H%M%S"),
                       etime.strftime("%Y%m%d"),
                       etime.strftime("%H%M%S"),
                       radcode, channel, ftype)
        logging.debug('cat ' + string.join(file_list) + ' > ' + fname)
        os.system('cat ' + string.join(file_list) + ' > ' + fname)

        # remove the unneeded files from the tmpdir
        if remove_extra_file:
            print("removing unneeded files")
            for fn in file_list:
                logging.debug('rm ' + fn)
                os.system('rm ' + fn)
                os.system('rm ' + fn+".bz2")
                #os.system('rm ' + fn+".gz")
    else:
        fname = None
        
    return fname

# run the code
def main():

    # Set the logging level
    logging.getLogger().setLevel(logging.WARNING)

    # input parameters
    ctr_date = dt.datetime(2012,12,31)
    rad = "fhe"
    #rad = "ade"
    channel = "."
    ftype = "fitacf"
    remove_extra_file = True 
    localdirfmt = "/sd-data/{year}/{ftype}/{radar}/"
    localdict = {"ftype" : ftype, "radar" : rad, "channel" : channel}
    tmpdir = "./data/tmp/"
    fnamefmt = ['{date}.{hour}......{radar}.{channel}.{ftype}',\
                '{date}.{hour}......{radar}.{ftype}']

    # prepare the data
    fname = fetch_concat(ctr_date, localdirfmt, localdict, tmpdir, fnamefmt,
                         remove_extra_file=remove_extra_file)

    return fname

if __name__ == "__main__":
    fname = main()

