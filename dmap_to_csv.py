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

def fetch_concat(ctr_date, localdirfmt, localdict, tmpdir, fnamefmt,
		 remove_extra_file=True, median_filter=False,
		 path_to_filter='./fitexfilter'):

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
    median_filter : bool
        If set to True, data will be filtered by a boxcar median filtere
    path_to_filter : full path to the boxcar filter binary file, including
	the file name.

    
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
        print('Concatenating all the files into one')

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

    # Boxcar filter
    if median_filter:
	fname = boxcar_filter(fname, path_to_filter)
        
    return fname

def boxcar_filter(fname, path_to_filter):
    """Does boxcar filtering to data in a file

    Parameters
    -----------
    fname : str
        Full path of a file (fitacf, fitex).
    path_to_filter : full path to the boxcar filter binary file, including
	the file name.

    Returns
    -------
    ffname : str
        Full path of a data file that is boxcar median filtered. 
	The filtered file name will be fname+"f" 
    
    """

    if fname is not None:
        # extract the data type (e.g., fitacf, fitex, etc.) from fname
        ftype = fname.split(".")[-1]
        if not ftype+'f' in fname:
            try:
                print("boxcar filtering the data")
                # do boxcar filtering
                ffname = fname + 'f'
                command = path_to_filter + ' ' + fname + ' > ' + ffname
                logging.debug("performing: {:s}".format(command))
                os.system(command)
                logging.debug("done filtering")
            except Exception, e:
                estr = 'problem filtering file, using the unfiltered one'
                logging.warning(estr)
        else:
            print("file " + fname + " exists")
            ffname = fname
    else:
        ffname = None
    return ffname

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
    median_filter=False
    path_to_filter = './fitexfilter'

    localdirfmt = "/sd-data/{year}/{ftype}/{radar}/"
    localdict = {"ftype" : ftype, "radar" : rad, "channel" : channel}
    tmpdir = "./data/tmp/"
    fnamefmt = ['{date}.{hour}......{radar}.{channel}.{ftype}',\
                '{date}.{hour}......{radar}.{ftype}']

    # Fetch and concatenate files
    fname = fetch_concat(ctr_date, localdirfmt, localdict, tmpdir, fnamefmt,
                         remove_extra_file=remove_extra_file,
			 median_filter=median_filter,
			 path_to_filter=path_to_filter)
    return fname

if __name__ == "__main__":
    fname = main()

