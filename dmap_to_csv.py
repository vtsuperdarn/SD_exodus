"""
Written by Muhammad on 09/01/2018
"""

import datetime as dt
#from davitpy.pydarn.sdio.fetchUtils import fetch_local_files
from davitpy.pydarn.sdio import radDataPtr, radDataReadRec
from davitpy.pydarn.sdio import radDataPtr
from davitpy import pydarn
import logging
import os
import glob
import string
from time import clock
import numpy as np
from dateutil.relativedelta import relativedelta

def uncompress_file(filename, outname=None, remove=False):
    """
    A function to perform an appropriate type of uncompression on a specified 
    file.  Current extensions include: bz2, gz, zip. This function does not 
    removed the compressed file. Requires bunzip2, gunzip, and unzip to be 
    installed.
    Parameters
    -----------
    filename : (str)
        Name of the compressed file
    outname : (NoneType/str)
        Compressed name of the desired output file (allows uncompressed file to
        be placed in a different location) or None (if the uncompressed file
        will stay in the same directory).  (default=None)
    remove : (bool)
        Remove compressed file after uncompression (default=False)
    Returns
    ---------
    outname : (NoneType/str)
        name of uncompressed file or None if the command was unsuccessful or
        the compression method could not be determined
    """
    import os

    # Check the inputs
    assert isinstance(filename, str), logging.error('filename must be a string')
    assert isinstance(outname, (str, type(None))), \
        logging.error('outname must be a string or None')
    assert isinstance(remove, bool), \
        logging.error('remove status must be Boolian')

    command = None  # Initialize command as None. It will be updated 
                    # if a known file compression is found.

    if outname is None:
        outname = filename

    if filename.find('.bz2') != -1:
        outname = outname.replace('.bz2', '')
        command = 'bunzip2 -c ' + filename + ' > ' + outname
    elif filename.find('.gz') != -1:
        outname = outname.replace('.gz', '')
        command = 'gunzip -c ' + filename + ' > ' + outname
    elif filename.find('.zip') != -1:
        outname = outname.replace('.zip', '')
        command = 'unzip -c ' + filename + ' > ' + outname
    #elif filename.find('.tar') != -1:
    #    outname = outname.replace('.tar', '')
    #    command = 'tar -xf ' + filename

    if command is not None:
        try:
            os.system(command)
            logging.info("performed [{:s}]".format(command))
        except:
            logging.warning("unable to perform [{:s}]".format(command))
            # Returning None instead of setting outname=None to avoid
            # messing with inputted outname variable
            return None

        if remove:
            command = 'rm {:s}'.format(filename)

            try:
                os.system(command)
                logging.info("performed [{:s}]".format(command))
            except:
                logging.warning("unable to perform [{:s}]".format(command))
    else:
        return None
        estr = "unknown compression type for [{:s}]".format(filename)
        logging.warning(estr)

    return outname

def fetch_local_files(stime, etime, localdirfmt, localdict, outdir, fnamefmt,
                      back_time=relativedelta(years=1), remove=False):

    """
    A routine to locate and retrieve file names from locally stored SuperDARN 
    radar files that fit the input criteria.
    Example
    -------
    Fetches one locally stored fitacf file stored in a directory structure
    given by localdirfmt. The file name format is specified by the fnamefmt 
    arguement.
    ::
        from pydarn.sdio import fetchUtils
        import datetime as dt
        import os
        filelist = fetchUtils.fetch_local_files(dt.datetime(2002,6,20), \
            dt.datetime(2002,6,21), '/sd-data/{year}/{month}/', \
            {'ftype':'fitacf'}, \
            "/tmp/sd/",'{date}.{hour}......{radar}.{channel}.{ftype}')
    Parameters
    ------------
    stime : (datetime)
        data starting time
    etime : (datetime)
        data ending time
    localdirfmt : (str)
        string defining the local directory structure
        (eg "{ftype}/{year}/{month}/{day}/")
    localdict : (dict)
        Contains keys for non-time related information in remotedirfmt and
        fnamefmt (eg remotedict={'ftype':'fitex','radar':'sas','channel':'a'})
    outdir : (str)
        Temporary directory in which to store uncompressed files (must end with
        a "/")
    fnamefmt : (str/list)
        Optional string or list of file name formats
        (eg fnamefmt = ['{date}.{hour}......{radar}.{channel}.{ftype}', \
            '{date}.C0.{radar}.{ftype}'] 
        or fnamefmt = '{date}.{hour}......{radar}.{ftype}')
    back_time : (dateutil.relativedelta.relativedelta)
        Time difference from stime that fetchUtils should search backwards
        until before giving up. (default=relativedelta(years=1))
    remove : (bool)
        Remove compressed file after uncompression (default=False)
    Returns
    --------
    file_stime : (datetime)
        actual starting time for located files
    filelist : (list)
        list of uncompressed files (including path)
    Note
    ------
    Weird edge case behaviour occurs when attempting to fetch all channel data
    (e.g. localdict['channel'] = '.').
    """
    import os
    import glob
    import re

    filelist = []
    temp_filelist = []

    # Test input
    assert isinstance(stime, dt.datetime), \
        logging.error('stime must be datetime object')
    assert isinstance(etime,dt.datetime), \
        logging.error('eTime must be datetime object')
    assert isinstance(localdirfmt, str) and localdirfmt[-1] == "/", \
        logging.error('localdirfmt must be a string ending in "/"')
    assert isinstance(outdir, str) and outdir[-1] == "/", \
        logging.error('outdir must be a string ending in "/"')
    assert os.path.isdir(outdir), logging.error("outdir is not a directory")
    assert isinstance(fnamefmt, (str, list)), \
        logging.error('fnamefmt must be str or list')

    #--------------------------------------------------------------------------
    # If fnamefmt isn't a list, make it one.
    if isinstance(fnamefmt,str):
        fnamefmt = [fnamefmt]

    #--------------------------------------------------------------------------
    # Initialize the start time for the loop
    ctime = stime.replace(second=0, microsecond=0)
    time_reverse = 1
    mintime = ctime - back_time

    # construct a checkstruct dictionary to detect if changes in ctime
    # lead to a change in directory to limit how often directories are listed
    time_keys = ["year", "month", "day", "hour", "min", "date"]
    keys_in_localdir = [x for x in time_keys if localdirfmt.find('{'+x+'}') > 0]

    checkstruct = {}
    for key in keys_in_localdir:
        checkstruct[key] = ''

    while ctime <= etime:
        # set the temporal parts of the possible local directory structure
        localdict["year"] = "{:04d}".format(ctime.year)
        localdict["month"] = "{:02d}".format(ctime.month)
        localdict["day"] = "{:02d}".format(ctime.day)
        localdict["hour"] = ctime.strftime("%H")
        localdict["min"] = ctime.strftime("%M")
        localdict["date"] = ctime.strftime("%Y%m%d")
        
        # check for a directory change
        dir_change = 0
        for key in keys_in_localdir:
            if (checkstruct[key] != localdict[key]):
                checkstruct[key] = localdict[key]    
                dir_change = 1
        else:
            # If there is no time structure to local directory structure,
            # only the first time will need a directory change
            if ctime <= stime:
                dir_change = 1

        # get the files in the directory if directory has changed
        if dir_change:
            # Local directory will be correct even if there is no date structure
            local_dir = localdirfmt.format(**localdict)
            try:
                files = os.listdir(local_dir)
            except:
                files = []

        # check to see if any files in the directory match the fnamefmt
        for namefmt in fnamefmt:
            # create a regular expression to check for the desired files
            name = namefmt.format(**localdict)
            regex = re.compile(name)

            # Go thorugh all the files in the directory
            for lf in files:
                #if we have a file match between a file and our regex
                if(regex.match(lf)):
                    if lf in temp_filelist: 
                        continue
                    else:
                        temp_filelist.append(lf)

                    # copy the file to outdir
                    outname = os.path.join(outdir,lf)
                    command='cp {:s} {:s}'.format(os.path.join(local_dir, lf),
                                                  outname)
                    try:
                        os.system(command)
                        logging.info("performed [{:s}]".format(command))
                    except:
                        estr = "unable to perform [{:s}]".format(command)
                        logging.warning(estr)

        # Advance the cycle time by the "lowest" time increment 
        # in the namefmt (either forward or reverse)
        if (time_reverse == 1 and len(temp_filelist) > 0) or ctime < mintime:
            time_reverse = 0
            ctime = stime.replace(second=0, microsecond=0)

        # Calculate if we are going forward or backward in time and set
        # ctime accordingly
        base_time_inc = 1 - 2 * time_reverse        

        if "{min}" in namefmt:
            ctime = ctime + relativedelta(minutes=base_time_inc)
        elif "{hour}" in namefmt:
            ctime = ctime + relativedelta(hours=base_time_inc)
        elif "{date}" in namefmt or "{day}" in remotedirfmt:
            ctime = ctime + relativedelta(days=base_time_inc)
        elif "{month}" in namefmt:
            ctime = ctime + relativedelta(months=base_time_inc)
        elif "{year}" in namefmt:    
            ctime = ctime + relativedelta(years=base_time_inc)

    # Make sure the found files are in order.  Otherwise the concatenation later
    # will put records out of order
    temp_filelist = sorted(temp_filelist)

    # attempt to unzip the files
    for lf in temp_filelist:
        outname = os.path.join(outdir, lf)
        uncompressed = uncompress_file(outname, None, remove=remove)

        if (type(uncompressed) is str):
        # save name of uncompressed file for output
            filelist.append(uncompressed)
        else:
        # file wasn't compressed, use outname
            filelist.append(outname)

    # Return the list of uncompressed files
    return filelist


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
    #file_list = fetch_local_files(stime, etime, localdirfmt, localdict, tmpdir, fnamefmt)
    ###################
    # Due to a bug related to davitpy, here is a walkaround to find the list of files need
    # Note: the .bz files have to be manually copied from sd-data to the folder defined by localdirfmt	

    file_list = fetch_local_files(stime, etime, localdirfmt, localdict, tmpdir, fnamefmt)

    #file_list = glob.glob(os.path.join(tmpdir, '*bz2'))
    ###################

    # Make sure all the fetched files have desired ftype
    file_list = [x for x in file_list if ftype in x]

    # check if we have found files
    if len(file_list) != 0:
        # concatenate the files into a single file
        print("Concatenating all the " + ftype + " files into one")

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
            print("removing unneeded " + ftype + " files")
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
    """Does boxcar median filtering to data in a file.

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


def dmap_to_csv(fname, stime, etime=None, sep="|",
                fileType="fitacf", readOnly=False):

   
    """Reads data from a dmap file and writes it to
    a csv file.
    Parameter
    ---------
    fname : str
        Full path of a dmap file (fitacf, fitex).
    stime : datetime.datetime
        The start time of interest
    etime : datetime.datetime
        The end time of interest
    sep : str
        Delimiter to use
    fileType : str
        SuperDARN fit data type (e.g., fitacf)
    Returns
    -------
    fname_csv : str 
        Full path (including the file name) of a csv file
    """

    # Get a file poiter
    myPtr = radDataPtr(sTime=stime, eTime=etime, fileName=fname, fileType=fileType)

    # Parameter names in a fitacf file
    header = sep.join(["time", "bmnum", "channel", "stid", "cp", "lmfit" , "fitex",
                       "exflg", "iqflg", "offset", "lmflg", "rawflg", "fType",
                       "acflg", "fitacf",                 # upto here are params in myBeam
                       "elv", "gflg", "nlag", "npnts", "p_l", "p_l_e", "p_s",
                       "p_s_e", "phi0", "phi0_e", "pwr0", "qflg", "slist", "v",
                       "v_e", "w_l", "w_l_e", "w_s", "w_s_e",  # upto here are params in myBeam.fit
                       "bmazm", "frang", "ifmode", "inttsc", "inttus", "lagfr",
                       "ltab", "mpinc", "mplgexs", "mplgs", "mppul", "nave", "noisemean",
                       "noisesearch", "noisesky", "nrang", "ptab", "rsep", "rxrise",
                       "scan", "smsep", "tfreq", "txpl", "xcf"]) # upto here are params in myBeam.prm

    # Output file name
    fname_csv = fname + ".csv"

    # Read the parameters of interest.
    try:
        myPtr.rewind()
    except Exception as e:
        logging.error(e)

    myBeam = myPtr.readRec()
    with open(fname_csv, "w") as f:
        f.write(header +"\n")
        while(myBeam is not None):
            if(myBeam.time > myPtr.eTime): break
            if(myPtr.sTime <= myBeam.time):

                # Params in myBeam
                time = str(myBeam.time).split(".")[0]    # Remove millisecond part
                bmnum = str(myBeam.bmnum)
                stid = str(myBeam.stid)
                cp = str(myBeam.cp)
                channel = str(myBeam.channel)
                lmfit = str(myBeam.lmfit)
                fitex = str(myBeam.fitex)
                exflg = str(myBeam.exflg)
                iqflg = str(myBeam.iqflg)
                offset = str(myBeam.offset)
                lmflg = str(myBeam.lmflg)
                rawflg = str(myBeam.rawflg)
                fType = str(myBeam.fType)
                acflg = str(myBeam.acflg)
                fitacf = str(myBeam.fitacf)

                # Params in myBeam.fit
                elv = "[]" if myBeam.fit.elv is None else str(myBeam.fit.elv) 
                gflg = "[]" if myBeam.fit.gflg is None else str(myBeam.fit.gflg)
                nlag = "[]" if myBeam.fit.nlag is None else str(myBeam.fit.nlag)
                npnts = str(myBeam.fit.npnts)
                p_l = "[]" if myBeam.fit.p_l is None else str(myBeam.fit.p_l)
                p_l_e = "[]" if myBeam.fit.p_l_e is None else str(myBeam.fit.p_l_e)
                p_l_e = p_l_e.replace("inf", "999999")
                p_s = "[]" if myBeam.fit.p_s is None else str(myBeam.fit.p_s)
                p_s_e = "[]" if myBeam.fit.p_s_e is None else str(myBeam.fit.p_s_e)
                p_s_e = p_s_e.replace("inf", "999999")
                phi0 = "[]" if myBeam.fit.phi0 is None else str(myBeam.fit.phi0)
                phi0_e = "[]" if myBeam.fit.phi0_e is None else str(myBeam.fit.phi0_e)
                phi0_e = phi0_e.replace("inf", "999999")
                pwr0 = "[]" if myBeam.fit.pwr0 is None else str(myBeam.fit.pwr0)
                qflg = "[]" if myBeam.fit.qflg is None else str(myBeam.fit.qflg)
                slist = str(myBeam.fit.slist)
                v = "[]" if myBeam.fit.v is None else str(myBeam.fit.v)
                v_e = "[]" if myBeam.fit.v_e is None else str(myBeam.fit.v_e)
                v_e = v_e.replace("inf", "999999")
                w_l = "[]" if myBeam.fit.w_l is None else str(myBeam.fit.w_l)
                w_l = w_l.replace("inf", "999999")
                w_l_e = "[]" if myBeam.fit.w_l_e is None else str(myBeam.fit.w_l_e)
                w_l_e = w_l_e.replace("inf", "999999")
                w_s = "[]" if myBeam.fit.w_s is None else str(myBeam.fit.w_s)
                w_s = w_s.replace("inf", "999999")
                w_s_e = "[]" if myBeam.fit.w_s_e is None else str(myBeam.fit.w_s_e)
                w_s_e = w_s_e.replace("inf", "999999")

                # Params in myBeam.prm
                bmazm = str(myBeam.prm.bmazm)
                frang = str(myBeam.prm.frang)
                ifmode = str(myBeam.prm.ifmode)
                inttsc = str(myBeam.prm.inttsc)
                inttus = str(myBeam.prm.inttus)
                lagfr = str(myBeam.prm.lagfr)
                ltab = "[]" if myBeam.prm.ltab is None else str(myBeam.prm.ltab)
                mpinc = str(myBeam.prm.mpinc)
                mplgexs = str(myBeam.prm.mplgexs)
                mplgs = str(myBeam.prm.mplgs)
                mppul = str(myBeam.prm.mppul)
                nave = str(myBeam.prm.nave)
                noisemean = str(myBeam.prm.noisemean)
                noisesearch = str(myBeam.prm.noisesearch)
                noisesky = str(myBeam.prm.noisesky)
                nrang = str(myBeam.prm.nrang)
                ptab = "[]" if myBeam.prm.ptab is None else  str(myBeam.prm.ptab)
                rsep = str(myBeam.prm.rsep)
                rxrise = str(myBeam.prm.rxrise)
                scan = str(myBeam.prm.scan)
                smsep = str(myBeam.prm.smsep)
                tfreq = str(myBeam.prm.tfreq)
                txpl = str(myBeam.prm.txpl)
                xcf = str(myBeam.prm.xcf)


                # Params in myBeam.rawacf
                #NOTE: add if needed

                # Params in myBeam.iqdat
                #NOTE: add if needed

                # Params in myBeam.fPtr
                #NOTE: add if needed


                # Write the current lbeam record to fname_csv
                line = sep.join([time, bmnum, channel, stid, cp, lmfit , fitex,
                                 exflg, iqflg, offset, lmflg, rawflg, fType,
                                 acflg, fitacf,                 # upto here are params in myBeam
                                 elv, gflg, nlag, npnts, p_l, p_l_e, p_s,
                                 p_s_e, phi0, phi0_e, pwr0, qflg, slist, v,
                                 v_e, w_l, w_l_e, w_s, w_s_e,   # upto here are params in myBeam.fit
                                 bmazm, frang, ifmode, inttsc, inttus, lagfr,
                                 ltab, mpinc, mplgexs, mplgs, mppul, nave, noisemean,
                                 noisesearch, noisesky, nrang, ptab, rsep, rxrise,
                                 scan, smsep, tfreq, txpl, xcf]) # upto here are params in myBeam.prm
                f.write(line +"\n")

            # Read the next beam record
            myBeam = myPtr.readRec()

    return fname_csv

# run the code
def main():

    # Set the logging level
    logging.getLogger().setLevel(logging.WARNING)

    # input parameters
    #ctr_date = dt.datetime(2012,12,31)
    #ctr_date = dt.datetime(2014,11,02)
    ctr_date = dt.datetime(2012,12,05)
    stime = ctr_date
    etime = ctr_date + dt.timedelta(days=1)
    #etime = None


    #rad = "fhe"
    rad = "bks"
    #rad = "ade"
    channel = "."
    ftype = "fitacf"

    csv_sep = "|"    # used to seperate variables in a csv file

    remove_extra_file = True 
    median_filter=False
    path_to_filter = './fitexfilter'

    #localdirfmt = "/sd-data/{year}/{ftype}/{radar}/"
    localdirfmt = "./sd-data/{year}/{ftype}/{radar}/"
    #localdirfmt = ".data/tmp/"
    localdict = {"ftype" : ftype, "radar" : rad, "channel" : channel}
    tmpdir = "./data/tmp/"
    fnamefmt = ['{date}.{hour}......{radar}.{channel}.{ftype}',\
                '{date}.{hour}......{radar}.{ftype}']

    # Fetch and concatenate files
    fname = fetch_concat(ctr_date, localdirfmt, localdict, tmpdir, fnamefmt,
                         remove_extra_file=remove_extra_file,
			 median_filter=median_filter,
			 path_to_filter=path_to_filter)


    # Convert dmap format to csv
    #fname = "./data/tmp/20121231.000000.20130101.000000.fhe.fitacf"
    #fname = "./data/tmp/20141101.000000.20141102.000000.bks.fitacf"
    #fname = "./data/tmp/20121204.000000.20121205.000000.bks.fitacf"
    #fname = "./data/tmp/20121205.000000.20121206.000000.bks.fitacf"
    print("Converting from dmap format to csv")
    fname_csv = dmap_to_csv(fname, stime, etime=etime, sep=csv_sep,
                            fileType=ftype)

    return fname_csv

if __name__ == "__main__":
    fname = main()

