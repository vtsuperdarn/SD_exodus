"""
Written by Muhammad on 09/01/2018
"""

import datetime as dt
from davitpy.pydarn.sdio.fetchUtils import fetch_local_files
from davitpy.pydarn.sdio import radDataPtr, radDataReadRec
from davitpy.pydarn.sdio import radDataPtr
from davitpy import pydarn
import logging
import os
import string
from time import clock
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
                w_l_e = "[]" if myBeam.fit.w_l_e is None else str(myBeam.fit.w_l_e)
                w_l_e = w_l_e.replace("inf", "999999")
                w_s = "[]" if myBeam.fit.w_s is None else str(myBeam.fit.w_s)
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
def main(inpStartTime, inpRad, inpFtype="fitacf",inpEndTime=None):

    # Set the logging level
    logging.getLogger().setLevel(logging.WARNING)

    # input parameters
    ctr_date = inpStartTime#dt.datetime(2012,12,31)
    stime = inpStartTime#ctr_date
    etime = inpEndTime#None
    #stime = dt.datetime(2012,12,31)
    #etime = dt.datetime(2012,12,31, 1, 0)


    rad = inpRad#"fhe"
    #rad = "ade"
    channel = "."
    ftype = inpFtype#"fitacf"

    csv_sep = "|"    # used to seperate variables in a csv file

    remove_extra_file = True 
    median_filter=False
    path_to_filter = './fitexfilter'

    localdirfmt = "/sd-data/{year}/{ftype}/{radar}/"
    localdict = {"ftype" : ftype, "radar" : rad, "channel" : channel}
    tmpdir = "/tmp/"#"./data/tmp/"
    fnamefmt = ['{date}.{hour}......{radar}.{channel}.{ftype}',\
                '{date}.{hour}......{radar}.{ftype}']

    # Fetch and concatenate files
    fname = fetch_concat(ctr_date, localdirfmt, localdict, tmpdir, fnamefmt,
                         remove_extra_file=remove_extra_file,
			 median_filter=median_filter,
			 path_to_filter=path_to_filter)


    # Convert dmap format to csv
    #fname = "./data/tmp/20121231.000000.20130101.000000.fhe.fitacf"
    print("Converting from dmap format to csv")
    fname_csv = dmap_to_csv(fname, stime, etime=etime, sep=csv_sep,
                            fileType=ftype)

    return fname_csv

if __name__ == "__main__":
    fname = main()

