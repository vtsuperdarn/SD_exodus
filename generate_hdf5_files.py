"""
Written by Xueling on 09/06/2018
"""

from davitpy import pydarn
import datetime as dt
import numpy as np
import pandas 
import h5py
import os
import ipdb
import time

class HDF5Converter(object):
    """
    Class to generate hdf5 files from fitacf
    for a given datetime range.
    """

    def __init__(self, startTime, endTime, inpRad, ftype="fitacf"):
        """
        Initialize parameters
        """
        self.startTime = startTime
        self.endTime = endTime
        self.inpRad = inpRad
        self.ftype = ftype
        self._paramDict_ = self.get_param_dict()
        self._scalar_colNames_ = self.get_scalar_colNames()
        self._array_colNames_ = self.get_array_colNames()
        self._void_colNames_ = self.get_void_colNames()


    def get_param_dict(self):
        """
        Parameter names and datatypes in a fitacf file
        """
        colNamesDict = {}

        # Downcasting some vars
        colNamesDict["atten"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Attenuation level.",\
									"Unit": " "}
        colNamesDict["bmazm"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Beam azimuth.",\
									"Unit": "degree"}
        colNamesDict["bmnum"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Beam number.",\
									"Unit": " "}
        colNamesDict["channel"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Channel number for a stereo radar (zero for all others).",\
									"Unit": " "}
        colNamesDict["combf"] = {"dt": h5py.special_dtype(vlen=str),\
									"Information": "Comment buffer.",\
									"Unit": " "}
        colNamesDict["cp"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Control program identifier.",\
									"Unit": " "}
        colNamesDict["ercod"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Error code.",\
									"Unit": " "}
        colNamesDict["fitacf.revision.major"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Major version number of the FitACF algorithm.",\
									"Unit": " "}
        colNamesDict["fitacf.revision.minor"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Minor version number of the FitACF algorithm.",\
									"Unit": " "}
        colNamesDict["frang"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Distance to first range.",\
									"Unit": "kilometers"}
        colNamesDict["gflg"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Ground scatter flag for ACF.",\
									"Unit": " "}
        colNamesDict["ifmode"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "if mode",\
									"Unit": " "}
        colNamesDict["intt.sc"] ={"dt":  h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Whole number of seconds of integration time.",\
									"Unit": " "}
        colNamesDict["intt.us"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Fractional number of microseconds of integration time.",\
									"Unit": " "}
        colNamesDict["lagfr"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Lag to first range.",\
									"Unit": "microseconds"}
        colNamesDict["ltab"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Lag table.",\
									"Unit": " "}
        colNamesDict["lvmax"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Maximum noise level allowed.",\
									"Unit": " "}
        colNamesDict["mpinc"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Multi-pulse increment.",\
									"Unit": "microseconds"}
        colNamesDict["mplgexs"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": " ",\
									"Unit": " "}
        colNamesDict["mplgs"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Number of lags in sequence.",\
									"Unit": " "}
        colNamesDict["mppul"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Number of pulses in sequence.",\
									"Unit": " "}
        colNamesDict["mxpwr"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Maximum power.",\
									"Unit": "kHz"}
        colNamesDict["nave"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Number of pulse sequences transmitted.",\
									"Unit": " "}
        colNamesDict["nlag"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Number of points in the fit.",\
									"Unit": " "}
        colNamesDict["noise.lag0"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Lag zero power of noise ACF.",\
									"Unit": " "}
        colNamesDict["noise.mean"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Average noise across frequency band.",\
									"Unit": " "}
        colNamesDict["noise.search"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Calculated noise from clear frequency search.",\
									"Unit": " "}
        colNamesDict["noise.sky"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Sky noise.",\
									"Unit": " "}
        colNamesDict["noise.vel"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Velocity from fitting the noise noise ACF.",\
									"Unit": " "}
        colNamesDict["nrang"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Number of ranges.",\
									"Unit": " "}
        colNamesDict["offset"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Offset between channels for a stereo radar (zero for all others).",\
									"Unit": " "}
        #colNamesDict["origin.code"] = h5py.special_dtype(vlen=str) #ValueError: VLEN strings do not support embedded NULLs
        colNamesDict["origin.code"] = {"dt": np.dtype('V1'),\
									"Information": "Code indicating origin of the data.",\
									"Unit": " "}
        colNamesDict["origin.command"] = {"dt": h5py.special_dtype(vlen=str),\
									"Information": "The command line or control program used to generate the data.",\
									"Unit": " "}
        colNamesDict["origin.time"] = {"dt": h5py.special_dtype(vlen=str),\
									"Information": "ASCII representation of when the data was generated.",\
									"Unit": " "}
        colNamesDict["p_l"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power from lambda fit of ACF.",\
									"Unit": " "}
        colNamesDict["p_l_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power error from lambda fit of ACF.",\
									"Unit": " "}
        colNamesDict["p_s"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power from sigma fit of ACF.",\
									"Unit": " "}
        colNamesDict["p_s_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Powr error from sigma fit of ACF.",\
									"Unit": " "}
        colNamesDict["ptab"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Pulse table.",\
									"Unit": " "}
        colNamesDict["pwr0"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Lag zero power.",\
									"Unit": " "}
        colNamesDict["qflg"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Quality of fit flag for ACF.",\
									"Unit": " "}
        #colNamesDict["radar.revision.major"] = h5py.special_dtype(vlen=str)  #ValueError: VLEN strings do not support embedded NULLs
        #colNamesDict["radar.revision.minor"] = h5py.special_dtype(vlen=str)  #ValueError: VLEN strings do not support embedded NULLs
        colNamesDict["radar.revision.major"] =  {"dt": np.dtype('V1'),\
									"Information": "Major version number of the radar operating system.",\
									"Unit": " "}
        colNamesDict["radar.revision.minor"] = {"dt":  np.dtype('V1'),\
									"Information": "Minor version number of the radar operating system.",\
									"Unit": " "}
        colNamesDict["rsep"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Range separation.",\
									"Unit": "kilometers"}
        colNamesDict["rxrise"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Receiver rise time.",\
									"Unit": "microseconds"}
        colNamesDict["scan"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Scan flag.",\
									"Unit": " "}
        colNamesDict["sd_l"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Standard deviation of lambda fit.",\
									"Unit": " "}
        colNamesDict["sd_phi"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Standard deviation of phase fit of ACF.",\
									"Unit": " "}
        colNamesDict["sd_s"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Standard deviation of sigma fit.",\
									"Unit": " "}
        colNamesDict["slist"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "List of stored ranges.",\
									"Unit": " "}
        colNamesDict["smsep"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Sample separation",\
									"Unit": "microseconds"}
        colNamesDict["stat.agc"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "AGC status word.",\
									"Unit": " "}
        colNamesDict["stat.lopwr"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "LOPWR status word.",\
									"Unit": " "}
        colNamesDict["stid"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Station identifier.",\
									"Unit": " "}
        colNamesDict["tfreq"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Transmitted frequency.",\
									"Unit": " "}
        colNamesDict["time"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "time",\
									"Unit": " "}
        colNamesDict["txpl"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Transmit pulse length.",\
									"Unit": "microseconds"}
        colNamesDict["txpow"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "Transmitted power.",\
									"Unit": "kW"}
        colNamesDict["v"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Velocity from ACF.",\
									"Unit": "m/s"}
        colNamesDict["v_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Velocity error from ACF.",\
									"Unit": "m/s"}
        colNamesDict["w_l"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width from lambda fit of ACF.",\
									"Unit": "m/s"}
        colNamesDict["w_l_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width error from lambda fit of ACF.",\
									"Unit": "m/s"}
        colNamesDict["w_s"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width from sigma fit of ACF.",\
									"Unit": "m/s"}
        colNamesDict["w_s_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width error from sigma fit of ACF.",\
									"Unit": " "}
        colNamesDict["xcf"] = {"dt": h5py.special_dtype(vlen=np.dtype('int32')),\
									"Information": "XCF flag.",\
									"Unit": " "}

        colNamesDict["x_qflg"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Quality of fit flag for XCF.",\
									"Unit": " "}
        colNamesDict["x_gflg"] = {"dt": h5py.special_dtype(vlen=np.dtype('int16')),\
									"Information": "Ground scatter flag for XCF.",\
									"Unit": " "}
        colNamesDict["x_p_l"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power from lambda fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_p_l_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power error from lambda fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_p_s"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power from sigma fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_p_s_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Power error from sigma fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_v"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Velocity from XCF.",\
									"Unit": " "}
        colNamesDict["x_v_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Velocity error from XCF.",\
									"Unit": " "}
        colNamesDict["x_w_l"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width from lambda fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_w_l_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width error from lambda fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_w_s"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width from sigma fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_w_s_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Spectral width error from sigma fit of XCF.",\
									"Unit": " "}
        colNamesDict["phi0"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Phase determination at lag zero of the ACF.",\
									"Unit": " "}
        colNamesDict["phi0_e"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Phase determination error at lag zero of the ACF.",\
									"Unit": " "}
        colNamesDict["elv"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Angle of arrival estimate.",\
									"Unit": " "}
        colNamesDict["elv_low"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Lowest estimate of angle of arrival.",\
									"Unit": " "}
        colNamesDict["elv_high"] ={"dt":  h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Highest estimat of angle of arrival.",\
									"Unit": " "}
        colNamesDict["x_sd_l"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Standard deviation of lambda fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_sd_s"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Standard deviation of sigma fit of XCF.",\
									"Unit": " "}
        colNamesDict["x_sd_phi"] = {"dt": h5py.special_dtype(vlen=np.dtype('f')),\
									"Information": "Standard deviation of phase fit of XCF.",\
									"Unit": " "}

        return colNamesDict


    def get_void_colNames(self):
        """
        3 numpy void colNames in a fitacf file
        """

        void_colNames = ["origin.code","radar.revision.major","radar.revision.minor"]

        return void_colNames


    def get_scalar_colNames(self):
        """
        42 Scalar_colNames in a fitacf file
        """

        scalar_colNames = ["atten","bmazm","bmnum","channel","combf","cp","ercod","fitacf.revision.major",\
							"fitacf.revision.minor","frang","ifmode","intt.sc","intt.us","lagfr","lvmax",\
							"mpinc","mplgexs","mplgs","mppul","mxpwr","nave","noise.lag0","noise.mean",\
							"noise.search","noise.sky","noise.vel","nrang","offset",\
							"origin.command","origin.time",\
							"rsep","rxrise","scan","smsep","stat.agc","stat.lopwr","stid","tfreq","time",\
							"txpl","txpow","xcf"]

        return scalar_colNames


    def get_array_colNames(self):
        """
        40 Array_colNames in a fitacf file
        """

        array_colNames = ["gflg","ltab","nlag","p_l","p_l_e","p_s","p_s_e","ptab","pwr0","qflg",\
							"sd_l","sd_phi","sd_s","slist","v","v_e","w_l","w_l_e","w_s","w_s_e",\
							"x_qflg","x_gflg","x_p_l","x_p_l_e","x_p_s","x_p_s_e","x_v","x_v_e",\
							"x_w_l","x_w_l_e","x_w_s","x_w_s_e","phi0","phi0_e","elv","elv_low",\
							"elv_high","x_sd_l","x_sd_s","x_sd_phi"]

        return array_colNames


    def get_dmap_dicts(self, filtered=False):
        """
        Get a list of dictionaries from dmap files
        """
        f = pydarn.sdio.radDataOpen(self.startTime, self.inpRad,
                    self.endTime, filtered=filtered,
                    fileType=self.ftype)
        if f is not None:
            recAll = pydarn.sdio.radDataReadAll(f)
            if recAll is not None:
                return [ rec.recordDict for rec in recAll]
        return None


    def create_hdf5_file(self, fitData, FileName, **kwargs):
        """
        Create a hdf5 file from the list of dicts (fitdata).
        """
        if fitData is not None:
            # get parameter names from the datadict
            colNames = set()
            for row in fitData:
            	names = set(row.keys())
            	colNames = colNames.union(names)
            colNames = sorted(list(colNames))

            #create datasets
            num_rec = len(fitData)
            #print num_rec
            f = h5py.File(FileName, "w")

            for column in colNames:
            	#print column
            	tmp_dset = f.create_dataset(column, (num_rec,), dtype=self._paramDict_[column]["dt"], **kwargs)
            	tmp_dset.attrs['Information'] = self._paramDict_[column]["Information"]
            	tmp_dset.attrs['Unit'] = self._paramDict_[column]["Unit"]

            #Read data into datasets
            for nr,row in enumerate(fitData):
            	#print nr
            	#ipdb.set_trace()
            	for column in colNames:
					if column in self._scalar_colNames_:
						f[column][nr] = [row.get(column)] #[] if row.get(column) is None else [row.get(column)]

					if column in self._array_colNames_:
						f[column][nr] = [] if row.get(column) is None else np.array(row.get(column)).ravel()

					if column in self._void_colNames_:
						f[column][nr] =  np.void(row.get(column))

            f.close()


if __name__ == "__main__":

	#time the code
    t0 = time.time()
    sTime = dt.datetime(2012,1,1)
    eTime = sTime + dt.timedelta(days=1)
    rad = "fhe"
    Obj = HDF5Converter(sTime, eTime, rad)
    fData = Obj.get_dmap_dicts()

    outFile = "/home/xueling/data/SD_exodus/"+sTime.strftime("%Y") +\
               "/"+ rad + "/hdf5/"+sTime.strftime("%Y%m%d")+"."+rad+".hdf5"
    Obj.create_hdf5_file(fData, outFile)
    t1 = time.time()

    print t1-t0
    print 'Done on '+sTime.strftime("%Y%m%d")+" for the "+rad+" radar!"
