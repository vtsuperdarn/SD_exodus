import pyarrow as pa
import pyarrow.parquet as pq
import pandas 
import numpy
from davitpy import pydarn
import datetime
# import the csv creation module
import os
import sys
module_path = os.path.abspath(os.path.join('../'))
if module_path not in sys.path:
    sys.path.append(module_path)
import dmap_to_csv
import generate_parquet_files


class CreateFiles(object):
    """
    In this class we'll generate different
    file formats such as Parquet, HDF5 and csv
    to from fitacf data.
    """
    def __init__(self, inpTime, inpRad):
        """
        Initialize date, rad and other variables used.
        NOTE : we'll create 1 file/day
        """
        self.startTime = inpTime
        self.inpRad = inpRad
        self.endTime = self.startTime + datetime.timedelta(days=1)

    def create_csv_fitacf_files(self, csvOutDir, fitOutDir):
        """
        Generate csv files from fitacf data
        using Muhammad's code.
        """
        fOut = dmap_to_csv.main(sTime, inpRad)
        # we'll need to compare the sizes 
        # of different file formats; have them
        # in one location.
        # move the csv file
        csvName = fOut.split("/")[-1]
        os.rename(fOut, csvOutDir + csvName)
        # move the actual fitacf file
        fitFileName = ".".join( csvName.split(".")[:-1] )
        fitFilePath = "/".join( csvOutDir.split("/")[:-1] )
        os.rename(fitFilePath + "/" + fitFileName, fitOutDir + fitFileName)

    def create_parquet_files(self, pqOutDir, \
            compression='brotli',version="2.0"):
        """
        Generate Parquet files from fitacf data
        """
        pqObj = generate_parquet_files.ParquetConverter(self.startTime,\
                         self.endTime, self.inpRad)
        fData = pqObj.get_dmap_dicts()
        paTab = pqObj.json_to_pyarrow_table(fData)
        outParquetFile = pqOutDir + self.startTime.strftime("%Y%m%d") +\
                         self.inpRad + ".parquet"
        pqObj.create_parquet_file(paTab, outParquetFile,\
                 compression=compression,version=version)


if __name__ == "__main__":
    sDate = datetime.datetime(2012,6,1)
    eDate = datetime.datetime(2012,7,1)
    selRadList = ["fhe"]
    pqOutDir = "/home/bharat/Documents/data/fit_cmpr_formats/pq/"
    while sDate <= eDate:
        for sr in selRadList:
            print "curr date--->", sDate
            print sDate, sr
            cfo = CreateFiles(sDate, sr)
            cfo.create_parquet_files(pqOutDir)
            sDate += datetime.timedelta(days=1)
            print " ******* Created Parquet File *******"
    # cfo = CreateFiles(currDate, selRad)