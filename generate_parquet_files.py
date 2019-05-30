import pyarrow as pa
import pandas 
import numpy
from davitpy import pydarn
import datetime
import os

class ParquetConverter(object):
    """
    Class to generate parquet files from fitacf
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
        self._schemaDict_ = self.get_schema_dict()

    def get_schema_dict(self):
        """
        Get colNames for A FEW fields
        NOTE : Some fields are given default
        colNames, i.e., whatever is assigned by pyarrow
        """
        colNamesDict = {}
        # Downcasting some vars
        colNamesDict["atten"] = pa.int16()
        colNamesDict["bmnum"] = pa.int16()
        colNamesDict["bmnum"] = pa.int16()
        colNamesDict["cp"] = pa.int16()
        colNamesDict["ercod"] = pa.int16()
        colNamesDict["fitacf.revision.major"] = pa.int32()
        colNamesDict["fitacf.revision.minor"] = pa.int32()
        colNamesDict["frang"] = pa.int16()
        colNamesDict["gflg"] = pa.list_(pa.int16())
        colNamesDict["ifmode"] = pa.int16()
        colNamesDict["intt.sc"] = pa.int16()
        colNamesDict["intt.us"] = pa.int32()
        colNamesDict["lagfr"] = pa.int32()
        colNamesDict["lagfr"] = pa.int32()
        colNamesDict["ltab"] = pa.list_(pa.list_(pa.int16()))
        colNamesDict["lvmax"] = pa.int32()
        colNamesDict["mpinc"] = pa.int32()
        colNamesDict["mplgexs"] = pa.int32()
        colNamesDict["mplgs"] = pa.int16()
        colNamesDict["mppul"] = pa.int16()
        colNamesDict["nave"] = pa.int16()
        colNamesDict["nlag"] = pa.list_(pa.int16())
        colNamesDict["nrang"] = pa.int16()
        colNamesDict["offset"] = pa.int16()
        colNamesDict["ptab"] = pa.list_(pa.int16())
        colNamesDict["qflg"] = pa.list_(pa.int16())
        colNamesDict["rsep"] = pa.int16()
        colNamesDict["rxrise"] = pa.int16()
        colNamesDict["scan"] = pa.int16()
        colNamesDict["slist"] = pa.list_(pa.int16())
        colNamesDict["smsep"] = pa.int16()
        colNamesDict["stat.agc"] = pa.int16()
        colNamesDict["stat.lopwr"] = pa.int16()
        colNamesDict["stid"] = pa.int16()
        colNamesDict["tfreq"] = pa.int32()
        colNamesDict["txpl"] = pa.int16()
        colNamesDict["txpow"] = pa.int32()
        colNamesDict["xcf"] = pa.int32()
        return colNamesDict

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

    def get_dicts_csv(self):
        import dmap_to_csv
        import csv_to_dict
        """
        Generate csv files from fitacf data
        using Muhammad's code.
        """
        fOut = dmap_to_csv.main(self.startTime, self.inpRad)
        # generate the dict from csv file!
        return csv_to_dict.main(fOut, orient="records")


    def json_to_pyarrow_table(self, fitData, useSchemaDict=True):
        """
        Convert the list of dicts (fitdata)
        into pyarrow table!
        """
        # get pyarrow record batches from the datadict
        colNames = set()
        for row in fitData:
            names = set(row.keys())
            colNames = colNames.union(names)
        colNames = sorted(list(colNames))
        colFitData = {}
        arrFitData = []
        for nr,row in enumerate(fitData):
            for column in colNames:
                _col = colFitData.get(column, [])
                _col.append(row.get(column))
                colFitData[column] = _col

        if useSchemaDict:
            for column in colNames:
                _col = colFitData.get(column)
                if column in self._schemaDict_.keys():
                    arrFitData.append( pa.array(_col, type= self._schemaDict_[column] ) )
                else:
                    arrFitData.append( pa.array(_col) )
        else:
            for column in colNames:
                arrFitData.append( pa.array(colFitData.get(column)) )
        outRecBatch = pa.RecordBatch.from_arrays(arrFitData, colNames)
        # write to a pyarrow table
        try:
            table = pa.Table.from_batches(outRecBatch)
        except TypeError:
            table = pa.Table.from_batches([outRecBatch])
        return table

    def create_parquet_file(self, inpTable, parquetFileName, **kwargs):
        """
        Create a parquet file from a pyarrow table
        **kwargs correspond to pyarrow.parquet.write_table() function
        """
        import pyarrow.parquet as pq
        pq.write_table(inpTable, parquetFileName, **kwargs)


if __name__ == "__main__":
    sTime = datetime.datetime(2017,12,2)
    eTime = sTime + datetime.timedelta(days=1)
    radSel = "bks"
    pqObj = ParquetConverter(sTime, eTime, radSel)
    fData = pqObj.get_dmap_dicts()
    # fData = pqObj.get_dicts_csv()
    paTab = pqObj.json_to_pyarrow_table(fData)
    outParquetFile = "/home/bharat/Documents/data/fit_cmpr_formats/pq/" +\
                    sTime.strftime("%Y%m%d") + radSel + ".parquet"
    pqObj.create_parquet_file(paTab, outParquetFile,\
             compression='brotli',version="2.0")
    print paTab