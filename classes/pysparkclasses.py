from pyspark.pandas.spark.functions import lit
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import pandas as pd
import pyarrow

from pathfiles import path, filepath


class EmpData:
    pathfiles = "/home/impressico/Desktop/Python/pysparT/Datasets/empdata.csv"


class Pypark:

    def __init__(self, pathfile, spar):
        self.pathfile = pathfile
        self.spark = spar

    def readData(self: list, pathfile):
        # input()# pls inter file path

        for i in range(0, len(self)):
            data = ''
            if (filepath[i] == pathfile):
                data = filepath[i]
                # print("i found the files")
                break
        return (data)

    def readCSV(self, pathfile: str):
        EmpData = spark.read.csv(path=pathfile,
                                 sep=',',
                                 header=True,
                                 inferSchema=True)
        EmpData.printSchema()
        EmpData.show()
        return EmpData

    ## sort our data according to the column or columns
    # lst = list(input().split())

    def SortedOurdata(self, empdata, lst: list):
        empdata.sort(lst, ascending=False).show(20)

    def ChangeDateFormats(self, empdata, col_name: str):
        newdata = empdata.withColumn(col_name, to_date(empdata[col_name], 'yyyy-MM-dd'))
        # EmpData = EmpData.withColumn("DateofHire", to_date(EmpData["DateofHire"], 'yyyy-MM-dd'))
        newdata.printSchema()

    def ChangeFormatOfColumn(self,empdata, col_name: str, DataType: str):
        newdata = empdata.withColumn(col_name, col(col_name).cast(DataType))
        newdata.printSchema()

    def UpadteValuesOfCoumns(self, empdata, col_name: str):
        empdata.withColumn(col_name, col(col_name) * 2).show()

    def CreateColumnFromExit(self, empdta, new_col_name: str, exit_col_name):
        empdta.withColumn(new_col_name, col(exit_col_name) * 10).show()

    def AddNewColum(self, empdata, col_name: str, defult_vale: str):
        newdata = empdata.withColumn(col_name, lit(defult_vale))
        newdata.printSchema()
        newdata.show()

    def renameColumn(self, empdata, exit_column_name: str, new_col_name: str):
        newdata = empdata.withColumnRenamed(exit_column_name, new_col_name)
        newdata.printSchema()

    def DroptheColumn(self, empdata, col_name: str):
        newdata = empdata.drop(col_name)
        newdata.printSchema()


pathfiles = "/home/impressico/Desktop/Python/pysparT/Datasets/empdata.csv"
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

park = Pypark(pathfiles, spark)
empddata = park.readCSV(pathfiles)
park.AddNewColum(empddata, col_name="Country", defult_vale="US")
park.ChangeFormatOfColumn(empddata, col_name='DateofHire', DataType="date")
