from pyspark.pandas.spark.functions import lit
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, col
import pandas as pd
import pyarrow

from pathfiles import path, filepath


class EmpData:
    pathfiles = "/home/impressico/Desktop/Python/pysparT/Datasets/empdata.csv"


class Pypark:

    def __init__(self, pathfile, Spark: SparkSession):
        """

        :param pathfile:we have give here file path
        :param spark:pyspark sessions we start
        """
        self.pathfile = pathfile
        self.spark = Spark

    def readData(self, lst: list, pathfile: str):
        pass
        """
         
        :param lst: if want pass list of files then is functions reads all the files and present schema of his
        :return:
        :param pathfile: path of files // or abosulte path
        :return: schema or dataframe
        """
        # input()# pls inter file path

        #for i in range(0, len(self)):
         #   data = ''
          #  if (filepath[i] == pathfile):
             #   data = filepath[i]
                # print("i found the files")
              #  break
        #return (data)

    def readCsv(self, pathfile: str) -> DataFrame:
        """
        this function  read csv files  and return dataframe.

        :rtype: DataFrame-print the schema and show the datsets
        :param pathfile:files path
        :return: return both schema and datasets
        """
        EmpData = spark.read.csv(path=pathfile,
                                 sep=',',
                                 header=True,
                                 inferSchema=True)
        EmpData.printSchema()
        EmpData.show()

        return EmpData

    ## sort our data according to the column or columns
    # lst = list(input().split())

    def sortOurData(self, empdata: DataFrame, lst: list) -> DataFrame:
        """
        this function sort our data in column basis
        :param lst: if want sort more than one columns then we pass the string list
        :return: Dataframe
        :type empdata: DataFrame-> our data  data
        :rtype: DataFrame
        """
        print(empdata.sort(lst, ascending=False).show(20))

    def changeDateFormat(self, empdata: DataFrame, col_name: str) -> DataFrame:
        """
       this function chnage the other fromate to date formate pf column
        :rtype: DataFrame
        :param empdata:datasets which we want change
        :param col_name: column which we want to change our dataType
        """
        newdata = empdata.withColumn(col_name, to_date(empdata[col_name], 'yyyy-MM-dd'))
        # EmpData = empdata.withColumn("DateofHire", to_date(empdata["DateofHire"], 'yyyy-MM-dd'))
        newdata.printSchema()

    def ChangeFormatOfColumn(self, empdata: DataFrame, col_name: str, DataType: str) -> DataFrame:
        """
 this  functions chnage data type of column of dataset
        :param empdata:dataset
        :param col_name:the column we want chnage our datatype inter here
        :param DataType:data type which u want to chnage here
        :rtype: DataFrame
        """
        newdata = empdata.withColumn(col_name, col(col_name).cast(DataType))
        newdata.printSchema()

    def UpadteValuesOfCoumns(self, empdata: DataFrame, col_name: str) -> DataFrame:
        """
      This function update values with some values
        :param empdata:dataset
        :param col_name:u want update the column  enter the column
        :rtype:dataframe
        """
        empdata.withColumn(col_name, col(col_name) * 2).show()

    def CreateColumnFromExit(self, empdta: DataFrame, new_col_name: str, exit_col_name: str) -> DataFrame:
        """
        this function create column from exits column

        :param empdta:dataset
        :param new_col_name:enter the name of new column and
        :param exit_col_name:enter the column from wheere u want ceate new column
        :rtype: DataFrame
        """
        empdta.withColumn(new_col_name, col(exit_col_name) * 10).show()

    def AddNewColum(self, empdata: DataFrame, col_name: str, defult_vale: str) -> DataFrame:
        """
        this function  add new column in our dataframe and set defalut value in that column

        :param empdata:dataset
        :param col_name:give new name for name new colun
        :param defult_vale:enter the defult value
        :rtype: DataFrame
        """
        newdata = empdata.withColumn(col_name, lit(defult_vale))
        newdata.printSchema()
        newdata.show()

    def renameColumn(self, empdata: DataFrame, exit_column_name: str, new_col_name: DataFrame) -> DataFrame:
        """
        this function rename the column name and

        :param empdata:dataset
        :param exit_column_name:name of the coulmn u wnat change
        :param new_col_name:name of change column
        :rtype: DataFrame
        """
        newdata = empdata.withColumnRenamed(exit_column_name, new_col_name)
        newdata.printSchema()

    def dropTheColumn(empData: DataFrame, col_Name: str) -> DataFrame:
        """
        this function drop the column from our dataset

        :param col_Name:if want drop the some coulmn then enter the column
      :rtype: DataFrame
       """
        newData = empData.drop(col_Name)
        newData.printSchema()


pathfiles = "/home/impressico/Desktop/Python/pysparT/Datasets/empdata.csv"
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

print(type(spark))
park = Pypark(pathfiles, spark)
empddata = park.readCsv(pathfiles)
print(type(empddata))
park.AddNewColum(empddata, col_name="Country", defult_vale="US")
park.ChangeFormatOfColumn(empddata, col_name='DateofHire', DataType="date")
