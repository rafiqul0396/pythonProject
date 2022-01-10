import glob
import os
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import *


def create_Session():
    '''
    this function return spark session
    so we create one session in this function
    '''
    spark = SparkSession \
        .builder \
        .appName("Our first Python Spark SQL example") \
        .getOrCreate()
    return spark


def create_dataFrame(spark, data, schema):
    '''
    spark:sessions
    data:is about our data it may be list of dict or any form of data
    or records of our dataframe
    schema: fields or attributes or column of dataframe
    '''
    dataFrame = spark.createDataFrame(data, schema)
    return dataFrame


if __name__ == "__main__":
    spark = create_Session()
    input_data = [(1, "Shivansh", "Data Scientist", "Noida"),
                  (2, "Rishabh", "Software Developer", "Banglore"),
                  (3, "Swati", "Data Analyst", "Hyderabad"),
                  (4, "Amar", "Data Analyst", "Noida"),
                  (5, "Arpit", "Android Developer", "Pune"),
                  (6, "Ranjeet", "Python Developer", "Gurugram"),
                  (7, "Priyanka", "Full Stack Developer", "Banglore")]

    schema = ["Id", "Name", "Job Profile", "City"]
    # calling function to create dataframe
    df = create_dataFrame(spark, input_data, schema)
    data_collect = df.collect()
    # that collect() is an action hence it does not return a DataFrame instead,
    # it returns data in an Array to the driver
    df.show()