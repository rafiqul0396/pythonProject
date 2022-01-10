from pyspark.sql import SparkSession


class Employee:
    spark = SparkSession \
        .builder \
        .appName("Our first Python Spark SQL example") \
        .getOrCreate()

emp=Employee()
emp.spark
print(emp)