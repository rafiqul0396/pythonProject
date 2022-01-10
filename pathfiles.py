import glob

from pyspark.shell import spark

filepath=r'/home/impressico/Desktop/Python/pysparT/Datasets/'
#print(filepath)
path=list(glob.glob('{0}/**/*'.format(filepath),recursive=True))
#for i in range(0,len(path)):
#    path

# filepath=list(glob.glob(filpath+"*.csv"))
#for i in path:
 #   EmpData = spark.read.csv(path=i,
   #                          sep=",",
   #                          header=True,
   #                          inferSchema=True)
    #EmpData.printSchema()

