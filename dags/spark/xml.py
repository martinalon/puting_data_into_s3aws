from pyspark.sql import DataFrame, SparkSession 
from pyspark.sql.functions import *
#import pyspark.sql.functions as F
#from pyspark.ml.feature import Tokenizer,  StopWordsRemover
#from pyspark import SparkContext
#from pyspark.sql import SQLContext

spark = SparkSession \
       .builder \
       .appName("Our First Spark example") \
       .getOrCreate()


data = spark.read.format("csv").option("header","true").option("inferSchema","true").load("log_reviews.csv")
data.createOrReplaceTempView("Data")
df2 = spark.sql("""
    SELECT 
        id_review,
        xpath_string(log, '/reviewlog/log/logDate') AS logDate, 
        xpath_string(log, '/reviewlog/log/device') AS device,
        xpath_string(log, '/reviewlog/log/location') AS location,
        xpath_string(log, '/reviewlog/log/os') AS os,
        xpath_string(log, '/reviewlog/log/ipAddress') AS ipAddress, 
        xpath_string(log, '/reviewlog/log/phoneNumber') AS phoneNumber   
    From Data""") 
df2.show(100)

print(type(df2))