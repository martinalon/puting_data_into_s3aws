from email import header

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

# import pyspark.sql.functions as F
# from pyspark.ml.feature import Tokenizer,  StopWordsRemover
# from pyspark import SparkContext
# from pyspark.sql import SQLContext

s3_data_source_path = "s3://martinflores-datasets-1/log_reviews.csv"
s3_data_output_path = "s3://recive-archivos-3000/data/"

spark = SparkSession.builder.appName("Our First Spark example").getOrCreate()
data = spark.read.csv(s3_data_source_path, header=True)
print("ya se ha leido (;")
data.write.format("csv").option("header", "true").save(
    s3_data_output_path, mode="overwrite"
)

# data.createOrReplaceTempView("Data")
# df2 = spark.sql("""
#    SELECT
#        id_review,
#        xpath_string(log, '/reviewlog/log/logDate') AS logDate,
#        xpath_string(log, '/reviewlog/log/device') AS device,
#        xpath_string(log, '/reviewlog/log/location') AS location,
#        xpath_string(log, '/reviewlog/log/os') AS os,
#        xpath_string(log, '/reviewlog/log/ipAddress') AS ipAddress,
#        xpath_string(log, '/reviewlog/log/phoneNumber') AS phoneNumber
#    From Data""")
# df2.show(100)

# print(type(df2))
