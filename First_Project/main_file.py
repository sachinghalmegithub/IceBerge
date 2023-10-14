

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadTextFilesFromS3").getOrCreate()
print('okay')