
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


# Create a SparkConf object
conf = SparkConf().setAppName("MyApp").setMaster("local[2]")\
    .set("spark.executor.memory", "2g")


conf.set("spark.hadoop.fs.s3a.access.key", "<>")
conf.set("spark.hadoop.fs.s3a.secret.key", "<>")
conf.set("spark.some.config.option","true")
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")

# Create a SparkSession object
spark = SparkSession.builder.config(conf=conf).getOrCreate()



print('#######----Print the configuration settings-----#####')
# Print the configuration settings
print("spark.app.name = ", conf.get("spark.app.name"))
print("spark.master = ", conf.get("spark.master"))
print("spark.executor.memory = ", conf.get("spark.executor.memory"))
print("spark.hadoop.fs.s3a.access.key = ", conf.get("spark.hadoop.fs.s3a.access.key"))
print("park.hadoop.fs.s3a.secret.key = ", conf.get("spark.hadoop.fs.s3a.secret.key"))

# read code from s3
#df = spark.read.format("parquet").load("s3a://ap-south-1-snowflake-bucket-aws-s3/iceberg-storage/data/iRXQpA/20231012_131512_00058_mi9j9-80d329d2-f90d-4861-ab73-c834997b88ec.parquet")
df = spark.read.format("parquet").load("s3a://ap-south-1-snowflake-bucket-aws-s3/iceberg-storage/")

#read a file from s3


df.show()


