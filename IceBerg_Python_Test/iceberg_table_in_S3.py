
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import string
import random

# Create a SparkConf object
conf = SparkConf().setAppName("MyApp").setMaster("local[2]")
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue","org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue.warehouse","s3a://ap-south-1-snowflake-bucket-aws-s3/spark-iceberg/")
conf.set("spark.sql.catalog.glue.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("park.sql.catalog.glue.io-impl","org.apache.iceberg.aws.s3.S3FileIO")


spark = SparkSession.builder.config(conf=conf).getOrCreate()

###### Create a table and insert data into the table ###
N=7
list=[1,2,3,4,5,6,7,8,9]
spark.sql("CREATE TABLE IF NOT EXISTS glue.iceberg_db.tmp_table_1 (id bigint, data string) USING iceberg").show()
df = spark.sql("Insert into glue.iceberg_db.tmp_table_1 values ({},'{}');".format(random.randint(1,10000),''.join(random.choices(string.ascii_lowercase + string.digits, k=N))))
spark.sql("select * from glue.iceberg_db.tmp_table_1;").show()
