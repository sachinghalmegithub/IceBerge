
##spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.iceberg:iceberg-aws-bundle:1.4.0 \



from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


# Create a SparkConf object
conf = SparkConf().setAppName("MyApp").setMaster("local[2]")
#conf.set("spark.hadoop.fs.s3a.access.key", "AKIAUS5FBZKAW67QZPEA")
#conf.set("spark.hadoop.fs.s3a.secret.key", "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")

# Working Packages
#conf.set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')

conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
#conf.set('spark.jars.packages', 'org.projectnessie:nessie-spark-3.2-extensions:0.43.0,org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
#--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.iceberg:iceberg-aws-bundle:1.4.0
conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set("spark.sql.catalog.glue","org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue.warehouse","s3a://ap-south-1-snowflake-bucket-aws-s3/spark-iceberg/")
conf.set("spark.sql.catalog.glue.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("park.sql.catalog.glue.io-impl","org.apache.iceberg.aws.s3.S3FileIO")


spark = SparkSession.builder.config(conf=conf).getOrCreate()
#df = spark.read.format("parquet").load("s3a://ap-south-1-snowflake-bucket-aws-s3/iceberg-storage/")

###### Create a table and insert data into the table ###

#spark.sql("CREATE TABLE glue.default.tmp_table_1 (id bigint, data string) USING iceberg").show()
#df = spark.sql("Insert into glue.default.tmp_table_1 values (1,'sachin'); ")
#df.show()
#spark.sql("select * from glue.default.tmp_table;").show()



#spark = SparkSession.builder.config(conf=conf).getOrCreate()
#df = spark.read.format("parquet").load("s3a://ap-south-1-snowflake-bucket-aws-s3/iceberg-storage/")

# Create a table and load S3 CSV file data into the iceberg table ###
#df=spark.sql("CREATE OR REPLACE TABLE glue.default.orders (Region String, Country String, Item_Type String, Sales_Channel String, Order_Priority String, Order_Date String, Order_ID int, Ship_Date String, Units_Sold int, Unit_Price Float, Unit_Cost Float, Total_Revenue Float, Total_Cost Float, Total_Profit Float) USING iceberg").show()



#s3://snowflake-bucket-aws-s3/data/Orders.csv
#orderdf = spark.read.csv("s3a://snowflake-bucket-aws-s3/data/Orders.csv",header='true',inferSchema='true')
#orderdf.show(10)
#orderdf.printSchema()
#orderdf.createOrReplaceTempView("sampleView")
#df=spark.sql("INSERT INTO TABLE glue.default.orders  SELECT * FROM sampleView")

#spark.sql("select * from glue.default.orders;").show(10)


# Time Travel

spark.sql("select * from glue.default.orders FOR TIMESTAMP AS OF TIMESTAMP '2023-10-14 07:40:00 UTC' ;").show(10)
#spark.sql("describe table glue.default.orders FOR TIMESTAMP AS OF TIMESTAMP '2023-10-14 12:50:00 UTC' ;").show()




