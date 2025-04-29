from pyspark.sql import SparkSession
spark=SparkSession.builder \
    .appName("Init Spark Session") \
    .master("local[1]") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.local.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .config("spark.hadoop.fs.local.checksum", "false") \
    .getOrCreate()
print(spark.sparkContext.version)