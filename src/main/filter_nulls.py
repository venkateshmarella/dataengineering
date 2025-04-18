from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def filter_invalid_rows(input_path, output_path,test_run=False):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Filter Invalid Rows") \
        .getOrCreate()

    # Read input data
    data_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Filter out rows with null or invalid values in required fields
    valid_data_df = data_df.filter(col("id").isNotNull() & col("name").isNotNull())

    # Write the valid rows to the output path
    valid_data_df.write.mode("overwrite").csv(output_path, header=True)

    if not test_run:
        # Stop SparkSession
        spark.stop()