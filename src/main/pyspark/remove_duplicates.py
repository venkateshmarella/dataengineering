from pyspark.sql import SparkSession

def deduplicate_data(input_path, output_path):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Deduplicate Data") \
        .getOrCreate()

    # Read input data
    data_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Remove duplicates based on all columns
    deduplicated_df = data_df.dropDuplicates()

    # Write the result to the output path
    deduplicated_df.write.csv(output_path, header=True)

    # Stop SparkSession
    spark.stop()