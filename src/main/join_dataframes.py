from pyspark.sql import SparkSession

def join_datasets(customer_path, orders_path, output_path):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Join Datasets") \
        .getOrCreate()

    # Read customer and orders data
    customers_df = spark.read.csv(customer_path, header=True, inferSchema=True)
    orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)

    # Perform an inner join on customer_id
    joined_df = customers_df.join(orders_df, on="customer_id", how="inner")

    # Write the result to the output path
    joined_df.write.csv(output_path, header=True)

    # Stop SparkSession
    spark.stop()