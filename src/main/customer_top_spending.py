from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def calculate_top_customers(input_path, output_path,test_run =False):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Top Customers by Spending") \
        .getOrCreate()

    # Read transactions data
    transactions_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Calculate total spending per customer
    customer_spending_df = transactions_df.groupBy("customer_id") \
        .agg(_sum("amount").alias("total_spending"))

    # Get the top 5 customers by spending
    top_customers_df = customer_spending_df.orderBy(col("total_spending").desc()).limit(5)

    # Write the result to the output path
    top_customers_df.write.mode("overwrite").csv(output_path, header=True)

    # Stop SparkSession
    if not test_run:
        spark.stop()