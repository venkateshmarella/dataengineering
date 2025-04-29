from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def calculate_avg_sales(input_path, output_path):
    # Create a SparkSession
    spark = SparkSession.builder \
                .appName("Sales Average") \
                .getOrCreate()
    print(f'from avg sales {spark.sparkContext.version}')
    
    # Read the sales data
    sales_df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Calculate average sales per product
    avg_sales_df = sales_df.groupBy("product").agg(avg(col("sales")).alias("average_sales"))

    # Write the output to the specified path
    avg_sales_df.write.mode("overwrite").csv(output_path, header=True)