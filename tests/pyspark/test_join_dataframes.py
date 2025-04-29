import os
import shutil
from pyspark.sql import SparkSession
from src.main.pyspark.join_dataframes import join_datasets

def test_join_datasets():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Join Datasets Test") \
        .master("local[1]") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.local.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.hadoop.fs.local.checksum", "false") \
        .getOrCreate()

    # Define input and output paths
    customer_path = "resources/test_customers.csv"
    orders_path = "resources/test_orders.csv"
    output_path = "tests/output/test_join_output"

    # Create sample customer data
    with open(customer_path, "w") as f:
        f.write("customer_id,name\n")
        f.write("1,Alice\n")
        f.write("2,Bob\n")
        f.write("3,Charlie\n")

    # Create sample orders data
    with open(orders_path, "w") as f:
        f.write("order_id,customer_id,amount\n")
        f.write("101,1,50.0\n")
        f.write("102,2,75.0\n")
        f.write("103,4,100.0\n")

    # Run the join
    join_datasets(customer_path, orders_path, output_path)

    # Read the output
    output_df = spark.read.csv(output_path, header=True, inferSchema=True)

    # Assert the results
    assert output_df.count() == 2  # Only 2 matching rows should remain
    assert set(row["customer_id"] for row in output_df.collect()) == {1, 2}

    # Cleanup
    os.remove(customer_path)
    os.remove(orders_path)
    shutil.rmtree(output_path)

    # Stop SparkSession
    spark.stop()

# Run the test
if __name__ == "__main__":
    test_join_datasets()