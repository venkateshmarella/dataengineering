import os
import shutil
from pyspark.sql import SparkSession
from top_customers import calculate_top_customers

def test_calculate_top_customers():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Top Customers Test") \
        .master("local[1]") \
        .getOrCreate()

    # Define input and output paths
    input_path = "test_transactions.csv"
    output_path = "test_output"

    # Create a sample input dataset
    with open(input_path, "w") as f:
        f.write("customer_id,amount\n")
        f.write("1,50.0\n")
        f.write("2,200.0\n")
        f.write("3,150.0\n")
        f.write("1,100.0\n")
        f.write("2,50.0\n")
        f.write("4,300.0\n")
        f.write("5,400.0\n")
        f.write("6,250.0\n")

    # Run the top customers calculation
    calculate_top_customers(input_path, output_path)

    # Read the output
    output_df = spark.read.csv(output_path, header=True, inferSchema=True)

    # Convert the result to a dictionary
    output_list = output_df.collect()
    output_dict = {row["customer_id"]: row["total_spending"] for row in output_list}

    # Expected top 5 customers
    expected_output = {
        "5": 400.0,
        "4": 300.0,
        "6": 250.0,
        "2": 250.0,
        "1": 150.0
    }

    # Assert the results
    assert len(output_dict) == 5
    assert output_dict == expected_output

    # Cleanup
    os.remove(input_path)
    shutil.rmtree(output_path)

    # Stop SparkSession
    spark.stop()

# Run the test
if __name__ == "__main__":
    test_calculate_top_customers()