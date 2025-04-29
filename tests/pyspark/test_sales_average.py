import os
import shutil
from pyspark.sql import SparkSession
from src.main.pyspark.sales_average import calculate_avg_sales

def test_calculate_avg_sales():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Sales Average Test") \
        .master("local[1]") \
        .getOrCreate()

    # Input and output paths
    input_path = "resources/test_sales_input.csv"
    output_path = "tests/output/test_sales_output"

    # Create a sample input file
    with open(input_path, "w") as f:
        f.write("product,sales\n")
        f.write("A,100\n")
        f.write("A,200\n")
        f.write("B,150\n")
        f.write("B,250\n")
        f.write("C,300\n")

    # Run the average sales calculation program
    calculate_avg_sales(input_path, output_path)

    # Read the output
    output_df = spark.read.csv(output_path, header=True, inferSchema=True)

    # Convert the output to a dictionary
    output_dict = {row["product"]: row["average_sales"] for row in output_df.collect()}

    # Assert the results
    assert output_dict == {"A": 150.0, "B": 200.0, "C": 300.0}

    # Cleanup
    os.remove(input_path)
    shutil.rmtree(output_path)

    # Stop the SparkSession
    spark.stop()

# Run the test
if __name__ == "__main__":
    test_calculate_avg_sales()