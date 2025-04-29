import os
import shutil
from pyspark.sql import SparkSession
from main.pyspark.remove_duplicates import deduplicate_data

def test_deduplicate_data():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Deduplicate Data Test") \
        .master("local[1]") \
        .getOrCreate()

    # Define input and output paths
    input_path = "resources/test_dedup_input.csv"
    output_path = "tests/output/test_dedup_output"

    # Create a sample input dataset
    with open(input_path, "w") as f:
        f.write("id,name,age\n")
        f.write("1,Alice,30\n")
        f.write("2,Bob,25\n")
        f.write("1,Alice,30\n")
        f.write("3,Charlie,35\n")

    # Run deduplication
    deduplicate_data(input_path, output_path)

    # Read the output
    output_df = spark.read.csv(output_path, header=True, inferSchema=True)

    # Assert the results
    assert output_df.count() == 3  # Only 3 unique records should remain

    # Cleanup
    os.remove(input_path)
    shutil.rmtree(output_path)

    # Stop SparkSession
    spark.stop()

# Run the test
if __name__ == "__main__":
    test_deduplicate_data()