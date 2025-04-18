import unittest
import os,sys
import shutil
from pyspark.sql import SparkSession
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from main.filter_nulls import filter_invalid_rows

class TestUtils(unittest.TestCase):
    def test_filter_invalid_rows(self):
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("Filter Invalid Rows Test") \
            .master("local[1]") \
            .getOrCreate()

        # Define input and output paths
        input_path = "test_filter_input.csv"
        output_path = "test_filter_output"

        # Create a sample input dataset
        with open(input_path, "w") as f:
            f.write("id,name,age\n")
            f.write("1,Alice,30\n")
            f.write(",Bob,25\n")
            f.write("3,,35\n")
            f.write("4,Charlie,40\n")

        # Run the filter
        filter_invalid_rows(input_path, output_path,True)

        # Read the output
        output_df = spark.read.csv(output_path, header=True, inferSchema=True)

        # Assert the results
        assert output_df.count() == 2  # Only 2 valid rows should remain
        assert set(row["id"] for row in output_df.collect()) == {1, 4}

        # Cleanup
        os.remove(input_path)
        shutil.rmtree(output_path)

            # Stop SparkSession
        spark.stop()

# Run the test
if __name__ == "__main__":
    unittest.main()