import unittest
import os,sys
from pyspark.sql import SparkSession
from src.main.pyspark.create_dataframe import DataframeUtils
from tests.utils.custom_test_runner_utils import run_tests,CustomTestRunner

print(os.getenv("PYTHONPATH"))


class TestDataframeUtils(unittest.TestCase):
    # The __init__ method is not required for unit tests in most cases unless necessary.
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestDataframeUtils") \
            .getOrCreate()
        self.dataframe_utils = DataframeUtils(self.spark)

    @unittest.skip("Skipping test_create_dataframe")
    def test_create_dataframe(self):
        data = [(1,'Alice'),(2,'Bob'),(3,'Charlie')]
        schema = ['id','name']
        df = self.dataframe_utils.create_dataframe(data, schema)
        assert df.count() == 3, "DataFrame count does not match expected value."
        assert df.columns == schema, "DataFrame schema does not match expected value."

    def test_create_dataframe_from_csv(self):
        # Create a temporary CSV file for testing
        csv_file_path = "resources\\customer_data.csv"
        
        df = self.dataframe_utils.create_dataframe_from_csv(csv_file_path)
        df.show()
        assert df.count() == 4, "DataFrame count does not match expected value."
        assert df.columns == ['id', 'name','salary','location'], "DataFrame schema does not match expected value."
        

    # Optional: Cleanup Spark session after tests
    def tearDown(self):
        self.spark.stop()

if __name__ == "__main__":
    # Run the tests using the custom test runner
    runner = CustomTestRunner(verbosity=2)
    unittest.main(testRunner=runner)