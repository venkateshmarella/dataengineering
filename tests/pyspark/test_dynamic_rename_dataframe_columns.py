from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.main.pyspark.dynamic_rename_dataframe_columns import DynamicRenameDataFrameColumns
from tests.utils.custom_test_runner_utils import CustomTestRunner
import unittest

class TestDynamicRenameColumns(unittest.TestCase):
    def setUp(self):
        # Initialize Spark session
        self.spark = SparkSession.builder \
                        .appName("Test Dynamic Rename Columns") \
                        .master("local[1]") \
                        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
                        .config("spark.hadoop.fs.local.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
                        .config("spark.hadoop.fs.local.checksum", "false") \
                        .getOrCreate()
        self.df_obj=DynamicRenameDataFrameColumns()


        # Sample DataFrame for testing
        self.df = self.spark.read.format("csv").load("resources/customer_data.csv", header=True, inferSchema=True)

        self.df.show()

    def test_rename_columns(self):
        # Sample rename dictionary
        #if columns are not present in the DataFrame, they should be ignored
        rename_dict = {
            "id": "customer_id",
            "name": "customer_name",
            "salary": "customer_salary",
            "age": "customer_age"
        }
        updated_df=self.df_obj.rename_columns(self.df, rename_dict)
        updated_df.show()
        self.assertEqual(updated_df.columns, ['customer_id', 'customer_name', 'customer_salary', 'location']) #location column should remain unchanged
        self.assertEqual(updated_df.count(), self.df.count())

if __name__ == "__main__":
    # Run the tests using the custom test runner
    runner = CustomTestRunner(verbosity=2)
    unittest.main(testRunner=runner)