import unittest
from pyspark.sql import SparkSession
from src.main.pyspark.find_mean_mode_median import FindMeanModeMedian
from tests.utils import custom_test_runner_utils

class TestFindMeanModeMedian(unittest.TestCase):
    def setUp(self):
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("TestFindMeanModeMedian") \
            .master("local[*]") \
            .getOrCreate()
        self.spark.sql("""
        SELECT * FROM VALUES
            (1, 'Male'),
            (2, NULL),
            (3, 'Female'),
            (4, 'Female'),
            (5, 'Male'),
            (6, 'Female'),
            (3, NULL),
            (7, 'Male')
        AS data(id, Gender)
    """).createOrReplaceTempView("people")
        # Now you can query it with Spark SQL
        self.df = self.spark.sql("SELECT * FROM people")
        self.df.show()

        self.obj=FindMeanModeMedian(self.spark)

    # def teardown_method(self):
        # self.spark.stop()

    def test_mean(self):
        # Test mean operation
        self.obj.get_result_from_operation(self.df, "id", "mean").show()
        
    def test_median(self):
        # Test median operation
        self.obj.get_result_from_operation(self.df, "id", "median").show()
    
    def test_mode(self):
        # Test mode operation
        self.obj.get_result_from_operation(self.df, "Gender", "mode").show()
if __name__ == "__main__":
    # Run the tests using the custom test runner
    custom_test_runner_utils.run_tests(TestFindMeanModeMedian)
    unittest.main()