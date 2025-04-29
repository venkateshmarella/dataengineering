import unittest
import os,sys
from pyspark.sql import SparkSession
from src.main.pyspark.create_dataframe import DataframeUtils


class CustomTextTestRunner(unittest.TextTestResult):
    def __init__(self, stream, descriptions, verbosity=2):
        super().__init__(stream, descriptions, verbosity)
        self.successes = []  # List to store successful tests
        self.failures = []   # List to store failed tests
        self.errors = []     # List to store errors
        self.skipped = []    # List to store skipped tests
    
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestDataframeUtils") \
            .getOrCreate()
        self.dataframe_utils = DataframeUtils(self.spark)

    # Override the method for a successful test to add it to the 'successes' list
    def addSuccess(self, test):
        super().addSuccess(test)
        self.successes.append(test)

    # Override the method for a failed test to add it to the 'failures' list
    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.failures.append(test)

    # Override the method for an error to add it to the 'errors' list
    def addError(self, test, err):
        super().addError(test, err)
        self.errors.append(test)

    # Override the method for skipped tests
    def addSkip(self, test, reason):
        self.skipped.append(test)
        self.stream.write(f">> SKIPPED <<: {reason}\n")

    # Optional: Cleanup Spark session after tests
    def tearDown(self):
        self.spark.stop()

    #Unused method to run the test suite
    @staticmethod
    def runSuite(testcase_cls):
        suite = unittest.TestLoader().loadTestsFromTestCase(testcase_cls)
        runner = unittest.TextTestRunner()
        result = runner.run(suite)

        print(f"Are all tests successful: {result.wasSuccessful()}")
        # Extract successful tests by filtering out failures and errors
        print(f"Number of tests run: {result.testsRun}")
        print(f"Number of failures: {len(result.failures)}")
        print(f"Number of errors: {len(result.errors)}")
        print(f"Number of skipped tests: {len(result.skipped)}")
        # Printing the skipped tests
        print(f"Skipped tests: {result.skipped}")

# Custom Test Runner Class
class CustomTestRunner(unittest.TextTestRunner):
    def _makeResult(self):
        return CustomTextTestRunner(self.stream, self.descriptions, self.verbosity)

# Utility function to run tests and categorize results
def run_tests(test_class):
    # Load tests from the test class
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    
    # Create a custom test runner
    runner = CustomTestRunner()
    
    # Run the tests and capture results
    result = runner.run(suite)
    
    # Print the segregated results
    print("Successful Tests:")
    for test in result.successes:
        print(test)

    print("\nFailed Tests:")
    for test in result.failures:
        print(test)

    print("\nErrored Tests:")
    for test in result.errors:
        print(test)

    print("\nSkipped Tests:")
    for test in result.skipped:
        print(test)

    print(f"\nTotal tests run: {result.testsRun}")