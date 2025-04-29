import unittest
from tests.utils.custom_test_runner_utils import run_tests,CustomTestRunner
class TestAutomatedTestsuite(unittest.TestCase):
    def test_addition(self):
        self.assertEqual(1 + 1, 2)

    def test_subtraction(self):
        self.assertEqual(5 - 3, 1) #Error Test

    def test_multiplication(self):
        self.assertEqual(3 * 3, 9)

    def test_division_by_zero(self):
        with self.assertRaises(ZeroDivisionError):
            result = 1 / 0  # This will raise ZeroDivisionError

    @unittest.skip("Skipping this test for demonstration purposes")
    def test_skip_example(self):
        pass
# Ensure this is the entry point if running as a script
if __name__ == "__main__":
    # run_tests(TestAutomatedTestsuite)
    runner = CustomTestRunner(verbosity=2)
    unittest.main(testRunner=runner)