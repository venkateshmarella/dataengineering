# Import necessary modules for testing and path manipulation
import os, sys
import unittest

# Add the source directory to the system path to allow importing the target function
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Import the function to be tested from the source module
from main.python.check_even_or_odd_number import identify_even_or_odd

# Define a test class inheriting from unittest.TestCase
class TestUtils(unittest.TestCase):
    # Test case to verify if the function correctly identifies an even number
    def test_verify_even_number(self):
        self.assertEqual(identify_even_or_odd(4), "Even")

    # Test case to verify if the function correctly identifies an odd number
    def test_verify_odd_number(self):
        self.assertEqual(identify_even_or_odd(3), "Odd")
    
    # Test case to verify the function's behavior with a float input
    def test_verify_float_number(self):
        self.assertEqual(identify_even_or_odd(4.5), "Even")

    # Test case to verify the function's behavior with a valid string representation of a number
    def test_verify_valid_string_number(self):
        self.assertEqual(identify_even_or_odd("10"), "Even")

    # Test case to verify the function's behavior with an invalid string input
    def test_verify_invalid_string_number(self):
        self.assertEqual(identify_even_or_odd("Hello"), "not a valid value for even or odd validations. given input value - Hello")

# Entry point for running the tests
if __name__ == "__main__":
    # Run all test cases
    unittest.main()