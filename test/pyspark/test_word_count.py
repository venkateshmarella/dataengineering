import os
import shutil
from pyspark.sql import SparkSession
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from main.pyspark.word_count import word_count

def test_word_count():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Word Count Test") \
        .master("local[1]") \
        .getOrCreate()

    # Input and output paths
    input_path = "test_input.txt"
    output_path = "test_output"

    # Create a sample input file
    with open(input_path, "w") as f:
        f.write("hello world\n")
        f.write("hello PySpark\n")
        f.write("hello world hello\n")

    # Run the word count program
    word_count(input_path, output_path,True)

    # Read the output
    output_df = spark.read.csv(output_path, header=True)

    # Convert the output to a dictionary
    output_dict = {row["word"]: int(row["count"]) for row in output_df.collect()}

    # Assert the results
    assert output_dict == {"hello": 4, "world": 2, "PySpark": 1}

    # Cleanup
    os.remove(input_path)
    shutil.rmtree(output_path)

    # Stop the SparkSession
    try:
        spark.stop()
    except NameError:
        print("spark session is already stopped")

# Run the test
if __name__ == "__main__":
    test_word_count()