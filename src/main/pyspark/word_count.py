from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def word_count(input_path, output_path,test_run:bool =False):
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Word Count") \
        .getOrCreate()

    # Read the text file
    text_df = spark.read.text(input_path)

    # Split the lines into words
    words_df = text_df.select(explode(split(col("value"), "\\s+")).alias("word"))

    # Group by words and count
    word_count_df = words_df.groupBy("word").count()

    word_count_df.show()

    # Write the output to the specified path
    word_count_df.write.mode("overwrite").csv(output_path, header=True)

    # Stop the SparkSession
    if not test_run:
        spark.stop()