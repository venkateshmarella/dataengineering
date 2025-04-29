from pyspark.sql import functions as F


# Measure	Description	                        Best Used When
# Mean	    Arithmetic average of all values.	Data is symmetrically distributed with no extreme outliers.
# Median	Middle value when data is sorted.	Data has outliers or is skewed.
# Mode	    Most frequently occurring value.	You want to preserve most common category/value. Often used for categorical data.
class FindMeanModeMedian:
    def __init__(self, spark):
        self.spark = spark

    def get_result_from_operation(self, df, column,operation):
        if operation == "mean":
            res=df.select(F.mean(col=column)).collect()[0][0]
            return df.fillna({f'{column}': res})
        elif operation == "median":
            res=df.approxQuantile(column,[0.5],0.01)[0] #A list of quantiles to calculate. 0.5 is the median (50th percentile) # and 0.01 is the relative error
            return df.fillna({f'{column}': res})
        elif operation == "mode":
            res=df.groupBy(column).agg(F.count("*").alias("cnt")).orderBy(F.desc("cnt")).first()[0]
            print(res)
            return df.fillna({f'{column}': res})
        else:
            raise ValueError("Invalid operation. Use 'mean', 'mode', or 'median'.")

