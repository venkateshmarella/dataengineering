from pyspark.sql import SparkSession
class DataframeUtils:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        if not self.spark:
            raise Exception("Spark session could not be set in main module. Please check your Spark installation.")
        print(f"DataframeUtils initialized with Spark version : {self.spark.sparkContext.version}")

    def create_dataframe(self, data=None, schema=None):
        """
        Create a DataFrame from the given data and schema.

        :param data: List of tuples or list of dictionaries.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        if data is None:
            data = []
        if schema is None:
            schema = []
        return self.spark.createDataFrame(self.spark.sparkContext.parallelize(data, numSlices=2),schema=schema)
        
    def create_dataframe_from_csv(self, file_path, header=True, inferSchema=True):
        """
        Create a DataFrame from a CSV file.

        :param file_path: Path to the CSV file.
        :param header: Whether the first row contains headers.
        :param inferSchema: Whether to infer the schema of the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.csv(file_path, header=header, inferSchema=inferSchema)
    
    def create_dataframe_from_json(self, file_path, schema=None):
        """
        Create a DataFrame from a JSON file.

        :param file_path: Path to the JSON file.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.json(file_path, schema=schema)
    
    def create_dataframe_from_parquet(self, file_path, schema=None):
        """
        Create a DataFrame from a Parquet file.

        :param file_path: Path to the Parquet file.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.parquet(file_path, schema=schema)
    
    def create_dataframe_from_orc(self, file_path, schema=None):
        """
        Create a DataFrame from an ORC file.

        :param file_path: Path to the ORC file.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.orc(file_path, schema=schema)
    
    def create_dataframe_from_avro(self, file_path, schema=None):
        """
        Create a DataFrame from an Avro file.

        :param file_path: Path to the Avro file.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.format("avro").load(file_path, schema=schema)
    
    def create_dataframe_from_sql(self, query, connection_properties):
        """
        Create a DataFrame from a SQL query.

        :param query: SQL query to execute.
        :param connection_properties: Connection properties for the database.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.jdbc(url=connection_properties['url'], table=query, properties=connection_properties)
    
    def create_dataframe_from_rdd(self, rdd, schema=None):
        """
        Create a DataFrame from an RDD.

        :param rdd: RDD to convert to DataFrame.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.createDataFrame(rdd, schema=schema)
    
    def create_dataframe_from_text(self, file_path, schema=None):
        """
        Create a DataFrame from a text file.

        :param file_path: Path to the text file.
        :param schema: Optional schema for the DataFrame.
        :return: A PySpark DataFrame.
        """
        return self.spark.read.text(file_path, schema=schema)
    
    def create_empty_dataframe(self, schema):
        """
        Create an empty DataFrame with the given schema.

        :param schema: Schema for the empty DataFrame.
        :return: An empty PySpark DataFrame.
        """
        return self.spark.createDataFrame([], schema)
    
    def create_empty_dataframe_spark_sql(self, sql_query='SELECT 1 AS col_0 LIMIT 0'):
        """
        Create an empty DataFrame using a SQL query.

        :param sql_query: SQL query to create an empty DataFrame.
        :return: An empty PySpark DataFrame.
        """
        return self.spark.sql(sql_query).limit(0)
