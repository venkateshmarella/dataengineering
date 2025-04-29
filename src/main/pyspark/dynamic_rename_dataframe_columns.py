from pyspark.sql.functions import col
class DynamicRenameDataFrameColumns:
    """
    A class to dynamically rename DataFrame columns based on a mapping dictionary.
    """
    def __init__(self):
        """
        Initialize the DynamicRenameDataFrameColumns class.
        """
        pass

    def rename_columns(self, df, rename_dict):
        """
        Rename the DataFrame columns based on the provided mapping dictionary.

        :return: A new DataFrame with renamed columns.
        """
        return df.select([col(c).alias(rename_dict.get(c, c)) for c in df.columns])