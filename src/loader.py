from pyspark.sql import SparkSession
from pyspark.sql import DataFrame




class DataLoader:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark



    def load_parquet(self, path: str) -> DataFrame:
        """
        Load parquet file into a Spark DataFrame
        """

        return self.spark.read.parquet(path)
