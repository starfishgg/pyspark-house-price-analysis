

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, when
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline



class DataCleaner:

    def __init__(self, dataframe: DataFrame) -> None:
        """
        Initialise cleaner with a Spark DataFrame
        """

        self.df: DataFrame = dataframe


    def check_missing_values(self) -> None:
        """
        Returns the number of missing values per column.
        """

        missing = self.df.select(
            [
                sum(
                    col(column).isNull().cast("int")
                ).alias(column)
                for column in self.df.columns
            ]
        )

        print("Missing values:")
        missing.show()


    def remove_duplicates(self) -> None:
        """
        Removes duplicate rows.
        """

        before = self.df.count()
        self.df = self.df.dropDuplicates()
        after = self.df.count()

        print(
            f"Removed {before - after} duplicates"
        )


    def clean(self) -> DataFrame:

        self.check_missing_values()
        self.remove_duplicates()
        self.convert_boolean_columns()
        self.encode_furnishing_status()
        self.add_price_per_area()

        return self.df


    def convert_boolean_columns(self) -> None:

        boolean_columns = [
            "mainroad",
            "guestroom",
            "basement",
            "hotwaterheating",
            "airconditioning",
            "prefarea"
        ]

        for column in boolean_columns:
            self.df = self.df.withColumn(
                column,
                when(col(column) == "yes", 1)
                .when(col(column) == "no", 0)
                .otherwise(None)
            )

        

    def encode_furnishing_status(self) -> None:
        """
        Applied one-hot encoding to furnishing status.
        """


        indexer = StringIndexer(
            inputCol="furnishingstatus",
            outputCol="furnishing_index"
        )

        encoder = OneHotEncoder(
            inputCol="furnishing_index",
            outputCol="furnishing_encoded"
        )

        pipeline = Pipeline(
            stages=[
                indexer,
                encoder
            ]
        )

        model = pipeline.fit(self.df)

        self.df = model.transform(self.df)


    def add_price_per_area(self) -> None:
        """
        Creates price per square metre feature.
        """

        self.df = self.df.withColumn(
            "price_per_area",
            col("price") / col("area")
        )


    def save_parquet(self, path: str) -> None:
        """
        Saves cleaned DataFrame as a parquet file.
        
        Args:
            path: Output parquet file path.
        """

        self.df.write.mode("overwrite").parquet(path)
        