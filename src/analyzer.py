

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, max, min, count, round




class DataAnalyzer:

    def __init__(self, dataframe: DataFrame) -> None:
        self.df = dataframe


    def total_houses(self) -> int:
        """
        Returns the total number of houses.
        """

        return self.df.count()


    def average_price_summary(self) -> DataFrame:
        """
        Calculates the average house price.
        """

        return (
            self.df
            .select(avg("price").alias("average_price"))
        )


    def get_price_range(self) -> DataFrame:
        """
        Returns minimum and maximum house prices.
        """

        return (
            self.df
            .select(
                min("price").alias("minimum_price"),
                max("price").alias("maximum_price")
            )
        )


    def houses_by_furnishing(self) -> DataFrame:
        """
        Counts houses by furnishing status.
        """

        return (
            self.df
            .groupBy("furnishingstatus")
            .agg(
                count("*").alias("number_of_houses")
            )
            .orderBy("number_of_houses", ascending=False)
        )


    def houses_by_parking(self) -> DataFrame:
        """
        Counts houses by parking status.
        """

        return (
            self.df
            .groupBy("parking")
            .agg(
                count("*").alias("number_of_houses")
            )
            .orderBy("parking")
        )


    def print_report(self):
        print("\nTotal houses:")
        print(self.total_houses())

        print("\nAverage price:")
        self.average_price_summary().show()

        print("\nPrice range:")
        self.get_price_range().show()

        print("\nHouses by furnishing:")
        self.houses_by_furnishing().show()

        print("\nHouses by parking:")
        self.houses_by_parking().show()

        print("\nAverage price by bedrooms:")
        self.average_price_by_bedrooms().show()

        print("\nAverage price per area:")
        self.average_price_per_area().show()

        print("\nAverage price by air conditioning:")
        self.average_price_by_airconditioning().show()

        print("\nAverage price by furnishing:")
        self.average_price_by_furnishing().show()

        print("\nAverage price by parking spaces:")
        self.average_price_by_parkingspaces().show()

        print("\nArea/price correlation:")
        print(self.area_price_correlation())


        print("\nAll numerical correlations:")
        print(self.numerical_correlations())



    def average_price_by_bedrooms(self) -> DataFrame:
        """
        Calculates average house price grouped by number of bedrooms.
        """

        return (
            self.df
            .groupBy("bedrooms")
            .agg(
                round(avg("price"), 2).alias("average_price")
            )
            .orderBy("bedrooms")
        )


    def average_price_per_area(self) -> DataFrame:
        """
        Calculates average price per square metre.
        """

        return(
            self.df
            .select(
                round(avg("price_per_area"), 2)
                .alias("average_price_per_area")
            )
        )


    def average_price_by_airconditioning(self) -> DataFrame:
        """
        Calculates average price based on air conditioning availability.
        """

        return (
            self.df
            .groupBy("airconditioning")
            .agg(
                round(avg("price"), 2)
                .alias("average_price")
            )
            .orderBy("average_price", ascending=False)
        )


    def area_price_correlation(self) -> float:
        """
        Calculates Pearson correlation between area and price
        """

        return self.df.stat.corr(
            "area",
            "price"
        )


    def average_price_by_furnishing(self) -> DataFrame:
        return (
            self.df
            .groupBy("furnishingstatus")
            .agg(
                round(avg("price"), 2)
                .alias("average_price")
            )
            .orderBy("average_price", ascending=False)
        )


    def average_price_by_parkingspaces(self) -> DataFrame:
        return(
            self.df
            .groupBy("parking")
            .agg(
                round(avg("price"), 2)
                .alias("average_price")
            )
            .orderBy("parking")
        )


    def numerical_correlations(self) -> dict:
        """
        Calculates correlations against house price.
        """

        columns = [
            "area",
            "bedrooms",
            "bathrooms",
            "stories",
            "parking"
        ]

        return {
            column: self.df.stat.corr(column, "price")
            for column in columns
        }