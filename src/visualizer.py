

import plotly.express as px
from pyspark.sql import DataFrame



class HouseVisualizer:

    def __init__(self, df: DataFrame) -> None:
        self.df = df


    def plot_price_by_bedrooms(self) -> px:
        """
        Creates a bar chart showing average price by bedrooms.
        """

        data = (
            self.df
            .groupBy("bedrooms")
            .avg("price")
            .toPandas()
        )

        fig = px.bar(
            data,
            x="bedrooms",
            y="avg(price)",
            title="Average House Price by Bedrooms",
            labels={
                "avg(price)": "Average Price",
                "bedrooms": "Bedrooms"
            }
        )

        return fig
    

    def plot_area_vs_price(self) -> px:
        """
        Shows relationship between house size and price.
        """

        data = self.df.toPandas()

        fig = px.scatter(
            data,
            x="area",
            y="price",
            title="House Area vs Price",
            labels={
                "area": "Area",
                "price": "Price"
            }
        )

        return fig


    def plot_price_by_furnishing(self) -> px:

        data = (
            self.df
            .groupBy("furnishingstatus")
            .avg("price")
            .toPandas()
        )

        fig = px.bar(
            data,
            x="furnishingstatus",
            y="avg(price)",
            title="Average Price by Furnishing Status"
        )

        return fig


    def plot_correlation_heatmap(self) -> px:

        columns = [
            "price",
            "area",
            "bedrooms",
            "bathrooms",
            "stories",
            "parking"
        ]

        data = (
            self.df
            .select(columns)
            .toPandas()
        )

        correlation = data.corr()

        fig = px.imshow(
            correlation,
            text_auto=True,
            title="Feature Correlations"
        )

        return fig
