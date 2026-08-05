

from pyspark.sql import SparkSession, DataFrame
import plotly.express as px

from src.loader import DataLoader
from src.analyzer import DataAnalyzer
from src.cleaner import DataCleaner
from src.visualizer import HouseVisualizer



def print_tests(df: DataFrame, spark: SparkSession) -> None:

    print("Master:")
    print(spark.sparkContext.master)

    print("Default parallelism:")
    print(spark.sparkContext.defaultParallelism)

    print("\nExecution plan:")
    df.explain(True)

    row_count = df.count()

    print(f"Number of rows: {row_count}")

    if row_count > 10:
        df.show(5)
    else:
        df.show()




def main():


    spark = (
        SparkSession.builder 
        .appName("HousePriceProject")
        .getOrCreate()
    )

    loader = DataLoader(spark)
    houses = loader.load_parquet("data/house-price.parquet")

    cleaner = DataCleaner(houses)
    cleaned_houses = cleaner.clean()

    cleaned_houses.printSchema()
    cleaner.save_parquet("data/house-price_CLEANED.parquet")

    analyzer = DataAnalyzer(cleaned_houses)
    # analyzer.print_report()
    
    
    fig: px = None

    visualizer = HouseVisualizer(cleaned_houses)
    fig = visualizer.plot_price_by_bedrooms()
    fig.show()
    fig = visualizer.plot_area_vs_price()
    fig.show()
    fig = visualizer.plot_price_by_furnishing()
    fig.show()
    fig = visualizer.plot_correlation_heatmap()
    fig.show()

    spark.stop()




if __name__ == "__main__":
    main()
