from pyspark.sql import SparkSession


# Create SparkSession
def get_spar_session(app_name: str, master: str = 'local[*]') -> SparkSession:
    """Creates and returns a configured SparkSession"""
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()


def load_data(spark: SparkSession, path: str):
    """Load data from path to a DataFrame"""
    return spark.read.csv(path, header=True, inferSchema=True)


def main():
    spark = get_spar_session('Twitter App')
    df_grammys: DataFrame = load_data(spark, 'GRAMMYs_tweets.csv')
    df_grammys.show(truncate=False)
    df_grammys.printSchema()
    df_financial: DataFrame = load_data(spark, 'financial.csv')
    df_financial.show(truncate=False)
    df_financial.printSchema()
    df_covid: DataFrame = load_data(spark, 'covid19_tweets.csv')
    df_covid.show(truncate=False)
    df_covid.printSchema()


if __name__ == "__main__":
    main()
