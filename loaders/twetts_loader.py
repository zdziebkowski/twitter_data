from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit


class TweetsLoader:
    # Class constants
    COVID_LABEL = "covid"
    GRAMMYS_LABEL = "grammys"
    FINANCE_LABEL = "finance"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_all_tweets(self) -> DataFrame:
        # Load individual datasets
        covid_df = self.load_covid()
        financial_df = self.load_financial()
        grammys_df = self.load_grammys()

        # Combine datasets using union by name
        return covid_df.unionByName(
            financial_df,
            allowMissingColumns=True
        ).unionByName(
            grammys_df,
            allowMissingColumns=True
        )

    def load_financial(self) -> DataFrame:
        return (self.spark_session.read
                .option("header", "true")
                .csv(path="financial.csv")
                .withColumn("category", lit(TweetsLoader.FINANCE_LABEL))
                .na.drop()
                )

    def load_grammys(self) -> DataFrame:
        return (self.spark_session.read
                .option("header", "true")
                .csv(path="GRAMMYs_tweets.csv")
                .withColumn("category", lit(TweetsLoader.GRAMMYS_LABEL))
                .na.drop()
                )

    def load_covid(self) -> DataFrame:
        return (self.spark_session.read
                .option("header", "true")
                .csv(path="covid19_tweets.csv")
                .withColumn("category", lit(TweetsLoader.COVID_LABEL))
                .na.drop()
                )
