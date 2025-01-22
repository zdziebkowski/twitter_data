from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode_outer, col, avg


class TweetsAnalyzer:
    # Class constants for column names
    HASHTAG_COLUMN = "hashtags"
    IS_RETWEET_COLUMN = "is_retweet"
    SOURCE_COLUMN = "source"
    USER_FOLLOWERS = "user_followers"
    USER_NAME = "user_name"
    USER_LOCATION = "user_location"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def calculate_hashtags(self, df: DataFrame) -> DataFrame:
        """
        AGGREGATION
        Args:
            df: Input DataFrame
        Returns:
            DataFrame with columns hashtag,count
        """
        return (df.withColumn(self.HASHTAG_COLUMN, explode_outer(col(self.HASHTAG_COLUMN)))
                .groupBy(self.HASHTAG_COLUMN)
                .count())

    def calculate_is_retweet_count(self, df: DataFrame) -> DataFrame:
        """
        AGGREGATION
        Args:
            df: Input DataFrame
        Returns:
            DataFrame with columns is_retweet,count
        """
        return df.groupBy(self.IS_RETWEET_COLUMN).count()

    def calculate_source_count(self, df: DataFrame) -> DataFrame:
        """
        AGGREGATION
        Args:
            df: Input DataFrame
        Returns:
            DataFrame with columns source,count
        """
        return df.groupBy(self.SOURCE_COLUMN).count()

    def calculate_avg_user_followers_per_location(self, df: DataFrame) -> DataFrame:
        """
        AGGREGATION
        Args:
            df: Input DataFrame
        Returns:
            DataFrame with columns user_location,avg
        """
        return (df.select(self.USER_NAME, self.USER_FOLLOWERS, self.USER_LOCATION)
                .filter(col(self.USER_NAME).isNotNull())
                .filter(col(self.USER_LOCATION).isNotNull())
                .dropDuplicates([self.USER_NAME])
                .groupBy(self.USER_LOCATION)
                .agg(avg(self.USER_FOLLOWERS).alias("avg")))
