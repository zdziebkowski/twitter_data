from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, col, to_timestamp
from pyspark.sql.types import LongType


class TweetsCleaner:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def clean_all_tweets(self, df):
        """
        Clean tweet data by:
        - Removing brackets from hashtags
        - Converting string timestamps to proper timestamp type
        - Converting string numeric fields to proper long type
        """
        return (df
                # Clean hashtags by removing brackets
                .withColumn("hashtags", regexp_replace(col("hashtags"), "\[", ""))
                .withColumn("hashtags", regexp_replace(col("hashtags"), "\]", ""))
                .withColumn("hashtags", regexp_replace(col("hashtags"), "\\[", ""))
                .withColumn("hashtags", regexp_replace(col("hashtags"), "\\]", ""))
                # Split hashtags string into array
                .withColumn("hashtags", split(col("hashtags"), ","))
                # Convert date string to timestamp
                .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
                # Convert user_created to timestamp
                .withColumn("user_created", to_timestamp(col("user_created")))
                # Convert numeric fields to long type
                .withColumn("user_favourites", col("user_favourites").cast(LongType()))
                .withColumn("user_friends", col("user_friends").cast(LongType()))
                .withColumn("user_followers", col("user_followers").cast(LongType()))
                )
