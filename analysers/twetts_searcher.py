from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, split, array_intersect, size, array


class TweetsSearch:
    # Class constants for column names
    TEXT = "text"
    USER_LOCATION = "user_location"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def search_by_keyword(self, keyword: str, df: DataFrame) -> DataFrame:
        """
        Search tweets containing a specific keyword
        Args:
            keyword: String to search for
            df: Input DataFrame
        Returns:
            DataFrame with filtered rows containing the keyword
        """
        return df.filter(col(self.TEXT).contains(keyword))

    def search_by_keywords(self, keywords: list, df: DataFrame) -> DataFrame:
        """
        Search tweets containing any of the given keywords
        Args:
            keywords: List of strings to search for
            df: Input DataFrame
        Returns:
            DataFrame with filtered rows containing any of the keywords
        """
        return (df.withColumn(
            "keyWordsResult",
            array_intersect(
                split(col(self.TEXT), " "),
                array(keywords)
            )
        )
                .filter(~col("keyWordsResult").isNull() & (size(col("keyWordsResult")) > 0))
                .drop("keyWordsResult"))

    def only_in_location(self, location: str, df: DataFrame) -> DataFrame:
        """
        Filter tweets by user location
        Args:
            location: Location to filter by
            df: Input DataFrame
        Returns:
            DataFrame with tweets from specified location
        """
        return df.filter(col(self.USER_LOCATION) == location)
