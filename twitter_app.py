from pyspark.sql import SparkSession
from pyspark.sql.functions import desc  # Import for descending order
from loaders.twetts_loader import TweetsLoader
from cleaners.twetts_cleaner import TweetsCleaner
from analysers.twetts_analyser import TweetsAnalyzer


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("twitter-data-analysis") \
        .master("local[*]") \
        .getOrCreate()

    # Initialize components
    tweets_loader = TweetsLoader(spark)
    tweet_cleaner = TweetsCleaner(spark)
    tweets_analyzer = TweetsAnalyzer(spark)

    # Load and clean data
    tweets_df = tweets_loader.load_all_tweets()
    tweets_cleaned_df = tweet_cleaner.clean_all_tweets(tweets_df)

    # Example analytics with sorting
    hashtag_counts = tweets_analyzer.calculate_hashtags(tweets_cleaned_df) \
        .orderBy(desc("count"))

    source_counts = tweets_analyzer.calculate_source_count(tweets_cleaned_df) \
        .orderBy(desc("count"))

    retweet_counts = tweets_analyzer.calculate_is_retweet_count(tweets_cleaned_df) \
        .orderBy(desc("count"))

    follower_stats = tweets_analyzer.calculate_avg_user_followers_per_location(tweets_cleaned_df) \
        .orderBy(desc("avg"))

    # Show results (limiting to top 20 for readability)
    print("\nTop 20 Hashtags:")
    hashtag_counts.show(20)

    print("\nSources by Usage:")
    source_counts.show()

    print("\nRetweet Statistics:")
    retweet_counts.show()

    print("\nTop 20 Locations by Average Followers:")
    follower_stats.show(20)


if __name__ == "__main__":
    main()
