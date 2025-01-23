from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from loaders.twetts_loader import TweetsLoader
from cleaners.twetts_cleaner import TweetsCleaner
from analysers.twetts_analyser import TweetsAnalyzer
from analysers.twetts_searcher import TweetsSearch


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
    tweets_searcher = TweetsSearch(spark)

    # Load and clean data
    tweets_df = tweets_loader.load_all_tweets()
    tweets_cleaned_df = tweet_cleaner.clean_all_tweets(tweets_df)

    # Analyze data
    hashtag_counts = tweets_analyzer.calculate_hashtags(tweets_cleaned_df) \
        .orderBy(desc("count"))

    source_counts = tweets_analyzer.calculate_source_count(tweets_cleaned_df) \
        .orderBy(desc("count"))

    retweet_counts = tweets_analyzer.calculate_is_retweet_count(tweets_cleaned_df) \
        .orderBy(desc("count"))

    follower_stats = tweets_analyzer.calculate_avg_user_followers_per_location(tweets_cleaned_df) \
        .orderBy(desc("avg"))

    # Search tweets
    trump_tweets = tweets_searcher.search_by_keyword("Trump", tweets_cleaned_df)
    print(f"\nTweets containing 'Trump': {trump_tweets.count()}")
    trump_tweets.select("text").show(20, truncate=False)

    trump_maga_tweets = tweets_searcher.search_by_keywords(["Trump", "MAGA"], tweets_cleaned_df)
    print(f"\nTweets containing 'Trump' or 'MAGA': {trump_maga_tweets.count()}")
    trump_maga_tweets.select("text").show(20, truncate=False)

    warsaw_tweets = tweets_searcher.only_in_location("Warsaw", tweets_cleaned_df)
    print(f"\nTweets from Warsaw: {warsaw_tweets.count()}")
    warsaw_tweets.select("text").show(20, truncate=False)

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
