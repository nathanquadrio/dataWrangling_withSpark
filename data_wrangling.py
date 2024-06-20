# Take care of any imports
import datetime
import os
import pyspark                          # type: ignore
import logging
import numpy as np                      # type: ignore
import pandas as pd                     # type: ignore
import matplotlib.pyplot as plt         # type: ignore

from pyspark.sql import SparkSession            # type: ignore
from pyspark.sql import Window                  # type: ignore
from pyspark.sql.functions import udf, desc, count, col, when  # type: ignore
from pyspark.sql.functions import sum as Fsum   # type: ignore
from pyspark.sql.types import IntegerType       # type: ignore


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the JAVA_HOME environment variable
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"

# Define file paths
input_path = "./data/sparkify_log_small.json"

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("Wrangling Data").getOrCreate()
        logger.info("Spark session created.")

        # Read JSON file into DataFrame
        user_log_df = spark.read.json(input_path)
        logger.info("Data read from JSON file.")

        # Show schema and some data for verification
        #user_log_df.printSchema()
        #user_log_df.show(n=5)

        # Describe the dataframe
        #user_log_df.describe().show()

        # Describe the statistics for the song length column
        #user_log_df.describe("length").show()

        # Count the rows in the dataframe
        #print(user_log_df.count())

        # Select the page column, drop duplicates, and sort by page
        #user_log_df.select("page").dropDuplicates().sort("page").show()

        # Select data for all pages where userId is 1046
        # user_log_df.select(["userId", "firstname", "page", "song"]) \
        #     .where(user_log_df.userId == "1046") \
        #     .show()
        
        # # Calculate Statistics by Hour
        get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
        user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))
        #print(user_log_df.head(1))

        # Select just the NextSong page
        songs_in_hour_df = user_log_df.filter(user_log_df.page == "NextSong") \
            .groupby(user_log_df.hour) \
            .count() \
            .orderBy(user_log_df.hour.cast("float"))
        
        #songs_in_hour_df.show()

        songs_in_hour_pd = songs_in_hour_df.toPandas()
        songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

        # plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
        # plt.xlim(-1, 24)
        # plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
        # plt.xlabel("Hour")
        # plt.ylabel("Songs played")
        # plt.show()

        # # Drop Rows with Missing Values
        #print(user_log_df.count())
        user_log_valid_df = user_log_df.dropna(how = "any", subset = ["userId", "sessionId"])
        
        # How many are there now that we dropped rows with null userId or sessionId?
        # print(user_log_valid_df.count())
        
        # select all unique user ids into a dataframe
        # user_log_df.select("page") \
        #     .dropDuplicates() \
        #     .sort("page").show()
        
        # Select only data for where the userId column isn't an empty string (different from null)
        user_log_valid_df = user_log_valid_df.filter(user_log_valid_df["userId"] != "")
        # print(user_log_valid_df.count())

        # # Users Downgrade Their Accounts
        #
        # Find when users downgrade their accounts and then show those log entries.
        # user_log_valid_df.filter("page = 'Submit Downgrade'").show()

        # user_log_df.select(["userId", "firstname", "page", "level", "song"]) \
        #     .where(user_log_df.userId == "1138") \
        #     .show()
        
        # Create a user defined function to return a 1 if the record contains a downgrade
        flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

        # Select data including the user defined function
        user_log_valid_df = user_log_valid_df \
            .withColumn("downgraded", flag_downgrade_event("page"))
        
        # print(user_log_valid_df.head(5))

        # Partition by user id
        # # Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
        windowval = Window.partitionBy("userId") \
            .orderBy(desc("ts")) \
            .rangeBetween(Window.unboundedPreceding, 0)
        
        # Fsum is a cumulative sum over a window - in this case a window showing all events for a user
        # Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
        user_log_valid_df = user_log_valid_df \
            .withColumn("phase", Fsum("downgraded") \
            .over(windowval))
        
        # user_log_valid_df.show()

        # Show the phases for user 1138 
        # user_log_valid_df \
        #     .select(["userId", "firstname", "ts", "page", "level", "phase"]) \
        #     .where(user_log_df.userId == "1138") \
        #     .sort("ts") \
        #     .show()
        
        # Select all unique pages
        all_pages_df = user_log_df.select("page").dropDuplicates()
        all_pages = [row.page for row in all_pages_df.collect()]

        # Select pages visited by the user with empty user_id
        visited_pages_df = user_log_df.filter(user_log_df.userId == "").select("page").dropDuplicates()
        visited_pages = [row.page for row in visited_pages_df.collect()]
        
        # Determine pages not visited by the user with empty user_id
        not_visited_pages = set(all_pages) - set(visited_pages)
        
        # Show results
        logger.info(f"Pages not visited by user with empty user_id: {not_visited_pages}")

        # # What is the total play count for the most frequently played artist?
        # Count the number of female users
        female_users_df = user_log_df.filter(user_log_df.gender == "F").select("userId").distinct()
        num_female_users = female_users_df.count()
        
        logger.info(f"Number of female users: {num_female_users}")

        # Filter out rows where the artist is "None"
        valid_artist_df = user_log_df.filter(user_log_df.artist.isNotNull())
        
        # Group by artist and count the number of plays
        artist_play_counts_df = valid_artist_df.groupby("artist").agg(count("*").alias("play_count"))
        
        # Identify the artist with the highest play count
        most_played_artist_df = artist_play_counts_df.orderBy(desc("play_count")).limit(1)
        
        # Show the result
        most_played_artist = most_played_artist_df.collect()[0]
        artist_name = most_played_artist["artist"]
        play_count = most_played_artist["play_count"]
        
        logger.info(f"The most frequently played artist is '{artist_name}' with a total play count of {play_count}")

        # # How many times do users listen to a song on average between visits to the 'Home' page?
        # Filter the dataset to include only 'Home' and 'NextSong' pages
        filtered_df = user_log_df.filter(user_log_df.page.isin("Home", "NextSong"))

        # Order the dataset by timestamp and user ID
        windowval = Window.partitionBy("userId").orderBy("ts")

        # Create a cumulative sum that increments when a 'Home' page is encountered
        filtered_df = filtered_df.withColumn("home_flag", when(col("page") == "Home", 1).otherwise(0))
        filtered_df = filtered_df.withColumn("home_cumsum", Fsum("home_flag").over(windowval))

        # Count the 'NextSong' actions between each 'Home' visit
        next_song_counts = filtered_df.filter(filtered_df.page == "NextSong") \
            .groupBy("userId", "home_cumsum") \
            .agg(count("page").alias("next_song_count"))

        # Compute the average number of 'NextSong' actions between 'Home' visits across all users
        avg_next_song_count = next_song_counts.agg({"next_song_count": "avg"}).collect()[0][0]

        # Round the average to the closest integer
        avg_next_song_count = round(avg_next_song_count)

        logger.info(f"Average number of songs listened to between visits to the 'Home' page: {avg_next_song_count}")


    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    finally:
        # Stop the Spark session
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()