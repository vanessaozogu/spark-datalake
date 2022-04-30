import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This functions creates a new Spark Session for processing
    the data or updates an existing one if it already exists
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads .JSON input data (song_data) from input_data path,
    process the data to extract song_table and artists_table and
    stores queried data to parquet files

    Args
    spark: sparkSession
    input_data: Location of the song_data .json files
    output_data: Location where parquet data files of dimension tables will be stored in S3
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    sfields = ['title', 'artist_id', 'year', 'duration']

    songs_table = df.select(sfields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    afields = ["artist_id", "artist_name as name", "artist_location as location",\
               "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(afields).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function loads log_data from input_data path,
    process the data to extract song_table and artists_table and
    stores queried data to parquet files. The output from the last
    function are also used in the spark.read.json command

    Args
    spark: sparkSession
    input_data: Location of the log_data .json files
    output_data: Location where parquet data files of dimension tables will be stored in S3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    ufields = ["userdId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]

    users_table = df.selectExpr(ufields).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("start_time", get_datetime('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(date_convert, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))).withColumn("day", day(col("start_time"))) \
        .withColumn("week", week(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", date_format(col("start_time"), 'E'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/*/*/*")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =  (df.withColumn("songplay_id", F.monotonically_increasing_id())
                       .join(song_df, song_df.title == df.song).select("songplay_id",
                       col("ts_timestamp").alias("start_time"),
                       col("userId").alias("user_id"), "level", "song_id", "artist_id",
                       col("sessionId").alias("session_id"), "location",
                       col("userAgent").alias("user_agent"))
                        )

    # write songplays table to parquet files partitioned by year and month
    #songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays.parquet")


def main():
    """
    This function extracts data from S3 bucket, transforms it into
    dimensional tables format and loads it back to S3 in parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://results"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
