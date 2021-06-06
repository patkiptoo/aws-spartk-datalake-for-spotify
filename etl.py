import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, TimestampType, DateType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data =   "{}/{}".format(input_data,"song_data/A/A/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df["song_id","title","artist_id","year","duration"]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet("{}/{}".format(output_data,"song_table"))

    # extract columns to create artists table
    artists_table = df["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]
    artists_table.dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("{}/{}".format(output_data,"artists_table"))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "{}/{}".format(input_data,"log_data/2018/11/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df["userId","firstName","lastName","gender","level"]
    users_table.dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet("{}/{}".format(output_data,"users_table"))

    # create timestamp column from original timestamp column
    get_timestamp = udf( lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf( lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("date_time",get_datetime(df.ts))
    
    # extract columns to create time table
    #time_table = df["start_time",df.date_time.hour,df.date_time.day,df.date_time.week,df.date_time.month,df.date_time.year,df.date_time.weekday]
    time_table = df.select('start_time',hour(df.date_time).alias('hour'),
                       dayofmonth(df.date_time).alias('day'),
                       weekofyear(df.date_time).alias('week'),
                       month(df.date_time).alias('month'),
                       year(df.date_time).alias('year'),
                       date_format(df.date_time,'E').alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet("{}/{}".format(output_data,"time_table"))

    # read in song data to use for songplays table
    song_data = "{}/{}".format(input_data,"song_data/A/A/*/*.json")
    song_df = spark.read.json(song_data)    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), "inner" ).dropDuplicates()
    songplays_table = songplays_table.withColumn("songplay_id", songplays_table.userId+str(songplays_table.sessionId)+str(songplays_table.itemInSession))
    songplays_table = songplays_table.withColumn('year', year(songplays_table.start_time).alias('year'))
    songplays_table = songplays_table.withColumn('month', month(songplays_table.start_time).alias('month'))
    songplays_table = songplays_table["songplay_id","start_time","userId","level","song_id","artist_id","sessionId","location","userAgent","year","month"]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet("{}/{}".format(output_data,"songplays_table"))

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3://spotify-dimensional-model-data-lake"
    
    ##TODO# process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
