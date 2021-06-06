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
    song_data = "s3a://udacity-dend/song_data/A/A/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # check schema
    # df.printSchema()
    # |-- artist_id: string (nullable = true)
    # |-- artist_latitude: double (nullable = true)
    # |-- artist_location: string (nullable = true)
    # |-- artist_longitude: double (nullable = true)
    # |-- artist_name: string (nullable = true)
    # |-- duration: double (nullable = true)
    # |-- num_songs: long (nullable = true)
    # |-- song_id: string (nullable = true)
    # |-- title: string (nullable = true)
    # |-- year: long (nullable = true)

    # extract columns to create songs table
    songs_table = df["song_id","title","artist_id","year","duration"]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet("s3://spotify-dimensional-model-data-lake/song_table/")

    # extract columns to create artists table
    artists_table = df["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]
    artists_table.dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("s3://spotify-dimensional-model-data-lake/artists_table/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "s3://udacity-dend/log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # check schema
    # df.printSchema()
    # |-- artist: string (nullable = true)
    # |-- auth: string (nullable = true)
    # |-- firstName: string (nullable = true)
    # |-- gender: string (nullable = true)
    # |-- itemInSession: long (nullable = true)
    # |-- lastName: string (nullable = true)
    # |-- length: double (nullable = true)
    # |-- level: string (nullable = true)
    # |-- location: string (nullable = true)
    # |-- method: string (nullable = true)
    # |-- page: string (nullable = true)
    # |-- registration: double (nullable = true)
    # |-- sessionId: long (nullable = true)
    # |-- song: string (nullable = true)
    # |-- status: long (nullable = true)
    # |-- ts: long (nullable = true)
    # |-- userAgent: string (nullable = true)
    # |-- userId: string (nullable = true)
    # df.show(5)

    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")
    #df.show(5)

    # extract columns for users table    
    users_table = df["userId","firstName","lastName","gender","level"]
    users_table.dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet("s3://spotify-dimensional-model-data-lake/users_table/")

    # create timestamp column from original timestamp column
    #+-----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+
    #|     artist|     auth|firstName|gender|itemInSession|lastName|   length|level|            location|method|    page|     registration|sessionId|                song|status|           ts|           userAgent|userId|
    #+-----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+
    #|   Harmonia|Logged In|     Ryan|     M|            0|   Smith|655.77751| free|San Jose-Sunnyval...|   PUT|NextSong|1.541016707796E12|      583|       Sehr kosmisch|   200|1542241826796|"Mozilla/5.0 (X11...|    26|
    #|The Prodigy|Logged In|     Ryan|     M|            1|   Smith|260.07465| free|San Jose-Sunnyval...|   PUT|NextSong|1.541016707796E12|      583|     The Big Gundown|   200|1542242481796|"Mozilla/5.0 (X11...|    26|
    #|      Train|Logged In|     Ryan|     M|            2|   Smith|205.45261| free|San Jose-Sunnyval...|   PUT|NextSong|1.541016707796E12|      583|            Marry Me|   200|1542242741796|"Mozilla/5.0 (X11...|    26|
    #|Sony Wonder|Logged In|   Samuel|     M|            0|Gonzalez|218.06975| free|Houston-The Woodl...|   PUT|NextSong|1.540492941796E12|      597|           Blackbird|   200|1542253449796|"Mozilla/5.0 (Mac...|    61|
    #|  Van Halen|Logged In|    Tegan|     F|            2|  Levine|289.38404| paid|Portland-South Po...|   PUT|NextSong|1.540794356796E12|      602|Best Of Both Worl...|   200|1542260935796|"Mozilla/5.0 (Mac...|    80|
    #+-----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+
    get_timestamp = udf( lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf( lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("date_time",get_datetime(df.ts))
    df.show(5)
    
    # extract columns to create time table
    #time_table = df["start_time",df.date_time.hour,df.date_time.day,df.date_time.week,df.date_time.month,df.date_time.year,df.date_time.weekday]
    time_table = df.select('start_time',hour(df.date_time).alias('hour'),
                       dayofmonth(df.date_time).alias('day'),
                       weekofyear(df.date_time).alias('week'),
                       month(df.date_time).alias('month'),
                       year(df.date_time).alias('year'),
                       date_format(df.date_time,'E').alias('weekday')
    ).dropDuplicates()
    #time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet("s3://spotify-dimensional-model-data-lake/time_table/")

    # read in song data to use for songplays table
    song_data = "s3a://udacity-dend/song_data/A/A/*/*.json"
    song_df = spark.read.json(song_data)    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), "inner" ).dropDuplicates()
    #songplays_table = songplays_table.withColumn("songplay_id", str(songplays_table.userId)+str(songplays_table.sessionId)+str(songplays_table.itemInSession))
    songplays_table = songplays_table.withColumn("songplay_id", songplays_table.userId+str(songplays_table.sessionId)+str(songplays_table.itemInSession))
    songplays_table.printSchema()
    songplays_table = songplays_table.withColumn('year', year(songplays_table.start_time).alias('year'))
    songplays_table = songplays_table.withColumn('month', month(songplays_table.start_time).alias('month'))
    songplays_table = songplays_table["songplay_id","start_time","userId","level","song_id","artist_id","sessionId","location","userAgent","year","month"]

    # write songplays table to parquet files partitioned by year and month
    # songplays_table.write.partitionedBy("year","month").mode("overwrite").parquet("s3://spotify-dimensional-model-data-lake/songplays_table/")
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet("{}/{}".format(output_data,"songplays_table"))

#@udf(datetime)
#def get_datetime(ts):
#    return datetime.fromtimestamp(ts * 1000)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://spotify-dimensional-model-data-lake"
    
    ##TODO# process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
