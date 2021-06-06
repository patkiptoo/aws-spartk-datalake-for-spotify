# aws-spartk-datalake-for-spotify
Big data processing of spotify listening log data.

## Overview and Purpose
This is a spark big data application that tested against AWS Spark Cluster emr-5.33.0
It processes song streaming log data for a streaming app spotify and builds schema on read star schema for anylytics. The logs are read from AWS S3 json format and written as parquet files.
The following star schema is created.

Fact Table

    songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday


### How to run

- Instantiate an emr cluster
- Login to master and scp the etl.py file
- Create a file dl.cfg with your credentials for emr cluster.

execute as follows:  
$ /usr/bin/spark-submit --master yarn etl.py




### Partitioning of parquet data

To improve analytical and further processing the songplays_table is partitioned by year and month. 

Amazon S3  
  |----spotify-dimensional-model-data-lake  
         |----------songplays_table/  
                  |---------------year=2018/  
                          |-------------------------month=11/  
                          
Amazon S3  
|------spotify-dimensional-model-data-lake  
|---------------------------------time_table/  
|----------------------------------------------------year=2018/  
|---------------------------------------------------------------month=11/ 


The song table is partitioned by year then artist

Amazon S3  
|------spotify-dimensional-model-data-lake  
|---------------------------------song_table/  
|----------------------------------------------------year=1964/  
|---------------------------------------------------------------artist_id=ARAJPHH1187FB5566A/  


Following are sample schemas 

### Sample Song Data Schema
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
    
    
### Sample log data 
    #+-----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+
    #|     artist|     auth|firstName|gender|itemInSession|lastName|   length|level|            location|method|    page|     registration|sessionId|                song|status|           ts|           userAgent|userId|
    #+-----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+
    #|   Harmonia|Logged In|     Ryan|     M|            0|   Smith|655.77751| free|San Jose-Sunnyval...|   PUT|NextSong|1.541016707796E12|      583|       Sehr kosmisch|   200|1542241826796|"Mozilla/5.0 (X11...|    26|
    #|The Prodigy|Logged In|     Ryan|     M|            1|   Smith|260.07465| free|San Jose-Sunnyval...|   PUT|NextSong|1.541016707796E12|      583|     The Big Gundown|   200|1542242481796|"Mozilla/5.0 (X11...|    26|
    #|      Train|Logged In|     Ryan|     M|            2|   Smith|205.45261| free|San Jose-Sunnyval...|   PUT|NextSong|1.541016707796E12|      583|            Marry Me|   200|1542242741796|"Mozilla/5.0 (X11...|    26|
    #|Sony Wonder|Logged In|   Samuel|     M|            0|Gonzalez|218.06975| free|Houston-The Woodl...|   PUT|NextSong|1.540492941796E12|      597|           Blackbird|   200|1542253449796|"Mozilla/5.0 (Mac...|    61|
    #|  Van Halen|Logged In|    Tegan|     F|            2|  Levine|289.38404| paid|Portland-South Po...|   PUT|NextSong|1.540794356796E12|      602|Best Of Both Worl...|   200|1542260935796|"Mozilla/5.0 (Mac...|    80|
    #+-----------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-----------------+---------+--------------------+------+-------------+--------------------+------+
    
