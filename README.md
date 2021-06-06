# aws-spartk-datalake-for-spotify
Big data processing of spotify listening log data.



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
    
