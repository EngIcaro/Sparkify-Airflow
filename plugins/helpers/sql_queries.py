class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)


    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM fact_songplays
    """)
# FROM songplays
    staging_events_create = ("""CREATE TABLE IF NOT EXISTS staging_events 
                                ( 	artist      varchar(256)    ,
                                    auth        varchar(256)    ,
                                    firstname   varchar(256)    ,
                                    gender      varchar(256)    ,
                                    iteminsession int4          ,
                                    lastname    varchar(256)    ,
                                    length      numeric(18,0)   ,
                                    "level"     varchar(256)    ,
                                    location    varchar(256)    ,
                                    "method"    varchar(256)    ,
                                    page        varchar(256)    ,
                                    registration numeric(18,0)  ,
                                    sessionid   int4            ,
                                    song        varchar(256)    ,
                                    status      int4            ,
                                    ts          int8            ,   
                                    useragent   varchar(256)    ,
                                    userid      int4 );
    """)

    staging_songs_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
                                ( 	song_id          varchar               ,
                                    num_songs        int                   ,
                                    artist_id        varchar               ,
                                    artist_latitude  float                 , 
                                    artist_longitude float                 ,
                                    artist_location  varchar               ,
                                    artist_name      varchar               ,
                                    title            varchar               ,
                                    duration         float                 ,
                                    year             int    );
    """)


    fact_songplays_create = ("""CREATE TABLE IF NOT EXISTS fact_songplays 
                            ( songplay_id varchar            ,
                              start_time  timestamp          ,
                              user_id     int                ,
                              level       varchar            ,
                              song_id     varchar            ,
                              artist_id   varchar            ,
                              session_id  int                ,
                              location    varchar            ,
                              user_agent  varchar            ,
                              CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
                               );
    """)


    user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_users
                            (user_id     int        NOT NULL,
                             first_name  varchar            ,
                             last_name   varchar            ,
                             gender      varchar            ,
                             level       varchar            ,
                             CONSTRAINT users_pkey PRIMARY KEY (user_id));
                        """)

    song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs 
                            (song_id   varchar  NOT NULL    ,
                             title     varchar  NOT NULL    ,
                             artist_id varchar  NOT NULL    , 
                             year      int                  ,
                             duration  float    NOT NULL    ,
                             CONSTRAINT songs_pkey PRIMARY KEY (song_id));
                        """)

    
    artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artists
                            (artist_id        varchar NOT NULL   ,
                             artist_name      varchar NOT NULL   ,
                             artist_location  varchar            ,
                             artist_latitude  float              ,
                             artist_longitude float);
                        """)


    time_table_create = (""" CREATE TABLE IF NOT EXISTS dim_time
                            (start_time timestamp    NOT NULL ,
                             time_hour         int     ,   
                             time_day          int     ,
                             time_week         int     ,
                             time_month        int     ,
                             time_year         int     ,
                             time_weekday      int     ,
                             CONSTRAINT time_pkey PRIMARY KEY (start_time));

                     """)
