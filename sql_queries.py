gender_type = """DO $$ BEGIN 
                    CREATE TYPE gender_type AS ENUM ('F', 'M'); 
                EXCEPTION 
                    WHEN duplicate_object THEN null; 
                END $$; 
""" 
 
# DROP TABLES 
def drop_table(name): 
    return f"DROP TABLE IF EXISTS {name};" 
 
songplay_table_drop = drop_table('songplay') 
user_table_drop = drop_table('users') 
song_table_drop = drop_table('song') 
artist_table_drop = drop_table('artist') 
time_table_drop = drop_table('time') 
 
# CREATE TABLES 
 
songplay_table_create = ("""CREATE TABLE songplay ( 
        songply_id SERIAL PRIMARY KEY,
        timestamp NUMERIC(13,0), 
        user_id INT, 
        level VARCHAR(10), 
        song_id VARCHAR(25), 
        artist_id VARCHAR(25), 
        session_id INT, 
        location VARCHAR(55), 
        user_agent VARCHAR(250), 
        CONSTRAINT fk_artist_songplay_table
        FOREIGN KEY(artist_id) 
        REFERENCES artist(artist_id)
        ); 
""") 
 
user_table_create = (gender_type + """CREATE TABLE users ( 
        user_id INT PRIMARY KEY, 
        first_name VARCHAR(35), 
        last_name VARCHAR(35), 
        gender gender_type, 
        level VARCHAR(10) 
        ); 
""") 
 
song_table_create = ("""CREATE TABLE song ( 
        song_id VARCHAR(25) PRIMARY KEY, 
        title VARCHAR(100), 
        artist_id VARCHAR(25) REFERENCES artist(artist_id), 
        duration FLOAT4, 
        year INT
        );
""") 
 
artist_table_create = ("""CREATE TABLE artist ( 
        artist_id VARCHAR(25) PRIMARY KEY, 
        name VARCHAR(100), 
        location VARCHAR(35), 
        latitude FLOAT4, 
        longitude FLOAT4 
        ); 
""") 
 
time_table_create = ("""CREATE TABLE time ( 
        time_id SERIAL PRIMARY KEY,
        hour INT, 
        day_of_week INT, 
        week INT, 
        month INT, 
        year INT 
        ); 
""") 
 
# INSERT RECORDS 
 
songplay_table_insert = ("""INSERT INTO songplay (timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s); 
""") 
 
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET 
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        level = EXCLUDED.level; 
""") 
 
song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) 
        VALUES(%s, %s, %s, %s, %s); 
""") 
 
artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude) 
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT (artist_id) DO UPDATE SET 
        name = EXCLUDED.name,
        location = EXCLUDED.location,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude; 
""") 
 
 
time_table_insert = ("""INSERT INTO time (hour, day_of_week, week, month, year) 
        VALUES(%s, %s, %s, %s, %s); 
""") 
 
# FIND SONGS 
 
song_select = ("""SELECT song.song_id, song.artist_id FROM song 
        LEFT JOIN artist ON song.artist_id = artist.artist_id  
        WHERE song_id = %s AND title = %s AND duration = %s; 
""") 
 
# QUERY LISTS 
 
create_table_queries = [artist_table_create, song_table_create, songplay_table_create, user_table_create, time_table_create] 
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]