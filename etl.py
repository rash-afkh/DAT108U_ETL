import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import logging

logging.basicConfig(filename='etl.log', level=logging.ERROR)

def process_song_file(cur, filepath:str):
    """
    Reads the song files, creates Artist data and Song data and writes them into artist table and song table
    - Inputs:
        - cur: sql query cursor
        - filepath: string path to a song file
    - Outputs:
        - None
    """

    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as err:
        logging.error(f"Error reading song file {filepath}")
        logging.exception(err)
        return

    # insert artist record
    artist_data_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    try:
        artist_data = df[artist_data_columns].values[0]
    except Exception as err:
        logging.error(f"Error retrieving artist data for {filepath}")
        logging.exception(err)
        return
    try:
        cur.execute(artist_table_insert, artist_data)
    except Exception as err:
        logging.error(f"Error inserting artist data for {filepath}")
        logging.exception(err)
        return

    # insert song record
    song_data_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    try:
        song_data = df[song_data_columns].values[0]
    except Exception as err:
        logging.error(f"Error reading song data for {filepath}")
        logging.exception(err)
    try:
        cur.execute(song_table_insert, song_data)
    except Exception as err:
        logging.error(f"Error inserting song data for {filepath}")
        logging.exception(err)
        return


def process_log_file(cur, filepath:str):
    """
    Reads the log files, creates Time data, User data and Songplay data and writes them into time table, user table and songplay table
    - Inputs:
        - cur: sql query cursor
        - filepath: string path to a log file
    - Outputs:
        - None
    """
        
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception as err:
        logging.error(f"Error reading log file for {filepath}")
        logging.exception(err)
        return

    # filter by NextSong action
    try:
        df = df.loc[df['page'] == 'NextSong']
    except Exception as err:
        logging.error(f"Error reading the 'page' column in file {filepath}")
        logging.exception(err)
        return

    # convert timestamp column to datetime
    try:
        t = pd.to_datetime(df['ts'], unit='ms')
    except Exception as err:
        logging.error("Error converting the timestamp column")
        logging.exception(err)
        return
    
    # insert time data records
    try:
        time_data = [t.dt.hour.to_list(), t.dt.day_of_week.to_list(), t.dt.isocalendar().week.to_list(), 
                    t.dt.month.to_list(), t.dt.year.to_list()]
        column_labels = ['hour', 'day_of_week', 'week', 'month', 'year']
        time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    except Exception as err:
        logging.error(f"Error creating time dataframe for {filepath}")
        logging.exception(err)
        return

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as err:
            logging.error(f"Error inserting row {i} of {filepath} time data into the time table")
            logging.exception(err)

    # load user table
    try:
        user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    except Exception as err:
        logging.error(f"Error creating the user dataframe for {filepath}")
        logging.exception(err)
        return

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except Exception as err:
            logging.error(f"Error inserting row {i} of user data into user table for file {filepath}")
            return

    # insert songplay records
    for i, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except Exception as err:
            logging.error(f"Error performing song select for row {i} of {filepath}")
            logging.exception(err)
            continue

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        try:
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, 
                        row.sessionId, row.location, row.userAgent) 
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as err:
            logging.error(f"Error inserting songplay data for {filepath}")
            logging.exception(err)
            continue


def process_data(cur, conn:psycopg2.connect, filepath:str, func):
    """
    Data processing function that will call process_song_file and process_log_file per song and log file
    - Inputs:
        - cur: sql cursot
        - conn: database connection instance
        - filepath: path to parent directory of song or log files
        - func: data processing functon such as process_log_file and process_song_file
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()