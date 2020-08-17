{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['dwh.cfg']"
      ]
     },
     "execution_count": 6,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import configparser\n",
    "config=configparser.ConfigParser()\n",
    "config.read('dwh.cfg')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {},
   "outputs": [],
   "source": [
    "LOG_DATA = config.get('S3', 'LOG_DATA')\n",
    "LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')\n",
    "SONG_DATA = config.get('S3', 'SONG_DATA')\n",
    "ARN = config.get(\"IAM_ROLE\", \"ARN\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "DROP TABLES"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [],
   "source": [
    "staging_events_table_drop = \"DROP TABLE IF EXISTS staging_events;\"\n",
    "staging_songs_table_drop = \"DROP TABLE IF EXISTS staging_songs;\"\n",
    "songplay_table_drop = \"DROP TABLE IF EXISTS songplays;\"\n",
    "user_table_drop = \"DROP TABLE IF EXISTS users;\"\n",
    "song_table_drop = \"DROP TABLE IF EXISTS songs;\"\n",
    "artist_table_drop = \"DROP TABLE IF EXISTS artists;\"\n",
    "time_table_drop = \"DROP TABLE IF EXISTS time;\""
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "CREATE TABLES"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [],
   "source": [
    "staging_events_table_create= (\"\"\"CREATE TABLE IF NOT EXISTS stagingevents (\n",
    "                event_id    BIGINT IDENTITY(0,1)    NOT NULL,\n",
    "                artist      VARCHAR                 NULL,\n",
    "                auth        VARCHAR                 NULL,\n",
    "                firstName   VARCHAR                 NULL,\n",
    "                gender      VARCHAR                 NULL,\n",
    "                itemInSession VARCHAR               NULL,\n",
    "                lastName    VARCHAR                 NULL,\n",
    "                length      VARCHAR                 NULL,\n",
    "                level       VARCHAR                 NULL,\n",
    "                location    VARCHAR                 NULL,\n",
    "                method      VARCHAR                 NULL,\n",
    "                page        VARCHAR                 NULL,\n",
    "                registration VARCHAR                NULL,\n",
    "                sessionId   INTEGER                 NOT NULL SORTKEY DISTKEY,\n",
    "                song        VARCHAR                 NULL,\n",
    "                status      INTEGER                 NULL,\n",
    "                ts          BIGINT                  NOT NULL,\n",
    "                userAgent   VARCHAR                 NULL,\n",
    "                userId      INTEGER                 NULL);\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "metadata": {},
   "outputs": [],
   "source": [
    "staging_songs_table_create = (\"\"\"CREATE TABLE IF NOT EXISTS stagingsongs (\n",
    "                num_songs           INTEGER         NULL,\n",
    "                artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,\n",
    "                artist_latitude     VARCHAR         NULL,\n",
    "                artist_longitude    VARCHAR         NULL,\n",
    "                artist_location     VARCHAR(500)   NULL,\n",
    "                artist_name         VARCHAR(500)   NULL,\n",
    "                song_id             VARCHAR         NOT NULL,\n",
    "                title               VARCHAR(500)   NULL,\n",
    "                duration            DECIMAL(9)      NULL,\n",
    "                year                INTEGER         NULL);\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [],
   "source": [
    "songplay_table_create = (\"\"\"CREATE TABLE IF NOT EXISTS songplays (\n",
    "                songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,\n",
    "                start_time  TIMESTAMP               NOT NULL,\n",
    "                user_id     VARCHAR(50)             NOT NULL DISTKEY,\n",
    "                level       VARCHAR(10)             NOT NULL,\n",
    "                song_id     VARCHAR(40)             NOT NULL,\n",
    "                artist_id   VARCHAR(50)             NOT NULL,\n",
    "                session_id  VARCHAR(50)             NOT NULL,\n",
    "                location    VARCHAR(100)            NULL,\n",
    "                user_agent  VARCHAR(255)            NULL\n",
    "    );\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {},
   "outputs": [],
   "source": [
    "user_table_create = (\"\"\" CREATE TABLE IF NOT EXISTS users (\n",
    "                user_id     INTEGER                 NOT NULL SORTKEY,\n",
    "                first_name  VARCHAR(50)             NULL,\n",
    "                last_name   VARCHAR(80)             NULL,\n",
    "                gender      VARCHAR(10)             NULL,\n",
    "                level       VARCHAR(10)             NULL\n",
    "    ) diststyle all;\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [],
   "source": [
    "song_table_create = (\"\"\"CREATE TABLE IF NOT EXISTS songs (\n",
    "                song_id     VARCHAR(50)             NOT NULL SORTKEY,\n",
    "                title       VARCHAR(500)           NOT NULL,\n",
    "                artist_id   VARCHAR(50)             NOT NULL,\n",
    "                year        INTEGER                 NOT NULL,\n",
    "                duration    DECIMAL(9)              NOT NULL\n",
    "    );\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [],
   "source": [
    "artist_table_create = (\"\"\"CREATE TABLE IF NOT EXISTS artists (\n",
    "                artist_id   VARCHAR(50)             NOT NULL SORTKEY,\n",
    "                name        VARCHAR(500)           NULL,\n",
    "                location    VARCHAR(500)           NULL,\n",
    "                latitude    DECIMAL(9)              NULL,\n",
    "                longitude   DECIMAL(9)              NULL\n",
    "    ) diststyle all;\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "metadata": {},
   "outputs": [],
   "source": [
    "time_table_create = (\"\"\"CREATE TABLE IF NOT EXISTS time (\n",
    "                start_time  TIMESTAMP               NOT NULL SORTKEY,\n",
    "                hour        SMALLINT                NULL,\n",
    "                day         SMALLINT                NULL,\n",
    "                week        SMALLINT                NULL,\n",
    "                month       SMALLINT                NULL,\n",
    "                year        SMALLINT                NULL,\n",
    "                weekday     SMALLINT                NULL\n",
    "    ) diststyle all;\"\"\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "STAGING TABLES"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "metadata": {},
   "outputs": [],
   "source": [
    "staging_events_copy = (\"\"\"COPY stagingevents FROM {}\n",
    "    credentials 'aws_iam_role={}'\n",
    "    format as json {}\n",
    "    STATUPDATE ON\n",
    "    region 'us-west-1';\n",
    "\"\"\").format(LOG_DATA, ARN, LOG_JSONPATH)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "metadata": {},
   "outputs": [],
   "source": [
    "staging_songs_copy = (\"\"\"\n",
    "\n",
    "    copy staging_songs \n",
    "    from {}\n",
    "    region 'us-west-1'\n",
    "    iam_role '{}'\n",
    "    compupdate off statupdate off\n",
    "    format as json 'auto'\n",
    "\n",
    "\"\"\").format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Final Table"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "metadata": {},
   "outputs": [],
   "source": [
    "songplay_table_insert = (\"\"\"INSERT INTO songplays (start_time,\n",
    "                                        user_id,\n",
    "                                        level,\n",
    "                                        song_id,\n",
    "                                        artist_id,\n",
    "                                        session_id,\n",
    "                                        location,\n",
    "                                        user_agent) SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \\\n",
    "                * INTERVAL '1 second'   AS start_time,\n",
    "            se.userId                   AS user_id,\n",
    "            se.level                    AS level,\n",
    "            ss.song_id                  AS song_id,\n",
    "            ss.artist_id                AS artist_id,\n",
    "            se.sessionId                AS session_id,\n",
    "            se.location                 AS location,\n",
    "            se.userAgent                AS user_agent\n",
    "    FROM stagingevents AS se\n",
    "    JOIN stagingsongs AS ss\n",
    "        ON (se.artist = ss.artist_name)\n",
    "    WHERE se.page = 'NextSong';\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "metadata": {},
   "outputs": [],
   "source": [
    "user_table_insert = (\"\"\"INSERT INTO users (user_id,\n",
    "                                        first_name,\n",
    "                                        last_name,\n",
    "                                        gender,\n",
    "                                        level)\n",
    "    SELECT  DISTINCT se.userId          AS user_id,\n",
    "            se.firstName                AS first_name,\n",
    "            se.lastName                 AS last_name,\n",
    "            se.gender                   AS gender,\n",
    "            se.level                    AS level\n",
    "    FROM stagingevents AS se\n",
    "    WHERE se.page = 'NextSong';\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "metadata": {},
   "outputs": [],
   "source": [
    "song_table_insert = (\"\"\"INSERT INTO songs(song_id,\n",
    "                                        title,\n",
    "                                        artist_id,\n",
    "                                        year,\n",
    "                                        duration)\n",
    "    SELECT  DISTINCT ss.song_id         AS song_id,\n",
    "            ss.title                    AS title,\n",
    "            ss.artist_id                AS artist_id,\n",
    "            ss.year                     AS year,\n",
    "            ss.duration                 AS duration\n",
    "    FROM stagingsongs AS ss;\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "metadata": {},
   "outputs": [],
   "source": [
    "artist_table_insert = (\"\"\"INSERT INTO artists (artist_id,\n",
    "                                        name,\n",
    "                                        location,\n",
    "                                        latitude,\n",
    "                                        longitude)\n",
    "    SELECT  DISTINCT ss.artist_id       AS artist_id,\n",
    "            ss.artist_name              AS name,\n",
    "            ss.artist_location          AS location,\n",
    "            ss.artist_latitude          AS latitude,\n",
    "            ss.artist_longitude         AS longitude\n",
    "    FROM stagingsongs AS ss;\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "metadata": {},
   "outputs": [],
   "source": [
    "time_table_insert = (\"\"\"INSERT INTO time (start_time,\n",
    "                                        hour,\n",
    "                                        day,\n",
    "                                        week,\n",
    "                                        month,\n",
    "                                        year,\n",
    "                                        weekday)\n",
    "    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \\\n",
    "                * INTERVAL '1 second'        AS start_time,\n",
    "            EXTRACT(hour FROM start_time)    AS hour,\n",
    "            EXTRACT(day FROM start_time)     AS day,\n",
    "            EXTRACT(week FROM start_time)    AS week,\n",
    "            EXTRACT(month FROM start_time)   AS month,\n",
    "            EXTRACT(year FROM start_time)    AS year,\n",
    "            EXTRACT(week FROM start_time)    AS weekday\n",
    "    FROM    stagingevents                   AS se\n",
    "    WHERE se.page = 'NextSong';\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "metadata": {},
   "outputs": [],
   "source": [
    "analytical_queries = [\n",
    "    'SELECT COUNT(*) AS total FROM artists',\n",
    "    'SELECT COUNT(*) AS total FROM songs',\n",
    "    'SELECT COUNT(*) AS total FROM time',\n",
    "    'SELECT COUNT(*) AS total FROM users',\n",
    "    'SELECT COUNT(*) AS total FROM songplays'\n",
    "]\n",
    "analytical_query_titles = [\n",
    "    'Artists table count',\n",
    "    'Songs table count',\n",
    "    'Time table count',\n",
    "    'Users table count',\n",
    "    'Song plays table count'\n",
    "]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "metadata": {},
   "outputs": [],
   "source": [
    "create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]\n",
    "drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]\n",
    "copy_table_queries = [staging_events_copy, staging_songs_copy]\n",
    "insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
