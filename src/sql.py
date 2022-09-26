import sqlite3
from functools import lru_cache

from retry import retry

AUTHOR_TABLE_NAME = "AUTHOR"
TOPIC_TABLE_NAME = "TOPIC"
TOPIC_URL_TABLE_NAME = "TOPIC_URL"
POST_TABLE_NAME = "POST"


@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def checkTableExists(db_connection, table_name):
    cursor = db_connection.cursor()

    @lru_cache(maxsize=None)
    def _run(table_name):
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name=(?)",
            (table_name,),
        )
        return bool(cursor.fetchone())

    return _run(table_name)


@lru_cache(maxsize=None)
@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def set_up(con):

    cur = con.cursor()
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("""PRAGMA synchronous = OFF""")
    # con.execute("""PRAGMA journal_mode = OFF""")
    cur = con.cursor()
    if not checkTableExists(con, AUTHOR_TABLE_NAME):

        # create table AUTHOR
        cur.execute(
            f"CREATE TABLE {AUTHOR_TABLE_NAME} ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
            "code TEXT VARCHAR2(10) UNIQUE NOT NULL)"
        )
    if not checkTableExists(con, TOPIC_TABLE_NAME):

        # create table TOPIC - need to grad first author of topic for author topic - post1
        cur.execute(
            f"CREATE TABLE {TOPIC_TABLE_NAME} ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
            "title TEXT VARCHAR2(500) NOT NULL,"
            "author_id INTEGER NOT NULL,"
            f"FOREIGN KEY (author_id) REFERENCES {AUTHOR_TABLE_NAME} (id),"
            "CONSTRAINT title_author UNIQUE (title, author_id))"
        )
    if not checkTableExists(con, TOPIC_URL_TABLE_NAME):

        # create table TOPIC_URL
        cur.execute(
            f"CREATE TABLE {TOPIC_URL_TABLE_NAME} ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
            "link TEXT VARCHAR2(500) UNIQUE NOT NULL,"
            "author_id INTEGER NOT NULL, "
            "topic_id INTEGER NOT NULL, "
            f"FOREIGN KEY (author_id) REFERENCES {AUTHOR_TABLE_NAME} (id),"
            f"FOREIGN KEY(topic_id) REFERENCES {TOPIC_TABLE_NAME} (id))"
        )
    if not checkTableExists(con, POST_TABLE_NAME):

        # create table POST
        cur.execute(
            f"CREATE TABLE {POST_TABLE_NAME} ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
            "content TEXT VARCHAR2(5000) NOT NULL,"
            "author_id INTEGER NOT NULL,"
            "topic_id INTEGER NOT NULL,"
            "topic_url_id INTEGER NOT NULL,"
            "created_at timestamp NOT NULL,"
            "toxicity DOUBLE NOT NULL,"
            "severe_toxicity DOUBLE NOT NULL,"
            "obscene DOUBLE NOT NULL,"
            "identity_attack DOUBLE NOT NULL,"
            "insult DOUBLE NOT NULL,"
            "threat DOUBLE NOT NULL,"
            "sexual_explicit DOUBLE NOT NULL,"
            f"FOREIGN KEY (author_id) REFERENCES {AUTHOR_TABLE_NAME} (id),"
            f"FOREIGN KEY (topic_id) REFERENCES {TOPIC_TABLE_NAME} (id),"
            f"FOREIGN KEY (topic_url_id) REFERENCES {TOPIC_URL_TABLE_NAME} (id))"
        )


@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def create_author(conn, code):
    """
    Create a author info into the author table
    :param conn:
    :param author:
    :return: author id
    """
    set_up(conn)

    @lru_cache(maxsize=None)
    def _run(code):
        sql = f""" INSERT OR IGNORE INTO {AUTHOR_TABLE_NAME}(code)
                  VALUES(?) """
        cur = conn.cursor()
        cur.execute(sql, (code,))
        conn.commit()
        # to_return = cur.lastrowid
        # if not to_return:
        sql = f"SELECT id FROM {AUTHOR_TABLE_NAME} WHERE code = (?)"

        cur.execute(sql, (code,))
        return cur.fetchone()[0]

    return _run(code)


@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def create_topic(conn, title, author_id):
    """
    Create topic table and insert title info, AUTHOR author_id
    :param conn:
    :param topic:
    :return: topic id
    """
    set_up(conn)

    @lru_cache(maxsize=None)
    def _run(title, author_id):
        cur = conn.cursor()

        sql = f""" INSERT OR IGNORE INTO {TOPIC_TABLE_NAME}(title, author_id)
                  VALUES(?, ?) """

        cur.execute(
            sql,
            (
                title,
                author_id,
            ),
        )
        conn.commit()
        # to_return = cur.lastrowid
        # if not to_return:

        sql = f"SELECT id FROM {TOPIC_TABLE_NAME} WHERE title = (?) and author_id = (?)"
        cur.execute(sql, (title, author_id))
        return cur.fetchone()[0]

    return _run(title, author_id)


@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def create_topic_url(conn, link, author_id, topic_id):
    """
    Create topic_url table and insert each topic_url link, author_id, topic_id
    :param conn:
    :param topic_url:
    :return: topic_url_id
    """
    set_up(conn)

    @lru_cache(maxsize=None)
    def _run(link, author_id, topic_id):
        sql = """ INSERT OR IGNORE INTO topic_url(link, author_id, topic_id)
                  VALUES(?, ?, ?) """
        cur = conn.cursor()
        cur.execute(
            sql,
            (
                link,
                author_id,
                topic_id,
            ),
        )
        conn.commit()
        # to_return = cur.lastrowid
        # if not to_return:
        sql = (
            f"SELECT id FROM {TOPIC_URL_TABLE_NAME} WHERE link = (?) and author_id ="
            " (?) and topic_id = (?)"
        )

        cur.execute(sql, (link, author_id, topic_id))
        return cur.fetchone()[0]

    return _run(link, author_id, topic_id)

@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def count_posts(conn):
    cur = conn.cursor()
    return cur.execute(f"SELECT COUNT() FROM {POST_TABLE_NAME}").fetchone()[0]


@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def create_post(conn, content, author_id, topic_id, topic_url_id, created_at, toxicity, severe_toxicity, obscene, identity_attack, insult, threat, sexual_explicit,):
    """
    Create a post table and insert content, author id, topic id, topic_url id, created_at, toxicity, severe_toxicity, obscene, identity_attack, insult, threat, sexual_explicit
    :param conn:
    :param post:
    :return: post id
    """

    set_up(conn)

    @lru_cache(maxsize=None)
    def _run(content, author_id, topic_id, topic_url_id, created_at, toxicity, severe_toxicity, obscene, identity_attack, insult, threat, sexual_explicit):
        sql = """ INSERT INTO post(content, author_id, topic_id, topic_url_id, created_at, toxicity, severe_toxicity, obscene, identity_attack, insult, threat, sexual_explicit)
                  VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
        cur = conn.cursor()
        cur.execute(
            sql,
            (
                content,
                author_id,
                topic_id,
                topic_url_id,
                created_at,
                toxicity,
                severe_toxicity, 
                obscene, 
                identity_attack, 
                insult, 
                threat, 
                sexual_explicit,
            ),
        )
        conn.commit()
        # to_return = cur.lastrowid
        # if not to_return:
        sql = (
            f"SELECT id FROM {POST_TABLE_NAME} WHERE content = (?) and author_id = (?)"
            " and topic_id = (?) and topic_url_id = (?)"
        )

        cur.execute(
            sql,
            (
                content,
                author_id,
                topic_id,
                topic_url_id,
            ),
        )
        return cur.fetchone()[0]

    return _run(content, author_id, topic_id, topic_url_id, created_at, toxicity, severe_toxicity, obscene, identity_attack, insult, threat, sexual_explicit)

@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def get_posts(conn):
    sql = (
        """ SELECT content, author_id, topic_id, topic_url_id, created_at FROM post """
    )

    cur = conn.cursor()
    cur.execute(
        sql,
    )
    for row in cur.fetchall():
        post_content = row[0]
        post_author_id = row[1]
        post_topic_id = row[2]
        post_topic_url_id = row[3]
        post_created_at = row[4]

        # topic_url
        sql = (
            f"SELECT link, author_id, topic_id FROM {TOPIC_URL_TABLE_NAME} WHERE id"
            " = (?)"
        )
        cur.execute(sql, (post_topic_url_id,))
        topic_url_info = cur.fetchone()
        topic_url_link = topic_url_info[0]
        topic_author_id = topic_url_info[1]
        topic_id = topic_url_info[2]

        # topic_author
        sql = f"SELECT code FROM {AUTHOR_TABLE_NAME} WHERE id = (?)"
        cur.execute(sql, (topic_author_id,))
        topic_author_info = cur.fetchone()
        topic_author_code = topic_author_info[0]

        # post_author
        sql = f"SELECT code FROM {AUTHOR_TABLE_NAME} WHERE id = (?)"
        cur.execute(sql, (post_author_id,))
        post_author_info = cur.fetchone()
        post_author_code = post_author_info[0]

        # topic_title
        sql = f"SELECT title FROM {TOPIC_TABLE_NAME} WHERE id = (?)"
        cur.execute(sql, (topic_id,))
        topic_info = cur.fetchone()
        topic_title = topic_info[0]

        to_return = {
            "post_content": post_content,
            "created_at": post_created_at,
            "topic_url_link": topic_url_link,
            "topic_title": topic_title,
            "topic_author_code": topic_author_code,
            "post_author_code": post_author_code,
        }
        yield to_return

@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def contains_url(con, url):
    cur = con.cursor()
    sql = f"SELECT count() FROM {TOPIC_URL_TABLE_NAME} WHERE link = (?)"

    return bool(cur.execute(sql, (url,)).fetchone()[0])

def add_content(con,content):
    topic_author_id = create_author(con,content["topic_author_code"])
    post_author_id = create_author(con,content["post_author_code"])    
    topic_id = create_topic(con,content["topic_title"], topic_author_id)
    topic_url_id = create_topic_url(con,content["topic_url_link"], topic_author_id, topic_id)
    post_id = create_post(con,content["post_content"], post_author_id, topic_id, topic_url_id, content["created_at"])

@retry(tries=21, delay=0.1, backoff=1.2, max_delay=4, logger=None)
def get_post(con, post_id):
    cur = con.cursor()
    sql = (
        f"SELECT * FROM {POST_TABLE_NAME} WHERE id = (?)"
    )

    cur.execute(
        sql, (post_id),
    )
    return cur.fetchone() 
