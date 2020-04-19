from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor


def store_url(c, url):
    if not url:
        return None

    sql = """
    INSERT INTO urls (url)
    VALUES (%s)
    ON CONFLICT ON CONSTRAINT urls_url_key DO NOTHING
    RETURNING url_id
    """
    c.execute(sql, (url,))
    row = c.fetchone()

    if not row:
        sql = """
        SELECT url_id FROM urls
        WHERE url = %s
        """
        c.execute(sql, (url,))
        row = c.fetchone()

    return row['url_id']


def store_search(c, pattern, found):
    if not pattern:
        return None

    sql = """
    INSERT INTO searches (pattern, found)
    VALUES (%s, %s)
    ON CONFLICT ON CONSTRAINT searches_pattern_found_key DO NOTHING
    RETURNING search_id
    """
    c.execute(sql, (pattern, found))
    row = c.fetchone()

    if not row:
        sql = """
        SELECT search_id FROM searches
        WHERE pattern = %s
        AND found = %s
        """
        c.execute(sql, (pattern, found))
        row = c.fetchone()

    return row['search_id']


def store_response(c, timestamp, url_id, search_id, time, code):
    sql = """
    INSERT INTO responses (ts, url_id, search_id, time, code)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT responses_pkey DO NOTHING
    RETURNING 'no_conflict'
    """
    c.execute(sql, (timestamp, url_id, search_id, time, code))

    return bool(c.fetchone())


def set_in_transaction_timeout(c):
    sql = "SET SESSION idle_in_transaction_session_timeout = '1min'"
    c.execute(sql)


def store_message(connection, message):
    c = connection.cursor(cursor_factory=RealDictCursor)
    set_in_transaction_timeout(c)

    url = message.value['url']
    url_id = store_url(c, url)

    r = message.value.get('response', {})
    s = r.get('search', {})
    pattern = s.get('pattern')
    found = s.get('found')
    search_id = store_search(c, pattern, found)

    unix_ts = message.timestamp / 1000.
    timestamp = datetime.fromtimestamp(unix_ts, timezone.utc)
    time = r.get('time')
    code = r.get('code')

    stored = store_response(c, timestamp, url_id, search_id, time, code)
    connection.commit()

    return stored
