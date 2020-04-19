from psycopg2.extras import RealDictCursor

ddl = ["""
    CREATE TABLE IF NOT EXISTS urls (
        url_id SERIAL PRIMARY KEY,
        url VARCHAR(8000) UNIQUE NOT NULL
    )
    """, """
    CREATE TABLE IF NOT EXISTS searches (
        search_id SERIAL PRIMARY KEY,
        pattern VARCHAR(1024) NOT NULL,
        found BOOLEAN,
        UNIQUE (pattern, found)
    )
    """, """
    CREATE TABLE IF NOT EXISTS responses (
        ts TIMESTAMP NOT NULL,
        url_id INTEGER NOT NULL,
        search_id INTEGER,
        time INTEGER,
        code SMALLINT,
        PRIMARY KEY (ts, url_id),
        FOREIGN KEY (url_id)
            REFERENCES urls (url_id)
            ON DELETE CASCADE,
        FOREIGN KEY (search_id)
            REFERENCES searches (search_id)
            ON DELETE SET NULL
    )
    """, """
    CREATE OR REPLACE FUNCTION responses_delete_unreferenced()
        RETURNS trigger AS
    $$
    BEGIN
        DELETE FROM searches
        WHERE NOT EXISTS (
            SELECT FROM responses
            WHERE responses.search_id = searches.search_id
        );
        DELETE FROM urls
        WHERE NOT EXISTS (
            SELECT FROM responses
            WHERE responses.url_id = urls.url_id
        );
        RETURN NULL;
    END;
    $$
    LANGUAGE 'plpgsql';
    """, """
    DROP TRIGGER IF EXISTS response_deleted ON responses;
    CREATE TRIGGER response_deleted
    AFTER DELETE
    ON responses
    FOR EACH ROW
    EXECUTE PROCEDURE responses_delete_unreferenced();
    """]


def responses_exists(cursor, schema):
    responses = '{}.responses'.format(schema)
    sql = "SELECT to_regclass(%s) as table"
    cursor.execute(sql, (responses,))
    row = cursor.fetchone()

    return bool(row.get('table'))


def run_ddl(connection, schema='public'):
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    if responses_exists(cursor, schema):
        return

    for command in ddl:
        cursor.execute(command)

    connection.commit()
