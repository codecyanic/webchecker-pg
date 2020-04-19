# WebChecker-pg

WebChecker-pg is a service for storing WebChecker output to a PostgreSQL database.

## Setup

Create a configuration file named `config.yaml` by customizing `example.config.yaml`.

Install package requirements by running:
```
pip install -r requirements.txt
```

Installing is currently not supported.

## Running

Run from the project directory by using the command:
```
./main.py
```

## Testing

For unit testing run:
```
python3 -m pytest
```

## Usage

Upon running, the WebChecker-pg will initialize the database.
Therefore there is no manual setup required.

The service is meant to be resilient against data loss.
When run, it will start consuming earliest data from Kafka.
This means that it can take a long time to reach the latest data.

By default, WebChecker-pg runs one separate worker thread for database
communication.
The number of threads can be increased for faster storage.

WebChecker-pg can store WebChecker output from multiple Kafka topics.
It is recommended to use a single WebChecker-pg instance per database and set
the appropriate number of threads.
However, running multiple instances is supported, even when they are consuming
the same data.

### Using database data

Data is stored across multiple tables:

- `urls` table contains `url` strings (truncated to 8000 Unicode characters)
- `searches` contains a search `pattern` (1024 Unicode characters) and a
  boolean value indicating if the pattern was `found` (in case of errors it
  will be set to `NULL`)
- `responses` contains HTTP response `time` and `code` (both `NULL` on errors),
  UTC time stamp (`ts`) and references to the other tables


Modification of table rows is not recommended.
Deleting entries from `searches` will also remove references from `responses`.
If entries from `responses` are deleted, coresponding entries for `searches`
and `urls` will also be deleted if no other entry is referencing them.
When a row in `urls` is deleted, all the `responses` entries associated with
them will also get deleted.

Example query for listing all check results for a particular URL:
```
SELECT *
FROM urls
LEFT JOIN responses ON
    responses.url_id = urls.url_id
LEFT JOIN searches ON
    responses.search_id = searches.search_id
WHERE url = 'http://example.com';
```
