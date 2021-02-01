# bq2psql-tool

## Description

bq2psql-tool is an agnostic CLI to expose commands which allows you to import data (and other related actions) between BigQuery
and Postgres.

Format:
```
bq2psql [command] [--flags]
```

### Tech Stack:
* [Golang](https://golang.org/doc/)
* [Cobra Framework](https://github.com/spf13/cobra#working-with-flags)
* [Viper](https://github.com/spf13/viper)
* [Docker](https://docs.docker.com/)

### Git
* Repository:
  https://github.com/GlobalFishingWatch/bq2psql-tool

## Usage

There are available the following commands:
* Import

---

### Command: [import]

The import command allows you to import data from BigQuery to Postgres.

#### Flags
##### Required flags
- `--project-id=` the project id where we want to run the query.
- `--query=` SQL query to get rows from BigQuery.
- `--table-name=` The destination name table.
- `--table-schema=` The destination table schema.
- `--postgres-address=` The database address and port.
- `--postgres-user=` The database user.
- `--postgres-password=` The database password.
- `--postgres-database=` The destination name database.

##### Optional flags
* `--view-name=` If you want to use a view, this is the destination view name

#### Example
Here an example of this command:
```
bq2psql import \
  --project-id=world-fishing-827 \
  --query="SELECT * FROM vessels" \
  --table-name="vessels_2021_02_01" \
  --table-schema="flag VARCHAR(3), first_transmission_date VARCHAR, last_transmission_date VARCHAR, id VARCHAR, mmsi VARCHAR, imo VARCHAR, callsign VARCHAR, shipname VARCHAR" \
  --postgres-address="localhost:5432" \
  --postgres-user="postgres" \
  --postgres-password="XaD2sd$34Sdas1$ae" \
  --postgres-database="postgres" \
  --view-name="vessels"
```

When you execute this command, under the hood happens the followings steps:
* The CLI check if the destination table exists (and creates it)
* The CLI executes the SQL query and gets the rows
* The CLI parses the results from RowIterator to JSON files. The keys are the name of each column.
* The CLI imports the parsed data to Postgres.