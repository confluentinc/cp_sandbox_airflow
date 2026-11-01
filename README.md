# cp_sandbox_airflow

A working example of Apache Airflow and Confluent Platform together: an Airflow DAG that
picks up files as they arrive and streams their contents into Apache Kafka topics.

This repository is part of the Confluent organization on GitHub.
It is public and open to contributions from the community.

Please see the LICENSE file for contribution terms.
Please see the CHANGELOG.md for details of recent updates.

## Who this is for

Teams that already run Airflow, are adopting Confluent Platform, and want to see a concrete
pattern for feeding Kafka from the batch and file-based jobs they already orchestrate.
You do not need Kafka experience. Short explanations of the Kafka ideas used here are included.

## The pattern

```
 files land in a folder
         |
         v
 Airflow DAG (every minute)
   1. list_new_files   find files not yet processed
   2. produce_file     one task per file: read, convert each line to JSON, produce to Kafka
   3. ledger           record the file as processed (Airflow Variable)
         |
         v
 Confluent Platform: Kafka topics  ->  consumers, ksqlDB, Kafka Connect sinks, Control Center
```

Airflow stays in charge of scheduling, retries, dependencies and visibility. Confluent
Platform becomes the durable, replayable stream that downstream systems read from.
Once the data is in a topic, any number of consumers can use it independently.

## Kafka concepts used here

- **Topic**: a named, append-only log of records. This example writes to two topics.
- **Record**: a key and a value. Here the value is a JSON object for one input line.
- **Key**: records with the same key always go to the same partition, so they stay in order.
  The key here is built from columns `c1` and `c2`.
- **Partition**: a topic is split into partitions for parallelism. Replication copies each
  partition to several brokers for durability.
- **Acknowledgement**: the broker confirms a write. The DAG waits for all acknowledgements
  before it marks a file as processed.

## What is in the repository

| File | Purpose |
|---|---|
| `dags/file_to_kafka.py` | The DAG |
| `dags/incrementals.py` | Parsing helpers, no Airflow imports, easy to unit test |
| `Dockerfile` | Airflow 3.0.0 image plus `apache-airflow-providers-apache-kafka` |
| `docker-compose.yaml` | Single-container Airflow for a quick demo, UI on port 8080, mounts `./dags` and `./input` |
| `docker-compose.override.example.yml` | Optional template for mapping broker host names (copy to `docker-compose.override.yml`) |
| `.env.example` | Template for the gitignored `.env` |

## Prerequisites

- A Confluent Platform cluster reachable from where Airflow runs. You need its bootstrap
  server address, and credentials if the listeners are secured.
- Docker, for the demo setup below. If you already run Airflow, see
  [Using your existing Airflow](#using-your-existing-airflow).
- Permission to create two topics, or topics your admin has created for you.

## Quick start (demo Airflow in Docker)

1. **Create the topics** (the DAG does not create them). Example, run from any host with
   the Kafka CLI tools, or from a container:

   ```bash
   kafka-topics --bootstrap-server <broker:9092> --create \
     --topic cankey-incrementals --partitions 3 --replication-factor 3
   kafka-topics --bootstrap-server <broker:9092> --create \
     --topic key-incrementals --partitions 3 --replication-factor 3
   ```

   Use a replication factor your cluster supports.

2. **Configure the connection.** Copy `.env.example` to `.env` (it is gitignored) and set
   the bootstrap server. The connection is one line, so keep it on one line:

   ```
   AIRFLOW_CONN_KAFKA_DEFAULT='{"conn_type":"kafka","extra":{"bootstrap.servers":"<broker:9092>"}}'
   CANKEY_TOPIC=cankey-incrementals
   KEY_TOPIC=key-incrementals
   ```

3. **Add input files.** Put gzipped, tab-delimited `.dat.gz` files in `input/CANKEY Incrementals`
   and `input/KEY Incrmentals`. The input folder is not committed.

4. **Start Airflow and enable the DAG:**

   ```bash
   docker compose up -d --build
   docker compose exec airflow airflow dags unpause file_to_kafka
   ```

   The UI is at http://localhost:8080. The standalone admin password is printed by
   `docker compose logs airflow`.

## Using your existing Airflow

You do not need the demo container. Add the pattern to the Airflow you already run:

1. Install the provider in your Airflow image or environment:
   `pip install apache-airflow-providers-apache-kafka` (it brings `confluent-kafka`).
2. Create an Airflow connection of type `kafka`, with your cluster settings in `extra`.
   You can use the `AIRFLOW_CONN_KAFKA_DEFAULT` environment variable as above, or the UI,
   or a secrets backend.
3. Copy `dags/file_to_kafka.py` and `dags/incrementals.py` into your DAGs folder and make
   the input folders visible to the workers (a volume mount or shared storage).
4. Set the topic and column settings below.

The DAG targets Airflow 3.x (it imports `get_current_context` from `airflow.sdk`).

### Connecting to a secured cluster

The `extra` field takes any librdkafka client setting. Examples:

```json
{"bootstrap.servers": "broker1:9093",
 "security.protocol": "SASL_SSL",
 "sasl.mechanism": "PLAIN",
 "sasl.username": "<user>",
 "sasl.password": "<password>",
 "ssl.ca.location": "/path/to/ca.pem"}
```

Other mechanisms such as SCRAM, Kerberos (GSSAPI), OAUTHBEARER and mutual TLS use the
same approach with the matching librdkafka settings. Keep secrets in a secrets backend
or a gitignored `.env`, never in the repository.

If your brokers use host names that only resolve on your own machine, containers will not
see them. Copy `docker-compose.override.example.yml` to `docker-compose.override.yml`
(gitignored) and map each advertised broker name to its IP under `extra_hosts`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `AIRFLOW_CONN_KAFKA_DEFAULT` | none | Kafka connection (JSON, one line) |
| `CANKEY_TOPIC` | `cankey-incrementals` | Topic for the first folder |
| `KEY_TOPIC` | `key-incrementals` | Topic for the second folder |
| `INCREMENTAL_COLUMNS` | `c1,c2,c3,c4,c5,c6` | Names for the six tab-separated columns |
| `INCREMENTAL_KEY_COLUMNS` | `c1,c2` | Columns joined with `\|` to form the record key |
| `INPUT_ROOT` | `/opt/airflow/input` | Parent folder of the two input folders |
| `KAFKA_CONN_ID` | `kafka_default` | Airflow connection id to use |

The folder-to-topic mapping lives in `SOURCES` in `dags/file_to_kafka.py`. Add an entry to
watch another folder.

## How the DAG behaves

- Runs every minute with one active run at a time.
- `list_new_files` returns files that are not in the ledger. Input folders can be
  read-only; files are never moved or modified.
- `produce_file` runs once per file using dynamic task mapping, so files are processed in
  parallel and a failure affects only that file. It retries twice, 30 seconds apart.
- On success the file is recorded as the Airflow Variable `f2k_processed:<filename>`.
- Lines with the wrong number of fields are logged and skipped.
- For a manual run, trigger with the run config `{"max_files": 1}` to cap how many
  files it picks up.

Record shape (values stay strings, so leading zeros are preserved):

```json
{"c1":"002","c2":"HO1304110","c3":"0264","c4":"22","c5":"13","c6":"27.30",
 "source":"cankey","source_file":"CAinvinc102826095008.dat.gz","line_no":3}
```

## Viewing the records

- **Control Center**: open Topics, pick a topic, then the Messages tab.
- **Console consumer**:

  ```bash
  kafka-console-consumer --bootstrap-server <broker:9092> \
    --topic cankey-incrementals --from-beginning --max-messages 5 \
    --formatter-property print.key=true
  ```

- **Record counts**: `kafka-get-offsets --bootstrap-server <broker:9092> --topic <topic>`
  prints `topic:partition:offset`; the offsets add up to the record count.

With the included sample set, the topics hold 26 and 919 records, matching the line
counts of the input files.

## Things to know before production

- **Delivery is at-least-once.** If a run fails after some records were sent, the retry
  sends the file again, so consumers should tolerate duplicates. Clearing a ledger
  Variable also re-sends that file. For stronger guarantees, consider the producer's
  idempotent mode or a transactional producer, and idempotent consumers.
- **The ledger is an Airflow Variable.** That is simple for a demo. At scale, use a table
  or object-store marker.
- **Schemas.** This example sends plain JSON. With Schema Registry you can enforce a schema
  and compatibility rules (Avro, Protobuf or JSON Schema) so consumers get a contract.
- **Topic settings.** Choose partitions, replication factor, `min.insync.replicas` and
  retention for your workload rather than copying the demo values.
- **Testing.** On Airflow 3.0.0, `airflow dags test` fails on the mapped task with
  `unexpected expand_input`. Use real runs instead.

## When to use something other than Airflow

Airflow is poll and batch oriented, which fits scheduled file drops, dependencies between
jobs and retries you can see. For continuous, low-latency file ingestion, or when you want
no custom code, Kafka Connect connectors (such as SpoolDir or FilePulse) are a better fit.
Many teams use both: Airflow to orchestrate and Connect to stream.

## Stopping

```bash
docker compose down        # keep the Airflow metadata
docker compose down -v     # also delete it, which clears the ledger
```
