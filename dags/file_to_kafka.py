"""Watch folders for new gzipped incremental files and produce each line as a JSON record to Kafka."""
import glob
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sdk import get_current_context
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

from incrementals import iter_records

log = logging.getLogger(__name__)

KAFKA_CONN_ID = os.getenv("KAFKA_CONN_ID", "kafka_default")
COLUMNS = os.getenv("INCREMENTAL_COLUMNS", "c1,c2,c3,c4,c5,c6").split(",")
KEY_COLUMNS = os.getenv("INCREMENTAL_KEY_COLUMNS", "c1,c2").split(",")
INPUT_ROOT = os.getenv("INPUT_ROOT", "/opt/airflow/input")

SOURCES = [
    {"name": "cankey", "dir": os.path.join(INPUT_ROOT, "CANKEY Incrementals"),
     "topic": os.getenv("CANKEY_TOPIC", "cankey-incrementals")},
    {"name": "key", "dir": os.path.join(INPUT_ROOT, "KEY Incrmentals"),
     "topic": os.getenv("KEY_TOPIC", "key-incrementals")},
]


def _ledger_key(path):
    return f"f2k_processed:{os.path.basename(path)}"


@dag(
    dag_id="file_to_kafka",
    schedule=timedelta(minutes=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["kafka", "files"],
)
def file_to_kafka():
    @task
    def list_new_files():
        """Files not yet in the processed ledger. The input mount may be read-only, so we never move files."""
        pending = []
        for src in SOURCES:
            for path in sorted(glob.glob(os.path.join(src["dir"], "*.dat.gz"))):
                if Variable.get(_ledger_key(path), default_var=None) is None:
                    pending.append({"path": path, "topic": src["topic"], "source": src["name"]})
        # Optional throttle for manual runs: trigger with conf {"max_files": 1}
        conf = get_current_context()["dag_run"].conf or {}
        if conf.get("max_files"):
            pending = pending[: int(conf["max_files"])]
        log.info("%d new file(s)", len(pending))
        return pending

    @task
    def produce_file(item):
        """Produce every line of one file, wait for broker acks, then record the file as processed."""
        producer = KafkaProducerHook(kafka_config_id=KAFKA_CONN_ID).get_producer()
        errors = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(str(err))

        sent = 0
        for key, value, _ in iter_records(item["path"], COLUMNS, KEY_COLUMNS, item["source"]):
            while True:
                try:
                    producer.produce(item["topic"], key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(1)
            producer.poll(0)
            sent += 1
        remaining = producer.flush(60)
        if remaining or errors:
            raise RuntimeError(f"{item['path']}: {remaining} undelivered, errors={errors[:3]}")
        # At-least-once: a failure above means the file is retried and may re-send records.
        Variable.set(_ledger_key(item["path"]), datetime.utcnow().isoformat())
        log.info("Produced %d record(s) from %s to %s", sent, item["path"], item["topic"])
        return sent

    produce_file.expand(item=list_new_files())


file_to_kafka()
