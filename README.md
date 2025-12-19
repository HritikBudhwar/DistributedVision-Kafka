# Distributed Kafka Image Processing Pipeline

A distributed Kafka-based image processing pipeline. Client, Master, and Worker components stream, process, edit, and return image tiles via Kafka topics with parallel and fault-tolerant execution.

## Project layout

- Broker/broker.txt — helper commands for starting Kafka and creating topics
- producer/app.py — Flask-based client UI that uploads images and shows progress
- Controller.py/master.py — Master: splits images, sends tiles to `tasks` topic, collects results
- consumer/consumer1.py — Worker 1 (color inversion)
- consumer/consumer2.py — Worker 2 (grayscale)

## High-level flow

1. Client uploads an image to the Flask UI (`producer/app.py`).
2. The Master (`master.py`) splits the image into tiles and produces messages to the `tasks` topic.
3. Workers (consumers) subscribe to `tasks`, process tiles, and produce processed tiles to the `results` topic.
4. Master consumes `results`, merges tiles, saves output image to `static/`, and logs job metadata.

## Prerequisites

- Linux / macOS / Windows
- Python 3.8+
- A running Kafka cluster with Zookeeper (or a Kafka version that doesn't require Zookeeper). The project uses three topics: `tasks`, `results`, and `heartbeats` (see `Broker/broker.txt`).

## Install Python deps

Create a virtual environment and install requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configure broker address

Edit the broker address in `Controller.py/master.py` (`BROKER`) and in `consumer/*.py` (`BOOTSTRAP_SERVER`) to point to your Kafka bootstrap server (e.g. `localhost:9092` or `10.244.x.x:9092`).

Note: `producer/app.py` imports `process_image_pipeline` as `from master import process_image_pipeline`. If you keep `master.py` inside `Controller.py`, run the Flask app with `PYTHONPATH` set so Python can import it, for example:

```bash
export PYTHONPATH=$PWD/Controller.py
python3 producer/app.py
```

Or move `master.py` to the project root alongside `producer/app.py`.

## Create Kafka topics (example)

See `Broker/broker.txt` for exact commands. Example (adjust `--bootstrap-server`):

```bash
# create tasks
bin/kafka-topics.sh --create --topic tasks --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
# create results
bin/kafka-topics.sh --create --topic results --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
# create heartbeats
bin/kafka-topics.sh --create --topic heartbeats --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Run order

1. Start Zookeeper (if needed) and Kafka broker(s).
2. Create the Kafka topics (`tasks`, `results`, `heartbeats`).
3. Start the worker processes:

```bash
python3 consumer/consumer1.py &
python3 consumer/consumer2.py &
```

4. Start the Flask client (and master is invoked from there):

```bash
# if master.py is in Controller.py folder
export PYTHONPATH=$PWD/Controller.py
python3 producer/app.py

# or if master.py is moved to root
python3 producer/app.py
```

5. Upload an image via the web UI (http://0.0.0.0:5000). The UI shows send/receive progress and worker heartbeats.

## Notes & Troubleshooting

- Make sure `confluent-kafka` C library dependencies are available on your system if using `confluent-kafka` (install librdkafka via your package manager or use the `confluent-kafka` install docs).
- If you see import errors for `cv2`, install `opencv-python` and ensure a compatible version of `numpy` is installed.
- The code uses `sqlite3` (stdlib) to log jobs to `new.db`.

## Recommended next steps

- Optionally parameterize broker addresses via environment variables.
- Add a small supervisor/service file to run workers and the Flask app in production.

---
Generated on 2025-12-19 from repository files.
