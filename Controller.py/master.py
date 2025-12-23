import cv2
import numpy as np
from confluent_kafka import Producer, Consumer
from PIL import Image
import io, time, os, json, sqlite3

# --- CONFIG ---
BROKER = ':9092' #replace with ip of own
TASKS_TOPIC = 'tasks'
RESULTS_TOPIC = 'results'
HEARTBEATS_TOPIC = 'heartbeats'
DB_PATH = "new.db"

# === Kafka setup ===
producer = Producer({'bootstrap.servers': BROKER})
consumer = Consumer({
    'bootstrap.servers': BROKER,
    'group.id': 'master-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([RESULTS_TOPIC])


# === SQLite setup ===
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS job_lab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_name TEXT,
            start_time REAL,
            end_time REAL,
            total_tiles INTEGER,
            worker_data TEXT
        )
    """)
    conn.commit()
    conn.close()


def log_job(image_name, start_time, end_time, total_tiles, worker_data):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO job_lab (image_name, start_time, end_time, total_tiles, worker_data)
        VALUES (?, ?, ?, ?, ?)
    """, (image_name, start_time, end_time, total_tiles, json.dumps(worker_data)))
    conn.commit()
    conn.close()


# === Image split/merge ===
def split_image(img, tile_size=512):
    w, h = img.size
    tiles = []
    for y in range(0, h, tile_size):
        for x in range(0, w, tile_size):
            box = (x, y, min(x + tile_size, w), min(y + tile_size, h))
            tiles.append((x, y, img.crop(box)))
    print(f"Split {w}x{h} → {len(tiles)} tiles")
    return tiles


def merge_tiles(tiles, size):
    w, h = size
    final = Image.new('RGB', (w, h))
    for (x, y, t) in tiles:
        final.paste(t, (x, y))
    return final


# === Main processing pipeline ===
def process_image_pipeline(image_file, progress_callback=None):
    init_db()

    img = Image.open(image_file).convert('RGB')
    image_name = os.path.basename(image_file)

    tiles = split_image(img)
    total = len(tiles)
    sent = recv = 0
    worker_stats = {}

    start_time = time.time()

    if progress_callback:
        progress_callback(sent, recv, total)

    # Send tiles
    for (x, y, tile) in tiles:
        buf = io.BytesIO()
        tile.save(buf, format='JPEG')
        producer.produce(TASKS_TOPIC, value=buf.getvalue(), headers={'x': str(x), 'y': str(y)})
        sent += 1
        if progress_callback:
            progress_callback(sent, recv, total)
    producer.flush()

    processed = {}

    # Receive processed tiles
    while len(processed) < total:
        msg = consumer.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue

        headers = dict(msg.headers() or [])
        x = int(headers.get('x', 0))
        y = int(headers.get('y', 0))
        worker_id = headers.get('worker_id', 'unknown')

        arr = np.frombuffer(msg.value(), np.uint8)
        tile_img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if tile_img is None:
            continue

        tile_pil = Image.fromarray(cv2.cvtColor(tile_img, cv2.COLOR_BGR2RGB))
        processed[(x, y)] = tile_pil
        recv = len(processed)

        worker_stats[worker_id] = worker_stats.get(worker_id, 0) + 1

        if progress_callback:
            progress_callback(sent, recv, total)

    # Save output image
    os.makedirs("static", exist_ok=True)
    timestamp = int(time.time() * 1000)
    output_filename = f"processed_{timestamp}.jpg"
    output_path = os.path.join("static", output_filename)
    merge_tiles([(x, y, t) for (x, y), t in processed.items()], img.size).save(output_path)

    end_time = time.time()
    log_job(image_name, start_time, end_time, total, worker_stats)

    print(f"[MASTER] ✅ Output: {output_path}")
    return output_filename  # ✅ fixed: return filename only

