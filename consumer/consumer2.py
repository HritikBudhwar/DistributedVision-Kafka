import time, json, cv2, numpy as np, socket
from datetime import datetime
from confluent_kafka import Consumer, Producer

# ==== CONFIG ====
WORKER_ID = "gray_worker"
GROUP_ID = "img_pipeline_group"  
BOOTSTRAP_SERVER = "10.244.60.122:9092"

TASK_TOPIC = "tasks"
RESULT_TOPIC = "results"
HEARTBEAT_TOPIC = "heartbeats"

# ==== KAFKA ====
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVER})
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
})
consumer.subscribe([TASK_TOPIC])

# ==== HEARTBEAT ====
def send_heartbeat(tile_count):
    hb = {
        "worker_id": WORKER_ID,
        "zt_ip": socket.gethostbyname(socket.gethostname()),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "alive",
        "tiles_processed": tile_count
    }
    producer.produce(HEARTBEAT_TOPIC, json.dumps(hb).encode("utf-8"))
    producer.flush()

# ==== IMAGE PROCESSING (GRAYSCALE) ====
def process_image(raw_bytes):
    np_arr = np.frombuffer(raw_bytes, np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if img is None:
        return None

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray_bgr = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
    _, buffer = cv2.imencode(".jpg", gray_bgr)
    return buffer.tobytes()

print(f"{WORKER_ID} Online - waiting for tasks...")

tile_count = 0
last_hb = 0

while True:
    msg = consumer.poll(1)

    # Heartbeat every 5s
    if time.time() - last_hb > 5:
        send_heartbeat(tile_count)
        last_hb = time.time()

    if not msg:
        continue
    if msg.error():
        print(f"Kafka Error: {msg.error()}")
        continue

    start_time = time.time()
    tile_id = msg.key().decode() if msg.key() else f"tile_{int(time.time())}"
    raw = msg.value()

    x = y = None
    if msg.headers():
        for k, v in msg.headers():
            if k == "x": x = int(v.decode())
            elif k == "y": y = int(v.decode())

    print(f"\nTile: {tile_id} | x={x}, y={y}")

    out_bytes = process_image(raw)
    if out_bytes is None:
        print(f"Decode failed for {tile_id}")
        continue

    tile_count += 1
    processing_time = round(time.time() - start_time, 4)

    producer.produce(
        RESULT_TOPIC,
        key=tile_id.encode(),
        value=out_bytes,
        headers={
            "x": str(x),
            "y": str(y),
            "worker": WORKER_ID,
            "time": str(processing_time)
        }
    )
    producer.flush()

    print(f"Grayscale tile: {tile_id} | time={processing_time}s | total={tile_count}")

