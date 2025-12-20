from flask import Flask, render_template_string, request, jsonify, url_for
from master import process_image_pipeline
import threading, os, time
from confluent_kafka import Consumer
import json

app = Flask(__name__)

# === GLOBAL STATE ===
job_status = {
    "in_progress": False,
    "completed": False,
    "sending_done": 0,
    "receiving_done": 0,
    "total_tiles": 0,
    "start_time": None,
    "end_time": None,
    "output_image": None
}

worker_status = {}

# === UPDATED HTML (NEW PROGRESS BAR STYLE) ===
HTML = """
<!doctype html>
<html>
<head>
  <title>Distributed Image Processor</title>
  <meta http-equiv="refresh" content="5">
  <style>
    body {
      font-family: 'Segoe UI', Arial, sans-serif;
      padding: 30px;
      background: #f8f9fa;
      color: #333;
    }

    h1 { color: #222; margin-bottom: 10px; }
    h3 { margin-bottom: 15px; }

    table {
      border-collapse: collapse;
      margin-top: 10px;
      width: 80%%;
      box-shadow: 0 0 5px rgba(0,0,0,0.05);
    }
    th, td {
      border: 1px solid #ddd;
      padding: 10px;
      text-align: center;
    }
    th {
      background-color: #f1f1f1;
    }

    .progress {
      margin: 25px 0;
      padding: 20px;
      background: #fff;
      width: 80%%;
      border-radius: 10px;
      box-shadow: 0 0 6px rgba(0,0,0,0.08);
    }

    .progress-container {
      position: relative;
      height: 28px;
      width: 100%%;
      background: #e9ecef;
      border-radius: 14px;
      overflow: hidden;
      margin-top: 8px;
      margin-bottom: 16px;
    }

    .bar-fill {
      height: 100%%;
      line-height: 28px;
      color: white;
      font-weight: 600;
      text-align: center;
      border-radius: 14px;
      transition: width 0.4s ease-in-out;
      font-size: 13px;
    }

    .send-bar {
      background: linear-gradient(90deg, #2196f3, #1976d2);
      box-shadow: 0 0 5px rgba(33,150,243,0.4);
    }

    .recv-bar {
      background: linear-gradient(90deg, #43a047, #2e7d32);
      box-shadow: 0 0 5px rgba(67,160,71,0.4);
    }

    .alive { color: #2e7d32; font-weight: bold; }
    .offline { color: #c62828; font-weight: bold; }

    .status-card {
      background: #fff;
      padding: 20px;
      border-radius: 10px;
      box-shadow: 0 0 6px rgba(0,0,0,0.08);
      width: 80%%;
      margin-top: 20px;
    }
  </style>
</head>
<body>
  <h1>Distributed Image Processor</h1>
  <form method="post" enctype="multipart/form-data">
    <input type="file" name="image" required>
    <input type="submit" value="Upload & Process">
  </form>

  <div class="progress">
    <h3>Job Progress</h3>

    {% if job['in_progress'] %}
      {% set send_pct = (job['sending_done'] / job['total_tiles'] * 100) if job['total_tiles'] else 0 %}
      {% set recv_pct = (job['receiving_done'] / job['total_tiles'] * 100) if job['total_tiles'] else 0 %}

      <div>
        <p><b>Sending Tasks:</b> {{ job['sending_done'] }}/{{ job['total_tiles'] }}</p>
        <div class="progress-container">
          <div class="bar-fill send-bar" style="width: {{ send_pct }}%%;">
            {{ send_pct|round(0) }}%%
          </div>
        </div>
      </div>

      <div>
        <p><b>Receiving Results:</b> {{ job['receiving_done'] }}/{{ job['total_tiles'] }}</p>
        <div class="progress-container">
          <div class="bar-fill recv-bar" style="width: {{ recv_pct }}%%;">
            {{ recv_pct|round(0) }}%%
          </div>
        </div>
      </div>

    {% elif job['completed'] %}
      <p><b>Job Completed!</b> Processed {{ job['total_tiles'] }} tiles in
      {{ (job['end_time'] - job['start_time'])|round(2) }}s</p>
      <img src="{{ url_for('static', filename=job['output_image']) }}" width="512" style="margin-top:15px; border-radius:10px; box-shadow:0 0 8px rgba(0,0,0,0.1);">

    {% else %}
      <p>No active job.</p>
    {% endif %}
  </div>

  <div class="status-card">
    <h3>Worker Status ({{ workers|length }})</h3>
    <table>
      <tr><th>Worker ID</th><th>Status</th><th>Tiles Processed</th><th>Last Heartbeat</th></tr>
      {% for w, info in workers.items() %}
        <tr>
          <td>{{ w }}</td>
          <td class="{{ 'alive' if info['status'] == 'alive' else 'offline' }}">{{ info['status'] }}</td>
          <td>{{ info['tasks_processed'] }}</td>
          <td>{{ info['last_heartbeat'] }}</td>
        </tr>
      {% endfor %}
    </table>
  </div>
</body>
</html>
"""

# === JOB WRAPPER ===
def tracked_process(image_path):
    job_status.update({
        "in_progress": True, "completed": False,
        "sending_done": 0, "receiving_done": 0,
        "total_tiles": 0, "start_time": time.time(),
        "end_time": None, "output_image": None
    })

    def progress_callback(sent, received, total):
        job_status["sending_done"] = sent
        job_status["receiving_done"] = received
        job_status["total_tiles"] = total

    try:
        output_filename = process_image_pipeline(image_path, progress_callback)
        job_status.update({
            "completed": True, "in_progress": False,
            "end_time": time.time(), "output_image": output_filename
        })
    except Exception as e:
        print(f"[ERROR] {e}")
        job_status["in_progress"] = False


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        img_file = request.files['image']
        os.makedirs("uploads", exist_ok=True)
        img_path = os.path.join("uploads", img_file.filename)
        img_file.save(img_path)
        threading.Thread(target=tracked_process, args=(img_path,), daemon=True).start()
    return render_template_string(HTML, job=job_status, workers=worker_status)


@app.route('/api/status')
def api_status():
    return jsonify({"job": job_status, "workers": worker_status})


def heartbeat_collector():
    from master import BROKER, HEARTBEATS_TOPIC
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'flask-heartbeat',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([HEARTBEATS_TOPIC])
    HEARTBEAT_TIMEOUT = 10
    while True:
        msg = consumer.poll(2.0)
        now = time.time()
        if msg:
            try:
                data = json.loads(msg.value().decode('utf-8'))
                wid = data.get("worker_id")
                ts = data.get("timestamp")
                worker_status[wid] = {
                    "status": "alive",
                    "tasks_processed": data.get("tiles_processed", 0),
                    "last_heartbeat": ts,
                    "last_update_time": now
                }
            except Exception as e:
                print(f"[WARNING] {e}")
        for wid, info in list(worker_status.items()):
            if now - info.get("last_update_time", 0) > HEARTBEAT_TIMEOUT:
                worker_status[wid]["status"] = "offline"


if __name__ == '__main__':
    os.makedirs('static', exist_ok=True)
    threading.Thread(target=heartbeat_collector, daemon=True).start()
    print("Flask application running at http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000)

