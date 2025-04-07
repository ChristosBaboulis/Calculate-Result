import os
import re
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from io import StringIO

app = func.FunctionApp()

# Azure Function trigger
@app.blob_trigger(arg_name="myblob", path="test-logs/{name}.log", connection="AzureWebJobsStorage")
def main(myblob: func.InputStream):
    logging.info(f"📥 Triggered by: {myblob.name} ({myblob.length} bytes)")
    # Azure Blob Storage setup
    CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
    CONTAINER_NAME = "test-logs"

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # Check how many .log files exist
    log_blobs = [b.name for b in container_client.list_blobs() if b.name.endswith(".log") and b.name.startswith("segment_")]
    if len(log_blobs) < 17:
        logging.info(f"✅ Found only {len(log_blobs)} log files — waiting for all 17.")
        return

    logging.info("✅ All 17 logs found. Starting aggregation.")

    # Matchers
    vehicles_pattern = re.compile(r"Total vehicles: Left = (\d+) \| Right = (\d+)")
    violations_pattern = re.compile(r"Total speed violations: (\d+)")
    speed_pattern = re.compile(r"Average speed: Left = ([\d.]+) km/h \| Right = ([\d.]+) km/h")

    NUM_SEGMENTS = 18
    logs_data = []

    for i in range(NUM_SEGMENTS):
        filename = f"segment_{i:03d}.mp4.log"
        try:
            blob_client = container_client.get_blob_client(blob=filename)
            content = blob_client.download_blob().readall().decode("utf-8")
            logs_data.append((i, content))
        except Exception as e:
            logging.warning(f"Log file not found: {filename}")

    logs_data.sort(key=lambda x: x[0])

    # Initialize totals
    total_left = total_right = total_violations = 0
    total_vehicles_per_5min = []
    total_speed_per_5min = []

    def parse_values(content):
        left = right = violations = 0
        avg_left = avg_right = 0.0

        v_match = vehicles_pattern.search(content)
        if v_match:
            left, right = int(v_match[1]), int(v_match[2])

        viol_match = violations_pattern.search(content)
        if viol_match:
            violations = int(viol_match[1])

        s_match = speed_pattern.search(content)
        if s_match:
            avg_left, avg_right = float(s_match[1]), float(s_match[2])

        return left, right, violations, avg_left, avg_right

    parsed_logs = {}
    for idx, content in logs_data:
        parsed_logs[idx] = parse_values(content)
        total_left += parsed_logs[idx][0]
        total_right += parsed_logs[idx][1]
        total_violations += parsed_logs[idx][2]

    intervals = [
        [0, 1, 0.5 * 2],
        [0.5 * 2, 3, 4],
        [5, 6, 0.5 * 7],
        [0.5 * 7, 8, 9],
        [10, 11, 0.5 * 12],
        [0.5 * 12, 13, 14],
        [15, 16],
        [17],
    ]

    for group in intervals:
        left_sum = right_sum = 0
        left_speed_total = right_speed_total = 0.0
        vehicle_count = 0

        for seg in group:
            if isinstance(seg, float):
                idx = int(seg)
                if idx in parsed_logs:
                    l, r, _, sl, sr = parsed_logs[idx]
                    left_sum += l / 2
                    right_sum += r / 2
                    left_speed_total += (sl * (l / 2))
                    right_speed_total += (sr * (r / 2))
                    vehicle_count += (l / 2 + r / 2)
            else:
                if seg in parsed_logs:
                    l, r, _, sl, sr = parsed_logs[seg]
                    left_sum += l
                    right_sum += r
                    left_speed_total += (sl * l)
                    right_speed_total += (sr * r)
                    vehicle_count += (l + r)

        total_vehicles_per_5min.append((left_sum, right_sum))
        if vehicle_count > 0:
            total_speed_per_5min.append((left_speed_total / (left_sum or 1), right_speed_total / (right_sum or 1)))
        else:
            total_speed_per_5min.append((0.0, 0.0))

    logging.info("=== Overall Summary ===")
    logging.info(f"Total Vehicles: Left = {int(total_left)}, Right = {int(total_right)}")
    logging.info(f"Total Speed Violations: {total_violations}")

    logging.info("=== Per 5-minute Summary ===")
    for i, (vl, vr) in enumerate(total_vehicles_per_5min, 1):
        sl, sr = total_speed_per_5min[i-1]
        logging.info(f"Interval {i}: Vehicles ➜ Left = {int(vl)}, Right = {int(vr)} | Avg Speed ➜ Left = {sl:.2f}, Right = {sr:.2f}")

    summary_output = StringIO()

    summary_output.write("=== Overall Summary ===\n")
    summary_output.write(f"Total Vehicles: Left = {int(total_left)}, Right = {int(total_right)}\n")
    summary_output.write(f"Total Speed Violations: {total_violations}\n\n")

    summary_output.write("=== Per 5-minute Summary ===\n")
    for i, (vl, vr) in enumerate(total_vehicles_per_5min, 1):
        sl, sr = total_speed_per_5min[i-1]
        summary_output.write(f"Interval {i}: Vehicles ➜ Left = {int(vl)}, Right = {int(vr)} | Avg Speed ➜ Left = {sl:.2f}, Right = {sr:.2f}\n")

    # Ανεβάζουμε το αρχείο στο container ως total.log
    total_blob_client = container_client.get_blob_client("total.log")
    total_blob_client.upload_blob(summary_output.getvalue(), overwrite=True)
    logging.info("📤 Uploaded total.log to test-logs container.")