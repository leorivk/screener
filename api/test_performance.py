import subprocess
import time
import psutil
import requests
import threading
import os

# --- Global variables ---
peak_memory_mb = 0
keep_monitoring = True

def monitor_memory(process):
    """Monitors the memory of a process and its children in a separate thread."""
    global peak_memory_mb, keep_monitoring
    try:
        while keep_monitoring:
            p = psutil.Process(process.pid)
            children = p.children(recursive=True)
            total_memory_bytes = p.memory_info().rss
            for child in children:
                total_memory_bytes += child.memory_info().rss
            
            current_memory_mb = total_memory_bytes / (1024 * 1024)
            if current_memory_mb > peak_memory_mb:
                peak_memory_mb = current_memory_mb
            time.sleep(0.1)
    except psutil.NoSuchProcess:
        pass

def run_test(model_name, port):
    """Starts server, runs test, and measures performance."""
    global keep_monitoring, peak_memory_mb
    
    print(f"--- Starting Performance Test for {model_name} Model ---")
    command = ["python3", "-m", "uvicorn", "app.main:app", "--port", str(port)]
    env = os.environ.copy()
    env["PYTHONPATH"] = "/Users/ivk/Documents/projects/screener/api/venv/lib/python3.9/site-packages"
    
    # Start server and memory monitoring
    server_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    monitor_thread = threading.Thread(target=monitor_memory, args=(server_process,))
    monitor_thread.start()
    
    print(f"Server for {model_name} started with PID: {server_process.pid}. Waiting for initialization...")
    time.sleep(5) # Server starts fast as it doesn't load data

    # API Latency Test
    url = f"http://127.0.0.1:{port}/screener"
    payload = {"market": "NASDAQ", "sector": "IT", "minMarketCap": 49950, "maxPer": 10}
    
    print("Sending request...")
    start_time = time.time()
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        response_size = len(response.content)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Request failed: {e}")
        response_size = -1
    end_time = time.time()
    latency = end_time - start_time
    print("Request finished.")

    # Shutdown
    print("Shutting down server...")
    keep_monitoring = False
    parent = psutil.Process(server_process.pid)
    for child in parent.children(recursive=True):
        child.terminate()
    parent.terminate()
    server_process.wait()
    monitor_thread.join()

    # Print Results
    print("\n--- Performance Results ---")
    print(f"Model: {model_name}")
    print(f"API Latency: {latency:.4f} seconds")
    print(f"Peak Memory Usage: {peak_memory_mb:.2f} MB")
    print(f"Response Size: {response_size / 1024:.2f} KB")
    print("---------------------------\n")

if __name__ == "__main__":
    run_test("Clickhouse Based", 8002)
