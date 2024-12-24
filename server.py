from flask import Flask
import subprocess
import threading
import os

app = Flask(__name__)

# Variable to determine the number of scripts to run
num = int(os.environ.get("NUMBER_OF_MACHINE_IN_CYCLE", 1))  # Default to 1 if NUM not set

# Function to start a specific script as a separate process
def start_script(script_name):
    subprocess.run(["python", script_name])

# Function to run scripts based on the value of `num`
def start_livestock_viewers():
    scripts = [
        "Livestockmarketviewer1.py",
        "Livestockmarketviewer2.py",
        "Livestockmarketviewer3.py",
        "Livestockmarketviewer4.py",
        "Livestockmarketviewer5.py",
    ]

    threads = []
    for i in range(num):
        if i < len(scripts):  # Ensure `num` doesn't exceed the available scripts
            thread = threading.Thread(target=start_script, args=(scripts[i],))
            threads.append(thread)
            thread.start()

    # Optionally, wait for all threads to complete (not required for Flask to run)
    for thread in threads:
        thread.join()

@app.route('/')
def index():
    return f"Livestock Market Viewer is running {num} script(s)!"

if __name__ == '__main__':
    # Start the livestock viewers based on `num`
    viewer_thread = threading.Thread(target=start_livestock_viewers)
    viewer_thread.start()

    # Use the PORT environment variable for the port
    port = int(os.environ.get("PORT", 5000))  # Default to 5000 if PORT not set
    app.run(host='0.0.0.0', port=port)
