import os
import subprocess
import queue
import threading
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


task_queue = queue.Queue()
queued_files = set()

class QueueHandler(FileSystemEventHandler):
    def on_created(self, event):
        file = event.src_path
        if 'transcoded' in event.src_path:
            return
        if not event.is_directory and file.lower().endswith(('.mp4', '.mov')):
            if file not in queued_files:
                print(f"adding {file} to queue")
                task_queue.put(file)
                queued_files.add(file)
            else:
                print(f"skipping duplicate file for {file}")

def searchdir(directory):
    for (dirpath, dirnames, filenames) in os.walk(directory):
        if 'transcoded' in dirpath: ##doesn't encode the completed tasks
            continue
        for filename in filenames: 
            if filename.endswith(('.mp4', '.avi', '.mkv', '.mov')):
                full_path = os.path.join(dirpath, filename)
                queued_files.add(full_path)
                yield full_path


def is_file_ready(file_path):
    while True: 
        try:    
            cur_size = os.path.getsize(file_path)
            time.sleep(5)
            next_size = os.path.getsize(file_path)
            if next_size == cur_size and next_size > 0:
                return True 
            else: 
                continue #file still uploading
        except OSError:
            time.sleep(2)
            continue
            
def transcode_video(input_path, output_dir='/data/raw-footage/transcoded/'):
    file_name = os.path.basename(input_path)
    output_path = os.path.join(output_dir, file_name)
    result = subprocess.run(['flatpak', 'run', '--command=HandBrakeCLI', 'fr.handbrake.ghb', 
                    '-i', input_path, 
                    '-o', output_path, 
                    '--preset', 'Production Max',
                    '-e', 'x265_10bit',
                    "--encoder-profile", "main10"])
    if result.returncode == 0:
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            os.remove(input_path)
            print(f"Success, deleted {file_name}")
        else: 
            print(f"Error: output file missing")
    else: 
        print("Error: Handbrake failed to transcode.")
import os
import subprocess
import queue
import threading
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


task_queue = queue.Queue()
queued_files = set()

class QueueHandler(FileSystemEventHandler):
    def on_created(self, event):
        file = event.src_path
        if 'transcoded' in event.src_path:
            return
        if not event.is_directory and file.lower().endswith(('.mp4', '.mov')):
            if file not in queued_files:
                print(f"adding {file} to queue")
                task_queue.put(file)
                queued_files.add(file)
            else:
                print(f"skipping duplicate file for {file}")

def searchdir(directory):
    for (dirpath, dirnames, filenames) in os.walk(directory):
        if 'transcoded' in dirpath: ##doesn't encode the completed tasks
            continue
        for filename in filenames: 
            if filename.endswith(('.mp4', '.avi', '.mkv', '.mov')):
                full_path = os.path.join(dirpath, filename)
                queued_files.add(full_path)
                yield full_path


def is_file_ready(file_path):
    while True: 
        try:    
            cur_size = os.path.getsize(file_path)
            time.sleep(5)
            next_size = os.path.getsize(file_path)
            if next_size == cur_size and next_size > 0:
                return True 
            else: 
                continue #file still uploading
        except OSError:
            time.sleep(2)
            continue
            
def transcode_video(input_path, output_dir='/data/raw-footage/transcoded/'):
    file_name = os.path.basename(input_path)
    output_path = os.path.join(output_dir, file_name)
    result = subprocess.run(['flatpak', 'run', '--command=HandBrakeCLI', 'fr.handbrake.ghb', 
                    '-i', input_path, 
                    '-o', output_path, 
                    '--preset', 'Production Max',
                    '-e', 'x265_10bit',
                    "--encoder-profile", "main10"])
    if result.returncode == 0:
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            os.remove(input_path)
            print(f"Success, deleted {file_name}")
        else: 
            print(f"Error: output file missing")
    else: 
        print("Error: Handbrake failed to transcode.")

def worker(): 
    while True: 
        input_path = task_queue.get()
        if input_path == None: break ##graceful kill switch
        if is_file_ready(input_path):
            transcode_video(input_path)
        task_queue.task_done()




if __name__ == "__main__":
    queued_files.clear()
    t = threading.Thread(target=worker)
    t.start()
    path = '/data/raw-footage/'
    for file in searchdir(path):
        task_queue.put(file)
    observer = Observer()
    observer.schedule(QueueHandler(), path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def worker(): 
    while True: 
        input_path = task_queue.get()
        if input_path == None: break ##graceful kill switch
        try:
            if is_file_ready(input_path):
                transcode_video(input_path)
        except OSError:
            print("ERROR TRANSCODING")
        finally:
            task_queue.task_done()




if __name__ == "__main__":
    queued_files.clear()
    t = threading.Thread(target=worker)
    t.start()
    path = '/data/raw-footage/'
    for file in searchdir(path):
        task_queue.put(file)
    observer = Observer()
    observer.schedule(QueueHandler(), path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
