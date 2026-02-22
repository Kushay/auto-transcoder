import os
import subprocess
import queue
import threading
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging

task_queue = queue.Queue()
queued_files = set()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('transcoder_logs.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class QueueHandler(FileSystemEventHandler):
    def on_created(self, event):
        file = event.src_path
        if 'transcoded' in event.src_path:
            return
        if not event.is_directory and file.lower().endswith(('.mp4', '.mov')):
            if file not in queued_files:
                logger.debug(f"listener daemon: adding {file} to queue, size: {os.path.getsize(file)}")
                task_queue.put(file)
                queued_files.add(file)
            else:
                logger.debug(f"skipping duplicate file for {file}")


def searchdir(directory):
    for (dirpath, dirnames, filenames) in os.walk(directory):
        if 'transcoded' in dirpath: ##doesn't encode the completed tasks
            continue
        for filename in filenames: 
            if filename.endswith(('.mp4', '.avi', '.mkv', '.mov')):
                full_path = os.path.join(dirpath, filename)
                queued_files.add(full_path)
                logger.debug(f"searchdir function: {full_path} found, added to queue.")
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
                logging.debug("is_file_ready function: file still uploading")
                continue #file still uploading
        except OSError:
            logging.debug("is_file_ready function: ran into OSError, running check again in 2 sec.")
            time.sleep(2)
            continue
            
def transcode_video(input_path, output_dir='/data/raw-footage/transcoded/'):
    file_name = os.path.basename(input_path)
    output_path = os.path.join(output_dir, file_name)
    logger.debug(f"Starting transcode for: '{file_name}', output='{output_path}'")
    result = subprocess.run(['flatpak', 'run', '--command=HandBrakeCLI', 'fr.handbrake.ghb', 
                    '-i', input_path, 
                    '-o', output_path, 
                    '--preset', 'Production Max',
                    '-e', 'x265_10bit',
                    "--encoder-profile", "main10"])
    if result.returncode == 0:
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            os.remove(input_path)
            logger.debug(f"Success, deleting {input_path}")
        else: 
            logger.error(f"Error: output file missing")
    else: 
        logger.error("Error: Handbrake failed to transcode.")





def worker(): 
    while True: 
        input_path = task_queue.get()
        current_queue = [os.path.basename(f) for f in list(task_queue.queue)]
        logger.debug(f"queue: preparing '{input_path}' to transcode, current queue: {current_queue}")
        if input_path == None: break ##graceful kill switch
        try:
            if is_file_ready(input_path):
                logger.debug(f"'{input_path}' is ready to transcode")
                transcode_video(input_path) 
        except OSError:
            logger.error(f"'{input_path}' failed to transcode.")
        finally:
            logger.debug(f"'{input_path}' is done transcoding. ")
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
    t.join()
    logger.critical("The script has stopped.")

