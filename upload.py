import json
import random
import sys
import time
from os.path import join, isdir, isfile, dirname, getsize, exists
import subprocess
from traceback import print_exception

def unhandled_exception_handler(exc_type, exc_value, exc_traceback):
    print_exception(exc_type, exc_value, exc_traceback, file = sys.stderr)
    cleanup()

sys.excepthook = unhandled_exception_handler




def upload(src, dst, folder_creation_cache):
    remote_dir = dirname(dst)
    if remote_dir not in folder_creation_cache:
        process = subprocess.run(["ssh", "hcdp@storeapi.ikewai.org", f"mkdir -p {remote_dir}",], capture_output = True, text = True)
        if process.returncode != 0 or process.stderr != "":
            raise Exception(f"Failed to create remote directory: {remote_dir}, code: {process.returncode}")
        folder_creation_cache.add(remote_dir)
    process = subprocess.run(["rsync", "-vr", src, f"hcdp@storeapi.ikewai.org:{dst}"], capture_output = True, text = True)
    if process.returncode != 0 or process.stderr != "":
        raise Exception(f"Failed to copy files: src: {src}, dst: {dst}, code: {process.returncode}")
        

def get_backoff(delay):
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 1 + random.uniform(0, 1)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * (2 + random.uniform(0, 1))
        return backoff


def upload_retry(src: str, dst: str, folder_creation_cache: set, retry: int, max_delay: float, delay: float = 0):
    if(delay > 0):
        time.sleep(delay)
    try:
        upload(src, dst, folder_creation_cache)
    except Exception as e:
        retry -= 1
        if retry < 0:
            raise e
        backoff = get_backoff(delay)
        if max_delay is not None:
            backoff = min(max_delay, backoff)
        upload_retry(src, dst, folder_creation_cache, retry, max_delay, backoff)
        
def cleanup(start, total, successes):
    end = time.time()
    duration = end - start
    print("File uploads complete: success: %d, failed: %s, time: %.2f seconds" % (successes, total - successes, duration))
    

def main():
    #start execution timer
    start = time.time()
    successes = 0

    #set up configuration
    config = None
    with open("config.json", "r") as f:
        config = json.load(f)
    #deconstruct config
    files_to_upload: list = config["upload"]
    #optional params
    retry: int = config.get("retry")
    max_delay: float = config.get("max_backoff")
    include_empty = config.get("include_empty")

    #default retry to 0
    if retry is None:
        retry = 0
    if include_empty is None:
        include_empty = False
    #set up folder caching
    folder_creation_cache = set() 

    total_files = len(files_to_upload)
    for file_info in files_to_upload:    
        #ignored if local is dir
        rename = file_info.get("rename")
        src = file_info["local_path"]
        dst = join("/mnt/lustre/annotated/", file_info["remote_path"])
        if not dst.endswith("/"):
            dst += "/"
        if exists(src):
            #if local path is a directory ensure has trailing slash
            if isdir(src) and not src.endswith("/"):
                src += "/"
            #otherwise if file and should be renamed add new name to dst
            elif isfile(src) and (include_empty or getsize(src) > 0) and rename:
                dst = join(dst, rename)
            try:
                upload_retry(src, dst, folder_creation_cache, retry, max_delay)
                successes += 1
            except Exception as e:
                print(f"Failed to upload file, Error: {e}", file = sys.stderr)
        else:
            total_files -= 1
            print(f"Warning: local path {src} does not exist. Skipping...")

    #finished, run cleanup
    cleanup(start, total_files, successes)

if __name__ == "__main__":
    main()