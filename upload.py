# Data uploader for workflow automation using Agave v2 for actors and file upload.

import json
from agavepy import Agave
import pickle
import random
import sys
import time
import os

def get_backoff(delay):
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * 2 + random.uniform(0, delay)
        return backoff

def retry_wrapper(funct, args: dict, exceptions: tuple, retry: int, delay: float = 0):
    try:
        funct(**args)
    except exceptions as e:
        retry -= 1
        if retry < 0:
            raise e
        backoff = get_backoff(delay)
        retry_wrapper(funct, args, exceptions, retry, backoff)


config = None

with open("config.json", "r") as f:
    config = json.load(f)

#deconstruct config
use_global_cache: bool = config["use_global_cache"]
global_cache_file: str = config["global_cache_file"]
agave_options: dict = config["agave_options"]
files_to_upload: list = config["upload"]
retry: int = config["retry"]
write_exec_stats = config.get("write_exec_stats")
print_exec_stats = config.get("print_exec_stats")
#default to print exec stats if not set
if print_exec_stats is None:
    print_exec_stats = True

folder_creation_cache = {}
if use_global_cache:
    try:
        with open(global_cache_file, "rb") as cache:
            folder_creation_cache = pickle.load(cache)
    except IOError:
        print("Could not find global cache. Creating a new one...")

ag = Agave(**agave_options)
#create token
ag.token.create()

start = time.time()
exec_details = {
    "success": [],
    "failed": [],
    "time": 0
}

# Finally, upload the files.
for file_info in files_to_upload:
    #relative to system root
    remote_path = file_info["remote_path"]
    local_path = file_info["local_path"]
    system_id = file_info["system_id"]

    system_cached_dirs = folder_creation_cache.get(system_id)
    #system not cached, register with empty set
    if system_cached_dirs is None:
        system_cached_dirs = set()
        folder_creation_cache[system_id] = system_cached_dirs

    #if remote_path is not in system cache create and add to cache
    if not remote_path in system_cached_dirs:
        args = {
            "body": {
                "action": "mkdir",
                "path": remote_path
            },
            "filePath": "",
            "systemId": system_id
        }
        try:
            #will recursively create directory off of system root
            retry_wrapper(ag.files.manage, args, (Exception), retry)
        except Exception as e:
            print("Unable to create directory:\nsystem: %s\path: %s\nerror: %s" % (system_id, remote_path, repr(e)), file = sys.stderr)
        #add remote path to cache so not attempting to remake if multiple files use the same remote
        system_cached_dirs.add(remote_path)

    rename = file_info.get("rename")

    with open(local_path, 'rb') as localFileToUpload:
        #pack import arguments into dict so rename can be excluded if not specified
        args = {
            "filePath": remote_path,
            "fileToUpload": localFileToUpload,
            "systemId": system_id,
        }
        if rename is not None:
            args["fileName"] = rename
        
        try:
            retry_wrapper(ag.files.importData, args, (Exception), retry)
            exec_details["success"].append(local_path)
        except Exception as e:
            print("Unable to upload file:\nlocal: %s\nsystem: %s\nremote: %s\nerror: %s" % (local_path, system_id, remote_path, repr(e)), file = sys.stderr)
            exec_details["failed"].append(local_path)


end = time.time()
duration = end - start
exec_details["time"] = duration
if print_exec_stats:
    success = len(exec_details["success"])
    failed = len(exec_details["failed"])
    print("File uploads complete:\nsuccess: %d, failed: %s, time: %.2f seconds" % (success, failed, duration))
if write_exec_stats is not None:
    with open(write_exec_stats, "wb") as f:
        json.dump(exec_details, f, indent = 4)

#dump cache to global if in use
if use_global_cache:
    with open(global_cache_file, "wb") as cache:
        pickle.dump(folder_creation_cache, cache)