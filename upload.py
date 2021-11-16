# Data uploader for workflow automation using Agave v2 for actors and file upload.

import json
from agavepy import Agave
import pickle
import random
import sys
import time
import os
from os.path import join, isdir, isfile, relpath
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from threading import Lock

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
        res = funct(**args)
        return res
    except exceptions as e:
        retry -= 1
        if retry < 0:
            raise e
        backoff = get_backoff(delay)
        return retry_wrapper(funct, args, exceptions, retry, backoff)


config = None

with open("config.json", "r") as f:
    config = json.load(f)

#deconstruct config
agave_options: dict = config["agave_options"]
files_to_upload: list = config["upload"]
#optional params
threads: int = config.get("processes")
retry: int = config.get("retry")
global_cache: str = config.get("global_cache")
write_exec_stats: str = config.get("write_exec_stats")
print_exec_stats: bool = config.get("print_exec_stats")

max_threads = cpu_count()
#if number of threads not provided, is 0 or less, or is greater than the max allowed set to the max
if threads is None or threads > max_threads or threads < 1:
    threads = max_threads

#default retry to 0
if retry is None:
    retry = 0
#default to print exec stats if not set
if print_exec_stats is None:
    print_exec_stats = True

folder_creation_cache = {}
if global_cache is not None:
    try:
        with open(global_cache, "rb") as cache:
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

cache_lock = Lock()
#lists are probably threadsafe due to the GIL, but use a lock when adding items to exec_details just to be safe
exec_details_lock = Lock()

with ThreadPool(threads) as pool:
    def file_info_handler(file_info):
        #relative to system root
        remote_root = file_info["remote_path"]
        local_root = file_info["local_path"]
        system_id = file_info["system_id"]
        #ignored if local is dir
        rename = file_info.get("rename")

        #permissions to set to remote directory
        dir_permissions = file_info.get("dir_permissions")
        if dir_permissions is None:
            dir_permissions = []
        #permissions to set to files
        file_permissions = file_info.get("file_permissions")
        if file_permissions is None:
            file_permissions = []

        path_data = []

        #if local path is a directory add all subfiles
        if isdir(local_root):
            for root, subdirs, files in os.walk(local_root):
                for filename in files:
                    #construct local file path
                    local_path = join(root, filename)
                    #get path relative to dir being copied
                    rel_path = relpath(root, local_root)
                    #combine relative path with remote path for file destination
                    remote_path = join(remote_root, rel_path)
                    paths = {
                        "local_path": local_path,
                        "remote_path": remote_path,
                        "fname": filename
                    }
                    path_data.append(paths)
        #if local path is just a file simply record local and remote data
        elif isfile(local_root):
            fname = rename
            if fname is None:
                fname = os.path.basename(local_root)
            paths = {
                "local_path": local_root,
                "remote_path": remote_root,
                "fname": fname
            }
            path_data.append(paths)
        #local path does not exist, record error
        else:
            print("Could not find local path %s" % (local_root), file = sys.stderr)
            with exec_details_lock:
                exec_details["failed"].append(local_root)

        dir_created = False

        def path_data_handler(paths):
            local_path = paths["local_path"]
            remote_path = paths["remote_path"]
            fname = paths["fname"]

            #lock while checking cache and creating/caching new dirs
            with cache_lock:
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
                        #add remote path to cache so not attempting to remake if multiple files use the same remote
                        system_cached_dirs.add(remote_path)
                        #one of the dirs in path data has been created successfully, so remote root should exist
                        dir_created = True
                    except Exception as e:
                        print("Unable to create directory:\nsystem: %s\path: %s\nerror: %s" % (system_id, remote_path, repr(e)), file = sys.stderr)
                else:
                    #dir already existed, can go ahead and apply perms
                    dir_created = True

            file_created = True
            with open(local_path, 'rb') as localFileToUpload:
                #pack import arguments into dict so rename can be excluded if not specified
                args = {
                    "filePath": remote_path,
                    "fileToUpload": localFileToUpload,
                    "systemId": system_id,
                    "fileName": fname
                }
                try:
                    retry_wrapper(ag.files.importData, args, (Exception), retry)
                    with exec_details_lock:
                        exec_details["success"].append(local_path)
                except Exception as e:
                    print("Unable to upload file:\nlocal: %s\nsystem: %s\nremote: %s\nerror: %s" % (local_path, system_id, remote_path, repr(e)), file = sys.stderr)
                    with exec_details_lock:
                        exec_details["failed"].append(local_path)
                    file_created = False
            #if file was successfully uploaded set permissions
            if file_created:
                for permission in file_permissions:
                    #combine file name with remote path
                    remote_file = join(remote_path, fname)
                    args = {
                        "body": permission,
                        "filePath": remote_file,
                        "systemId": system_id
                    }
                    try:
                        retry_wrapper(ag.files.updatePermissions, args, (Exception), retry)
                    except Exception as e:
                        print("Unable to apply permissions to remote file:\npermissions: %s\nsystem: %s\nfile: %s\nerror: %s" % (permission, system_id, remote_file, repr(e)), file = sys.stderr)


        pool.map(path_data_handler, path_data)
        
        #this stuff relies on previous, how to handle async over multiple file items?

        #if remote dir was successfully created set remote directory permissions
        if dir_created:
            for permission in dir_permissions:
                #apply permission to remote root dir
                args = {
                    "body": permission,
                    "filePath": remote_root,
                    "systemId": system_id
                }
                try:
                    retry_wrapper(ag.files.updatePermissions, args, (Exception), retry)
                except Exception as e:
                    print("Unable to apply permissions to remote directory:\npermissions: %s\nsystem: %s\ndir: %s\nerror: %s" % (permission, system_id, remote_root, repr(e)), file = sys.stderr)

    pool.map(file_info_handler, files_to_upload)


end = time.time()
duration = end - start
exec_details["time"] = duration
if print_exec_stats:
    success = len(exec_details["success"])
    failed = len(exec_details["failed"])
    print("File uploads complete: success: %d, failed: %s, time: %.2f seconds" % (success, failed, duration))
if write_exec_stats is not None:
    with open(write_exec_stats, "w") as f:
        json.dump(exec_details, f, indent = 4)

#dump cache to global if in use
if global_cache is not None:
    with open(global_cache, "wb") as cache:
        pickle.dump(folder_creation_cache, cache)