# Data uploader for workflow automation using Agave v2 for actors and file upload.

import json
from agavepy import Agave
import pickle

config = None

with open("config.json", "r") as f:
    config = json.load(f)

#deconstruct config
use_global_cache: bool = config["use_global_cache"]
global_cache_file: str = config["global_cache_file"]
agave_options: dict = config["agave_options"]
files_to_upload: list = config["upload"]

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
        #will recursively create directory off of system root
        ag.files.manage(body = {"action": "mkdir", "path": remote_path}, filePath = "", systemId = system_id)
        #add remote path to cache so not attempting to remake if multiple files use the same remote
        system_cached_dirs.add(remote_path)

    rename = file_info.get("rename")

    with open(local_path, 'rb') as localFileToUpload:
        #pack import arguments into dict so rename can be excluded if not specified
        importArgs = {
            "filePath": remote_path,
            "fileToUpload": localFileToUpload,
            "systemId": system_id,
        }
        if rename is not None:
            importArgs["fileName"] = rename
        ag.files.importData(**importArgs)

#dump cache to global if in use
if use_global_cache:
    with open(global_cache_file, "wb") as cache:
        pickle.dump(folder_creation_cache, cache)
