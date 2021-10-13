# Data uploader for workflow automation using Agave v2 for actors and file upload.

import json, os
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

folder_creation_cache = set()
if use_global_cache:
    try:
        with open(global_cache_file, "rb") as cache:
            folder_creation_cache = pickle.load(cache)
    except IOError:
        print("Could not find global cache. Creating a new one...")

ag = Agave(**agave_options)

# Finally, upload the files. Double-check this
print(f"Files to upload: {files_to_upload}")
for file_info in files_to_upload:

    remote_path = file_info["remote_path"]
    local_path = file_info["local_path"]

    if not remote_path in folder_creation_cache:
        try:
            #should recursively make parent directories as needed based on agave docs
            ag.files.manageOnDefaultSystem(body = {"action": "mkdir", "path": remote_path}, sourceFilePath = "/")
        #operation will fail if directory already exists (what exactly does it throw in this case? need to specify if adding retry, just pass for now)
        except:
            pass
        #add remote path to cache so not attempting to remake if multiple files use the same remote
        folder_creation_cache.add(remote_path)

    rename = file_info.get("rename")

    with open(local_path, 'rb') as localFileToUpload:
        #pack import arguments into dict so rename can be excluded if not specified
        importArgs = {
            "filePath": remote_path,
            "fileToUpload": local_path
        }
        if rename is not None:
            importArgs["rename"] = rename
        res = ag.files.importData(**importArgs)
        print(res)

#dump cache to global if in use
if use_global_cache:
    with open(global_cache_file, "wb") as cache:
        pickle.dump(folder_creation_cache, cache)