# Data uploader for workflow automation using Agave v2 for actors and file upload.

import json, os, datetime, mimetypes
from typing import List
from agavepy import Agave

ag = Agave(
    api_key=os.environ.get('TAPIS_API_KEY'),
    api_secret=os.environ.get('TAPIS_API_SECRET'),
    client_name=os.environ.get('TAPIS_CLIENT_NAME')
)

# Set up upload directories
with open("file-upload-list.json") as file_list:
    files_to_upload: list = json.loads(file_list.read())['upload']

# Dynamically make the directories to upload into - in progress - may be more complex than day/month/year
base_upload_dir = '/containerization'
current_year: str = str(datetime.datetime.now().year)
current_month: str = str(datetime.datetime.now().month)
current_day: str = str(datetime.datetime.now().day)
ag.files.manageOnDefaultSystem(body={"action": "mkdir", "path": current_year}, sourceFilePath=base_upload_dir)
ag.files.manageOnDefaultSystem(body={"action": "mkdir", "path": current_month}, sourceFilePath=f"{base_upload_dir}/{current_year}")
ag.files.manageOnDefaultSystem(body={"action": "mkdir", "path": current_day}, sourceFilePath=f"{base_upload_dir}/{current_year}/{current_month}")


# Finally, upload the files. Double-check this
print(f"Files to upload: {files_to_upload}")
for file_info in files_to_upload:
    localFileToUpload = open(file_info['local-path'], 'rb')
    res = ag.files.importData(fileName=file_info['local-path'], filePath=f"{base_upload_dir}/{current_year}/{current_month}/{current_day}", fileToUpload=localFileToUpload)
    print(res)