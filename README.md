# Simple Uploader container for sending files into the 'Ike Wai Gateway.

*Available as Docker image `ikewai/data-uploader:latest`*
Configure using Docker Compose (`docker-compose.yml`) and the JSON-formatted list of files (`files-to-upload.json`). See examples in the `/quickstart` directory to get started.

### 1. Authenticate using environment variables in your `docker-compose.yml` file.

### 2. Mount your files from the host filesystem using the `docker-compose.yml` file.

### 3. Set up the list of files to upload to the gateway, and the destination of each file in `files-to-upload.json`. Mount this file into /scripts.