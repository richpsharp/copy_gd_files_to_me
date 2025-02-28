import argparse
import logging
import os
import pickle
import sys
import time

import google.auth.transport.requests
import google_auth_oauthlib.flow
import googleapiclient.discovery


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('googleapiclient.discovery').setLevel(logging.INFO)

SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_user_credentials():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                "client_secret.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return creds


LOGGER = logging.getLogger(__name__)


def copy_shared_files(target_folder_name):
    creds = get_user_credentials()
    drive_service = googleapiclient.discovery.build("drive", "v3", credentials=creds)

    def create_folder_with_backoff(folder_body, max_retries=20, base_delay=1.0):
        """
        Create a folder with exponential backoff on errors (e.g., SSL).
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                return drive_service.files().create(
                    body=folder_body,
                    fields='id'
                ).execute()
            except Exception as e:
                if attempt >= max_retries:
                    raise
                LOGGER.warning(
                    f"Retrying folder creation '{folder_body.get('name')}' "
                    f"(attempt {attempt}/{max_retries}) due to error: {e}. "
                    f"Waiting {base_delay} seconds..."
                )
                time.sleep(base_delay)
                base_delay *= 2

    def get_or_create_folder_id(folder_name):
        query = (
            f"name = '{folder_name}' "
            "and mimeType = 'application/vnd.google-apps.folder' "
            "and trashed = false"
        )
        results = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        files_found = results.get('files', [])
        if files_found:
            return files_found[0]['id']
        else:
            folder_body = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            new_folder = create_folder_with_backoff(folder_body)
            return new_folder['id']

    def file_exists_in_folder(parent_id, file_name):
        """
        Check if a non-folder file with the given name already exists
        under the specified parent folder.
        """
        query = (
            f"'{parent_id}' in parents "
            "and mimeType != 'application/vnd.google-apps.folder' "
            "and trashed = false "
            f"and name = '{file_name}'"
        )
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        existing = results.get("files", [])
        return len(existing) > 0

    def copy_file_with_backoff(file_id, body, max_retries=5, base_delay=1.0):
        attempt = 0
        while True:
            attempt += 1
            try:
                return drive_service.files().copy(
                    fileId=file_id,
                    body=body,
                    fields="id, name, parents"
                ).execute()
            except Exception as e:
                if attempt >= max_retries:
                    raise
                LOGGER.warning(
                    f"Retrying copy for '{body.get('name')}' (attempt {attempt}/{max_retries}) "
                    f"due to error: {e}. Waiting {base_delay} seconds..."
                )
                time.sleep(base_delay)
                base_delay *= 2

    def get_or_create_subfolder(parent_id, folder_name):
        """
        Check if a folder with `folder_name` exists under `parent_id`.
        If it exists, return that folder's ID; otherwise create a new folder.
        """
        query = (
            f"'{parent_id}' in parents "
            "and mimeType = 'application/vnd.google-apps.folder' "
            f"and name = '{folder_name}' "
            "and trashed = false"
        )
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        folders_found = results.get('files', [])
        if folders_found:
            existing_id = folders_found[0]['id']
            LOGGER.info(f"Folder '{folder_name}' already exists under parent '{parent_id}' -> {existing_id}")
            return existing_id
        else:
            LOGGER.info(f"Creating new folder '{folder_name}' under parent '{parent_id}'")
            folder_body = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            new_folder = create_folder_with_backoff(folder_body)
            return new_folder['id']

    target_folder_id = get_or_create_folder_id(target_folder_name)

    def replicate_folder(source_folder_id, destination_parent_id):
        folder_data = drive_service.files().get(
            fileId=source_folder_id,
            fields="id, name"
        ).execute()

        folder_name = folder_data['name']
        LOGGER.info(f"Replicating folder: {folder_name}")
        new_folder_id = get_or_create_subfolder(destination_parent_id, folder_name)

        page_token = None
        while True:
            resp = drive_service.files().list(
                q=f"'{source_folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()

            for item in resp.get('files', []):
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    replicate_folder(item['id'], new_folder_id)
                else:
                    if file_exists_in_folder(new_folder_id, item['name']):
                        LOGGER.info(
                            f"File '{item['name']}' already exists in folder ID '{new_folder_id}', skipping."
                        )
                        continue
                    LOGGER.info(f"Submitting copy for file: {item['name']}")
                    body = {
                        'name': item['name'],
                        'parents': [new_folder_id]
                    }
                    try:
                        result = copy_file_with_backoff(item['id'], body)
                        LOGGER.info(f"Copied '{name}' -> new file ID: {result['id']}")
                    except Exception as e:
                        LOGGER.exception(f"Error copying '{name}': {e}")
                        raise

            page_token = resp.get('nextPageToken', None)
            if not page_token:
                break

    page_token = None
    while True:
        response = drive_service.files().list(
            q="sharedWithMe = true",
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()

        for f in response.get("files", []):
            name = f['name']
            if f["mimeType"] == "application/vnd.google-apps.folder":
                try:
                    replicate_folder(f['id'], target_folder_id)
                except Exception as e:
                    LOGGER.exception(f"Error copying folder '{name}': {e}")
                    raise
            else:
                if file_exists_in_folder(target_folder_id, name):
                    LOGGER.info(
                        f"File '{name}' already exists in folder ID '{target_folder_id}', skipping."
                    )
                    continue
                LOGGER.info(f"Submitting copy for shared file: {name}")
                body = {
                    "name": name,
                    "parents": [target_folder_id]
                }
                try:
                    result = copy_file_with_backoff(f["id"], body)
                    LOGGER.info(f"Copied '{name}' -> new file ID: {result['id']}")
                except Exception as e:
                    LOGGER.exception(f"Error copying '{name}': {e}")
                    raise

        page_token = response.get("nextPageToken", None)
        if not page_token:
            break

    LOGGER.info('waiting for executor to shutdown')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy shared with me files to the folder id.')
    parser.add_argument('target_folder_id', help='google folder id')
    args = parser.parse_args()
    copy_shared_files(args.target_folder_id)
    LOGGER.info('all done')
