from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import logging
import os
import pickle
import sys

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


import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import googleapiclient.discovery

LOGGER = logging.getLogger(__name__)

def copy_shared_files(target_folder_name):
    creds = get_user_credentials()
    drive_service = googleapiclient.discovery.build("drive", "v3", credentials=creds)
    executor = ThreadPoolExecutor(max_workers=5)

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
            new_folder = drive_service.files().create(
                body={
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                },
                fields='id'
            ).execute()
            return new_folder['id']

    # Helper method so the submission to ThreadPoolExecutor runs the `.execute()` call
    def copy_file(file_id, body):
        return drive_service.files().copy(
            fileId=file_id,
            body=body,
            fields="id, name, parents"
        ).execute()

    target_folder_id = get_or_create_folder_id(target_folder_name)

    def replicate_folder(source_folder_id, destination_parent_id):
        folder_data = drive_service.files().get(
            fileId=source_folder_id,
            fields="id, name"
        ).execute()

        LOGGER.info(f"Replicating folder: {folder_data['name']}")

        # Create the corresponding folder in the destination
        new_folder = drive_service.files().create(
            body={
                'name': folder_data['name'],
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [destination_parent_id]
            },
            fields="id"
        ).execute()
        new_folder_id = new_folder['id']

        page_token = None
        while True:
            resp = drive_service.files().list(
                q=f"'{source_folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()

            tasks = []
            for item in resp.get('files', []):
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    # Recursively copy subfolder
                    replicate_folder(item['id'], new_folder_id)
                else:
                    LOGGER.info(f"Submitting copy for file: {item['name']}")
                    body = {
                        'name': item['name'],
                        'parents': [new_folder_id]
                    }
                    # Submit the copy_file function to the executor
                    future = executor.submit(copy_file, item['id'], body)
                    tasks.append((item['name'], future))
                    break

            # Collect results from all copy tasks
            for name, future in tasks:
                try:
                    result = future.result()
                    LOGGER.info(f"Copied '{name}' -> new file ID: {result['id']}")
                    sys.exit()
                except Exception as e:
                    LOGGER.error(f"Error copying '{name}': {e}")

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

        main_tasks = []
        for f in response.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                try:
                    replicate_folder(f['id'], target_folder_id)
                except Exception as e:
                    LOGGER.error(f"Error copying folder '{f['name']}': {e}")
            else:
                LOGGER.info(f"Submitting copy for shared file: {f['name']}")
                body = {
                    "name": f['name'],
                    "parents": [target_folder_id]
                }
                future = executor.submit(copy_file, f["id"], body)
                main_tasks.append((f['name'], future))

        # Wait for the top-level copy tasks to finish
        for name, future in main_tasks:
            try:
                result = future.result()
                LOGGER.info(f"Copied '{name}' -> new file ID: {result['id']}")
            except Exception as e:
                LOGGER.error(f"Error copying '{name}': {e}")

        page_token = response.get("nextPageToken", None)
        if not page_token:
            break

    executor.shutdown(wait=True)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy shared with me files to the folder id.')
    parser.add_argument('target_folder_id', help='google folder id')
    args = parser.parse_args()
    copy_shared_files(args.target_folder_id)
    LOGGER.info('all done')
