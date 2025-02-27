from __future__ import print_function
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import os
import pickle
import google.auth.transport.requests
import google_auth_oauthlib.flow
import googleapiclient.discovery

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


def copy_shared_files(target_folder_id):
    creds = get_user_credentials()
    drive_service = googleapiclient.discovery.build("drive", "v3", credentials=creds)
    executor = ThreadPoolExecutor(max_workers=5)

    def replicate_folder(folder_id, destination_parent_id):
        folder_data = drive_service.files().get(
            fileId=folder_id,
            fields="id, name"
        ).execute()

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
                q=f"'{folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()

            tasks = []
            for item in resp.get('files', []):
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    replicate_folder(item['id'], new_folder_id)
                else:
                    body = {
                        'name': item['name'],
                        'parents': [new_folder_id]
                    }
                    tasks.append(executor.submit(drive_service.files().copy, fileId=item['id'], body=body))

            for _ in as_completed(tasks):
                pass

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

        tasks = []
        for file in response.get("files", []):
            if file["mimeType"] == "application/vnd.google-apps.folder":
                replicate_folder(file['id'], target_folder_id)
            else:
                body = {
                    "name": file['name'],
                    "parents": [target_folder_id]
                }
                tasks.append(executor.submit(drive_service.files().copy, fileId=file["id"], body=body))

        for _ in as_completed(tasks):
            pass

        page_token = response.get("nextPageToken", None)
        if not page_token:
            break

    executor.shutdown(wait=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy shared with me files to the folder id.')
    parser.add_argument('target_folder_id', help='google folder id')
    args = parser.parse_args()
    copy_shared_files(args.target_folder_id)
    print('all done')
