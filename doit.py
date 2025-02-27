from __future__ import print_function
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
    page_token = None

    while True:
        response = drive_service.files().list(
            q="sharedWithMe = true",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        for file in response.get("files", []):
            print(file['name'])
            try:
                body = {
                    "name": file['name'],
                    'parents': [target_folder_id]
                }
                copied_file = drive_service.files().copy(
                    fileId=file["id"],
                    body=body
                ).execute()
                print(f"Copied: {file['name']} -> {copied_file['id']}")
            except Exception as e:
                print(f"Error copying {file['name']}: {e}")

        page_token = response.get("nextPageToken", None)
        if not page_token:
            print('no more pages')
            break
        else:
            print('next page')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Copy shared with me files to the folder id.')
    parser.add_argument('target_folder_id', help='google folder id')
    args = parser.parse_args()
    copy_shared_files(args.target_folder_id)
    print('all done')
