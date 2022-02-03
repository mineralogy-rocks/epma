# -*- coding: utf-8 -*-
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive', ]


class DriveApi:
    def __init__(self):
        self.creds = None
        self.service = None

        self.folders_map = {
            'RRUFF': '1_ZqorMP7Z6Yu-C2_-R0ucBLOK5LHjVkK',
            'APVV': '1WyG_K_LgsHD9CxDM-mc6MW0clxGKTVFU',
        }

    def authorize(self):
        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('.env.json', SCOPES)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())

        try:
            self.service = build('drive', 'v3', credentials=self.creds)

        except HttpError as error:
            # TODO: handle error?
            print(f'An error occurred when creating a drive service: {error}')

    def get_sheets_meta(self, parent_folder: str, mineral_list: list) -> list:
        parent_folder_id = self.folders_map[parent_folder]

        q = ("(mimeType contains 'application/vnd.ms-excel' or "
             "mimeType contains 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
             "mimeType contains 'text/plain') and "
             "parents in '{}' and ".format(parent_folder_id) +
             ' or '.join(["name contains '{}'".format(mineral) for mineral in mineral_list])
             )
        try:
            search = self.service.files().list(
                q=q,
                pageSize=100,
                fields="nextPageToken, files(id, name)"
            ).execute()
            items = search.get('files', [])

            return items

        except HttpError as error:
            # TODO: handle error?
            print(f'An error occurred: {error}')

    def disconnect(self):
        self.service.close()
