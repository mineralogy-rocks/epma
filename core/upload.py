# -*- coding: utf-8 -*-
import io

from googleapiclient.http import MediaIoBaseDownload
import pandas as pd

from utils.gdrive import DriveApi
from utils.gsheet import GoogleSheet

DriveApi = DriveApi()
GoogleSheet = GoogleSheet()

DriveApi.authorize()
GoogleSheet.run_main()


def get_clinopyroxene_subgroup():
    sheets_meta = DriveApi.get_sheets_meta(parent_folder='RRUFF', mineral_list=['Aegirine', 'Augite', 'Clinoenstatite',
                                                                                'Clinoferrosilite', 'Davisite', 'Esseneite',
                                                                                'Grossmanite', 'Jadeite', 'Jervisite',
                                                                                'Kanoite', 'Kushiroite', 'Namansilite',
                                                                                'Natalyite', 'Omphacite', ''])


file_id = sheets_meta[0]['id']
request = DriveApi.service.files().get_media(fileId=file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()
    print ("Download %d%%." % int(status.progress() * 100))
data_ = pd.read_excel(fh, usecols=None, sheet_name=None)

for sheet in data_.keys():


import openpyxl
import io

# The bytes object (Something like b'PK\x03\x04\x14\x00\x06\x00\x08\x00\x00...)
file = part.get_payload(decode=True)

xlsx = io.BytesIO(file)
wb = openpyxl.load_workbook(xlsx)
ws = wb['Sheet1']

for cells in ws.iter_rows():
    print([cell.value for cell in cells])