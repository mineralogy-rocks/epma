# -*- coding: utf-8 -*-
import io
import re

from googleapiclient.http import MediaIoBaseDownload
import pandas as pd

from utils.gdrive import DriveApi
from utils.gsheet import GoogleSheet
from core import constants

DriveApi = DriveApi()
GoogleSheet = GoogleSheet()

DriveApi.authorize()
GoogleSheet.run_main()

def get_pyroxene_group():
    pyroxene_group = GoogleSheet._get_children(specie_name='pyroxene group', status_in=[constants.IMA_SPECIE],
                                               taxonomy_level='group')
    sheets_meta = DriveApi.get_sheets_meta(parent_folder='RRUFF', mineral_list=pyroxene_group)
    output = pd.DataFrame(columns=['rruff_id', 'mineral_name', 'SiO2', 'FeO', 'Fe2O3', 'MgO', 'CaO', 'Al2O3', 'TiO2',
                                   'MnO', 'Na2O', 'K2O', 'Cr2O3', 'F', 'Total', ])

    for spreasdheet in sheets_meta:
        spreadsheet = sheets_meta[0] # TODO: remove after testing
        file_id = spreasdheet['id']
        regex_pattern = re.match('(^[A-Za-z0-9-]+)__(\w+-\d+)__', spreasdheet['name'])
        mineral_name, rruff_id = regex_pattern.group(1), regex_pattern.group(2)

        request = DriveApi.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print ("Download %d%%." % int(status.progress() * 100))
        data_ = pd.read_excel(fh, usecols=None, sheet_name=None)

        for sheet in data_.keys():
            sheet = 'pdf_output' # TODO: remove after testing
            print(data_[sheet])
            data_[sheet].set_index('Oxide', inplace=True)
            transpose_ = data_[sheet].transpose()
            transpose_ = transpose_.iloc[:, transpose_.columns.isin(output.columns)]
            transpose_['rruff_id'] = rruff_id + '__' + transpose_.index
            transpose_['mineral_name'] = mineral_name

            # TODO: add regex filtering of unnamed rows, e.g. 'Unnamed: 14'

            output.append(transpose_.reset_index(drop=True))
