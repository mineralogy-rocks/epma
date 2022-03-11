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

    # TODO: add regex pattern to match column headers of oxide names

    oxide_pattern = re.compile(r"SiO2|FeO|Fe2O3|MgO|CaO|Al2O3|TiO2|MnO|ZnO|Na2O|K2O|Cr2O3|F|Total")

    for spreasdheet in sheets_meta:
        spreadsheet = sheets_meta[0] # TODO: remove after testing
        file_id = spreadsheet['id']
        regex_pattern = re.match('(^[A-Za-z0-9-]+)__(\w+-\d+)__', spreadsheet['name'])
        mineral_name, rruff_id = regex_pattern.group(1), regex_pattern.group(2)

        request = DriveApi.service.files().get_media(fileId=file_id) # Actinolite - 1emu_NdP4ZHrO7eQyLCw0yJk7LOcp1_Y_
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


    # 1 iterate over columns and get a column of Oxides
    test = data_['pdf_output'] # or Sheet1
    columns_ = test.columns.to_list()
    column = None
    while columns_:
        column_ = columns_.pop(0)
        oxide_pattern = re.compile(r"As2O5|SiO2|FeO|Fe2O3|MgO|CaO|CuO|Al2O3|TiO2|MnO|ZnO|Na2O|K2O|PbO|SO3|Cr2O3|F|"
                                   r"Total")
        if test[column_].str.match(oxide_pattern, na=False).sum() > 4:
            test.rename(columns={column_: 'Oxide'}, inplace=True)
            break

    analyses = test[test[column_].str.match(oxide_pattern, na=False)]

    # 3 capture ID columns