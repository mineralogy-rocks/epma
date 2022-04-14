# -*- coding: utf-8 -*-
import io
import re

from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import numpy as np

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
    output = pd.DataFrame(columns=['rruff_id', 'mineral_name', 'analysis', 'SiO2', 'FeO', 'Fe2O3', 'MgO', 'CaO', 'Al2O3',
                                   'TiO2', 'MnO', 'Na2O', 'K2O', 'Cr2O3', 'F', 'Total', ])

    # TODO: add regex pattern to match column headers of oxide names

    oxide_pattern = re.compile(r"^"
                               r"(BaO|SiO2|FeO|Fe2O3|MgO|CaO|Al2O3|TiO2|MnO|ZnO|Na2O|K2O|CO2|Cr2O3|CuO|F)"
                               r"$")
    cation_pattern = re.compile(r"^"
                               r"(Si|Al|Mg|Fe|Ca|Ti|Mn|Na|Cr|Cu)"
                               r"$")

    for spreasdheet in sheets_meta:
        spreadsheet = sheets_meta[0] # TODO: remove after testing
        file_id = spreadsheet['id']
        regex_pattern = re.match('(^[A-Za-z0-9-]+)__(\w+-\d+)__', spreadsheet['name'])
        mineral_name, rruff_id = regex_pattern.group(1), regex_pattern.group(2)

        # NOTE: Augite - 1v7J6USE6njnCFP6IN3VaziTiNaMNvRP5

        request = DriveApi.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print ("Download %d%%." % int(status.progress() * 100))
        data_ = pd.read_excel(fh, usecols=None, sheet_name=None, header=None)

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


    test = data_['pdf_output']  # or Sheet1

    # 0 skip non-meaningful rows
    test.set_index(0, inplace=True)
    test = test[np.logical_or(test.index.str.match(oxide_pattern, na=False),
                              test.index.str.match(cation_pattern, na=False))]

    # 1 skip Standard Deviation and Average columns
    test = test.loc[:, test.columns < test.columns[test.isna().all()].min()]

    # 2 Transpose dataset
    transpose_ = test.T
    transpose_.reset_index(drop=True)
    transpose_['rruff_id'] = rruff_id + '__' + transpose_.index.astype(str)
    transpose_['mineral_name'] = mineral_name

