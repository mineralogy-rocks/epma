# -*- coding: utf-8 -*-
import io
import re

from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import numpy as np

from utils.gdrive import DriveApi
from utils.gsheet import GoogleSheet
import core.constants as constants
import core.patterns as patterns


DriveApi = DriveApi()
GoogleSheet = GoogleSheet()

DriveApi.authorize()
GoogleSheet.run_main()

use_cols = ['SAMPLE NAME', 'MINERAL', 'SPOT', 'RIM/CORE (MINERAL GRAINS)']

clinopyroxenes_ = pd.read_csv('./data/GEOROC_CLINOPYROXENES.csv',
                              encoding_errors='ignore',
                              usecols=[*use_cols, *['SIO2(WT%)', 'TIO2(WT%)', 'ZRO2(WT%)', 'HF2O3(WT%)', 'AL2O3(WT%)', 'CR2O3(WT%)',
                                       'CE2O3(WT%)', 'Y2O3(WT%)', 'V2O3(WT%)', 'NB2O5(WT%)', 'FE2O3T(WT%)', 'FE2O3(WT%)',
                                       'FEOT(WT%)', 'FEO(WT%)', 'FET(WT%)', 'CAO(WT%)', 'MGO(WT%)', 'MNO(WT%)', 'BAO(WT%)',
                                       'SRO(WT%)', 'PBO(WT%)', 'NIO(WT%)', 'ZNO(WT%)', 'COO(WT%)', 'CUO(WT%)', 'CS2O(WT%)',
                                       'RB2O(WT%)', 'K2O(WT%)', 'NA2O(WT%)', 'P2O5(WT%)', 'H2O(WT%)', 'H2OP(WT%)',
                                       'H2OM(WT%)', 'F(WT%)', 'CL(WT%)', 'CL2O(WT%)', 'SO2(WT%)', 'SO3(WT%)', 'S(WT%)',
                                       'LOI(WT%)', 'O(WT%)', 'SI(WT%)', 'AL(WT%)', 'TI(WT%)', 'FE(WT%)', 'MG(WT%)', 'MN(WT%)',
                                       'CA(WT%)', 'Y(WT%)', 'K(WT%)', 'NA(WT%)', 'CR(WT%)', 'NI(WT%)']])

orthopyroxenes_ = pd.read_csv('./data/GEOROC_ORTHOPYROXENES.csv',
                              encoding_errors='ignore',
                              use_cols=[*use_cols, *['SIO2(WT%)', 'TIO2(WT%)', 'ZRO2(WT%)', 'AL2O3(WT%)', 'CR2O3(WT%)', 'V2O3(WT%)',
                                        'V2O5(WT%)', 'NB2O5(WT%)', 'FE2O3T(WT%)', 'FE2O3(WT%)', 'FEOT(WT%)', 'FEO(WT%)',
                                        'FET(WT%)', 'CAO(WT%)', 'MGO(WT%)', 'MNO(WT%)', 'BAO(WT%)', 'SRO(WT%)', 'NIO(WT%)',
                                        'ZNO(WT%)', 'COO(WT%)', 'K2O(WT%)', 'NA2O(WT%)', 'P2O5(WT%)', 'H2O(WT%)', 'H2OP(WT%)',
                                        'H2OM(WT%)', 'F(WT%)', 'CL(WT%)', 'SO2(WT%)', 'SO3(WT%)', 'LOI(WT%)', 'TI(WT%)',
                                        'FE(WT%)', 'MG(WT%)', 'MN(WT%)', 'CA(WT%)', 'K(WT%)', 'NA(WT%)', 'CR(WT%)']])

pyroxenes_ = pd.read_csv('./data/GEOROC_PYROXENES.csv',
                         encoding_errors='ignore',
                         usecols=[*use_cols, *['SIO2(WT%)', 'TIO2(WT%)', 'ZRO2(WT%)', 'AL2O3(WT%)', 'CR2O3(WT%)', 'SC2O3(WT%)',
                                   'CE2O3(WT%)', 'V2O3(WT%)', 'V2O5(WT%)', 'FE2O3T(WT%)', 'FE2O3(WT%)', 'FEOT(WT%)',
                                   'FEO(WT%)', 'CAO(WT%)', 'MGO(WT%)', 'MNO(WT%)', 'BAO(WT%)', 'SRO(WT%)',
                                   'NIO(WT%)', 'ZNO(WT%)', 'K2O(WT%)', 'NA2O(WT%)', 'LI2O(WT%)', 'P2O5(WT%)',
                                   'H2O(WT%)', 'H2OP(WT%)', 'H2OM(WT%)', 'CO2(WT%)', 'CO2(PPB)', 'F(WT%)', 'CL(WT%)',
                                   'SO2(WT%)', 'SO3(WT%)', 'S(WT%)', 'LOI(WT%)', 'O(WT%)', 'SI(WT%)', 'AL(WT%)',
                                   'TI(WT%)', 'FE(WT%)', 'MG(WT%)', 'MN(WT%)', 'CA(WT%)', 'K(WT%)', 'NA(WT%)',
                                   'CR(WT%)', 'NI(WT%)']])

def get_pyroxene_group():
    pyroxene_group = GoogleSheet._get_children(specie_name='pyroxene group', status_in=[constants.IMA_SPECIE],
                                               taxonomy_level='group')
    sheets_meta = DriveApi.get_sheets_meta(parent_folder='RRUFF', mineral_list=pyroxene_group)

    output = pd.DataFrame()

    for spreadsheet_ in sheets_meta:
        spreadsheet_ = sheets_meta[2] # TODO: remove after testing
        file_id = spreadsheet_['id']
        regex_pattern = re.match('(^[A-Za-z0-9-]+)__(\w+-\d+)__', spreadsheet_['name'])
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
            sheet_ = data_[sheet].copy()
            # sheet_ = data_['pdf_output'].copy()

            # 0 skip non-meaningful rows
            sheet_.set_index(0, inplace=True)
            sheet_ = sheet_[np.logical_or(sheet_.index.str.match(patterns.oxide_pattern, na=False),
                                          sheet_.index.str.match(patterns.cation_pattern, na=False))]

            # 1 skip Standard Deviation and Average columns
            if len(sheet_.columns[sheet_.isna().all()]):
                sheet_ = sheet_.loc[:, sheet_.columns < sheet_.columns[sheet_.isna().all()].min()]

            # 2 Transpose dataset
            transpose_ = sheet_.T
            transpose_.reset_index(drop=True)
            transpose_['rruff_id'] = rruff_id + '__' + transpose_.index.astype(str)
            transpose_['mineral_name'] = mineral_name

            output = pd.concat([output, transpose_.reset_index(drop=True)])

