# -*- coding: utf-8 -*-
from utils.gdrive import DriveApi
from utils.gsheet import GoogleSheet

DriveApi = DriveApi()
GoogleSheet = GoogleSheet()

DriveApi.authorize()


def get_clinopyroxene_subgroup():
    sheets_meta = DriveApi.get_sheets_meta(parent_folder='RRUFF', mineral_list=['Aegirine', 'Augite', ])

