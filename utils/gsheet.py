# -*- coding: utf-8 -*-
from asyncio import gather, run

import pandas as pd
import numpy as np
from gspread_asyncio import AsyncioGspreadClientManager
from google.oauth2.service_account import Credentials


class GoogleSheet:
    def __init__(self):
        self.agcm = AsyncioGspreadClientManager
        self.sheet_mapping = [
            {'ws_name': 'Masterlist2', 'ss_name': 'Status data', 'local_name': 'status'},
            {'ws_name': 'Groups', 'ss_name': 'Groups_ver1', 'local_name': 'groups'},
        ]
        self.status = None
        self.groups = None

    def _get_children(self, specie_name: str, status_in: list = [], taxonomy_level: str = 'group') -> np.ndarray:
        mineral_list = self.groups.loc[
            (self.groups[taxonomy_level].str.lower() == specie_name.lower()) & (~self.groups['mineral_name'].isna())
        ][['mineral_name']]

        mineral_list = mineral_list.drop_duplicates('mineral_name').set_index('mineral_name')
        status = self.status.set_index('Mineral_Name')
        mineral_list = mineral_list.join(status['all_indexes'].str.split(r'; +|;'), how='inner')
        mineral_list['all_indexes'] = mineral_list['all_indexes'].apply(lambda x: [float(x_) for x_ in x])

        if len(status_in):
            mineral_status_flat = mineral_list.explode('all_indexes')
            mineral_status_flat = mineral_status_flat.loc[mineral_status_flat['all_indexes'].isin(status_in)]
            mineral_list = mineral_list.loc[mineral_status_flat.index]

        return np.unique(mineral_list.index.to_numpy())

    def _get_local_name(self, ss_name: str):
        try:
            return [sheet['local_name'] for sheet in self.sheet_mapping if sheet['ss_name'] == ss_name]
        except Exception as exc:
            print(f'Problem with loading "{ss_name}" - {exc}')

    @staticmethod
    def _get_creds():
        creds = Credentials.from_service_account_file('.env-gsheet.json')
        scoped = creds.with_scopes([
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        return scoped

    async def get_sheet_data(self, worksheet_name: str, sheet_name: str):
        agc = await self.agcm(self._get_creds).authorize()
        try:
            print(f'started grabbing data of {sheet_name}')
            ags = await agc.open(worksheet_name)
            agw = await ags.worksheet(sheet_name)
            data = await agw.get_all_values()
            print(f'Processed {sheet_name}')

            headers = data.pop(0)
            output = pd.DataFrame(data, columns=headers).replace(r'', np.nan)  # TODO: lower headers
            local_var = self._get_local_name(ss_name=sheet_name)[0]
            setattr(self, local_var, output)
            return output

        except Exception as exc:
            print(f'An error occurred while reading sheet_name={sheet_name}: {exc}')

    async def main(self):
        await gather(*(self.get_sheet_data(sheet['ws_name'], sheet['ss_name']) for sheet in self.sheet_mapping))

    def run_main(self):
        import time
        s = time.perf_counter()
        run(self.main())
        elapsed = time.perf_counter() - s
        print(f"Executed in {elapsed:0.2f} seconds.")
