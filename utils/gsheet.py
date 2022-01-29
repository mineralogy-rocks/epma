# -*- coding: utf-8 -*-
from asyncio import gather, run

from gspread_asyncio import AsyncioGspreadClientManager
from google.oauth2.credentials import Credentials


class GoogleSheet:
    def __init__(self):
        self.agcm = AsyncioGspreadClientManager
        self.sheet_mapping = [
            {'ws_name': 'Masterlist2', 'ss_name': 'Status data', 'local_name': 'status_data'},
            {'ws_name': 'Groups', 'ss_name': 'Groups_ver1', 'local_name': 'groups_formulas'},
        ]
        self.status_data = None
        self.groups_formulas = None


    def get_local_name(self, ss_name: str):
        '''
        A function which returns local_name from self.sheet_mappings
        '''
        try:
            return [sheet['local_name'] for sheet in self.sheet_mapping if sheet['ss_name'] == ss_name]
        except:
            print(f'ss_name="{ss_name}" is not present in gsheets_api!')


    def get_creds(self):
        creds = Credentials.from_service_account_file('functions/.env.json')
        scoped = creds.with_scopes([
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        return scoped


    async def get_sheet_data(self, worksheet_name: str, sheet_name: str):
        agc = await self.agcm(self.get_creds).authorize()
        try:
            print(f'started grabbing data of {sheet_name}')
            ags = await agc.open(worksheet_name)
            agw = await ags.worksheet(sheet_name)
            data = await agw.get_all_values()
            print(f'Processed {sheet_name}')
            headers = data.pop(0)
            output = pd.DataFrame(data, columns=headers).replace(r'', np.nan)
            local_var = self.get_local_name(ss_name=sheet_name)[0]
            setattr(self, local_var, output)
            return output
        except Exception as error:
            print(error)
            print(f'An error occurred while reading sheet_name={sheet_name}')


    async def main(self):
        await gather(*(self.get_sheet_data(sheet['ws_name'], sheet['ss_name']) for sheet in self.sheet_mapping))


    def run_main(self):
        import time
        s = time.perf_counter()
        run(self.main())
        elapsed = time.perf_counter() - s
        print(f"Executed in {elapsed:0.2f} seconds.")
