import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import json

default_sheet_id = "1kZ9XdRK1cB3DGuFgKxGk6CrqjeJ3s5vn4QndPet2PDQ"


class SpreadsheetLoaderSettings:
    def __init__(self, auth: str, sheet_id=default_sheet_id):
        self.sheet_id = sheet_id
        self.auth = auth


class SpreadsheetLoader:
    def __init__(self, settings: SpreadsheetLoaderSettings):
        self.settings = settings
        self.gc = None

    def get_gc(self):
        if self.gc is None:
            # Read the .json file and authenticate with the links
            info = json.loads(self.settings.auth)
            credentials = Credentials.from_service_account_info(info, scopes=gspread.auth.DEFAULT_SCOPES)
            # Request authorization and open the selected spreadsheet
            self.gc = gspread.authorize(credentials).open_by_key(self.settings.sheet_id)
        return self.gc

    def get(self, index=-1, first_column="A", last_column="I"):
        gc = self.get_gc()
        # Prompts for all spreadsheet values
        values = gc.get_worksheet(index).get_values(f"{first_column}:{last_column}")
        df = pd.DataFrame(values)
        # Turns the return into a dataframe
        df.columns = df.iloc[0]
        df.drop(df.index[0], inplace=True)

        return df

    def next_available_row(self, spreadsheet_index: int):
        str_list = list(filter(None, self.get_gc().get_worksheet(index=spreadsheet_index).col_values(1)))
        return str(len(str_list) + 1)

    def next_available_col(self, spreadsheet_index: int, penultimate=False):
        str_list = list(filter(None, self.get_gc().get_worksheet(index=spreadsheet_index).row_values(1)))
        penultimate_offset = 1 if penultimate else 0
        return chr(ord("A") + len(str_list) - penultimate_offset)

    def next_available_col_index(self, spreadsheet_index: int, penultimate=False):
        str_list = list(filter(None, self.get_gc().get_worksheet(index=spreadsheet_index).row_values(1)))
        penultimate_offset = 1 if penultimate else 0
        return len(str_list) - penultimate_offset + 1

    @staticmethod
    def change_color(sheet_id, start_row, end_row, start_col, end_col, r=1, g=1, b=1):
        return {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row, "endRowIndex": end_row,
                    "startColumnIndex": start_col, "endColumnIndex": end_col
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {
                        "red": r,
                        "green": g,
                        "blue": b
                    }
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        }
