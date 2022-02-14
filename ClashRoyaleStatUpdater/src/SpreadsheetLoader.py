import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import json

default_sheet_id = "1kZ9XdRK1cB3DGuFgKxGk6CrqjeJ3s5vn4QndPet2PDQ"
default_auth = '''{
            "type": "service_account",
            "project_id": "spreadsheet-reader-316917",
            "private_key_id": "646e3a4b1b0a70378689e0f55b23767374d7e331",
            "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDplOVBCQRu0gj3\\nEM1pww+podKa+vyUgP/OQ6A4D7i6n8xcEG0KGIzdFazj93cJmulOTTIssWjHr5CZ\\nMffLL3/eJfQ39PTqA1ps+lDvYaT8F8ZCJHycMn5oIt00DKMZYvFcHjfcdUaSx0uK\\nlsKbZ9F6SjOjjoi8dBhvR2whuI8k9GAqxYk/hZxbjYbAWxrV1RWBX7PD3ERfrl++\\nuE4/BzCGADOpbbKLYQYJ3Gz6yixmGUk7ocqogCplfCAfwWxtM6I2zdz4d8QjfQjs\\nfZpk7Na/ebENwqEZLkrsm0T44hgsSVuwUvPIg03CfvYbi9VifKVYp91rD4PY3Pn3\\nLP+rzuobAgMBAAECggEACivG5cx2OHQF4ZSw41gFrto14BfbaPbnet/f+YgSnI4y\\n4iXVLhzcx526nidGCYNP8xjZi4T/PEz0ZMSpE04u6QmCmtMqhBbG9RAwoekrgs5v\\nU601nBYFOvgrAY1fik5lejzlD0D28swFXwJ5Av3neUXmqEqfh/TIM2C1vLpiVxLp\\nUaRowX1X6BB36gHjP30f7Fs6J5tGaV1tgc6KJQg5OHs5Fz8taJBH88puOn0+ddam\\nC/LdFM9j23jmoP5+ya+cHY9jqCsdPf10wCBbNsmWDwhgrY1IpQRA30ZdL9KHOlOK\\n8/JDlfi7BMo75t/YV5dt4MeCTzWyq79i2kbZCNDhQQKBgQD7ZBp6b67mT2gVUPe8\\nuNh4We9gYVrbKLiqgeZRaN817wTPOczYGZKrLJhfBRQwdhFHZueblnBG3ja9Hlhd\\n6TaMoTShkK4xY/qIHhTrePpksNGVUCdhjcMvKu1dwzLD6DuH3GbWPzxOc3Z3sPXv\\n0j8s35Ql2zRZLPdHzh8609n1wQKBgQDt3TRDtqMRpFnfwKGJcCMTi2tLzzjkEkTY\\nqnkxD0vxnZhy1PGYkVdWMrUrOgAoIEYsui/aNOvC9P/5WsSK/P+E7QhvT5jOK31f\\njB7afJQK1AiayubKBVmQzqaXKLZ8fiKdUQy3VVkmIp7L2qC7XeUYufsBMSyd4/vz\\nB3SQzHEu2wKBgFqWd/Qmyp+zfY4w3xTihx7XSasxacwPoHvQ024CnGyS9Oi3q1kz\\n3eZQ65dR/TR5V0CjlFI8o7jl2lPL24v7vexvKsgNmrexj1X9gQxZS+F81gk4GPjO\\nXMdicKaY8HIn/Uu34FbT8qdSdB4tZnJFEP7akkgR9Yss6O63GAnazXABAoGAfcsl\\nT8Yv2S2kxhtWkpu2QSjTVqVBfgRXWopVS2e+jwn8TIZnOntqx9BLVY/380CWPCM/\\nGVQxdsog8VtaY/LGoyLD+jILKsV/KV+uFXgaxts5rbsucJqBsn5HRdunHpGKds4d\\nPnupiOx4NHDr3gsbFIeOwiOiZ+HhJbbEo6srFE8CgYEA5wxuAsY+J4lIWIKDHYKP\\njP2zt/dzpNld5p3zeTUQiivGWRC7PyQWrbrTHbWhTCVPs+bJUJsXka761ZuGfdds\\nYGhTZ+2WnJFmx/jQ8fF5aBdpL+T5aJpOIBRiOEJCDlXbKclSwz8se3rFrwdaxLFr\\n93puLJVXgutoAhOTYI7FVoM=\\n-----END PRIVATE KEY-----\\n",
            "client_email": "python-spreadsheet-reader@spreadsheet-reader-316917.iam.gserviceaccount.com",
            "client_id": "114261222231092187519",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/python-spreadsheet-reader%40spreadsheet-reader-316917.iam.gserviceaccount.com"
        }'''


class SpreadsheetLoaderSettings:
    def __init__(self, sheet_id=default_sheet_id, auth=default_auth):
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
