from datetime import datetime
from enum import Enum
from itertools import groupby
from operator import itemgetter

import pandas as pd
from gspread.exceptions import APIError
from pandas import DataFrame

from ClashRoyaleAPI import ApiConnectionManager, DataExtractor, Role
from SpreadsheetLoader import SpreadsheetLoader


class ColumnIndex(str, Enum):
    NAME = "Pseudo"
    TAG = "Tag"
    SCORE = "Total"
    AVERAGE = "Moyenne"
    ROLE = "Grade"

    @staticmethod
    def ordered_col_indexes():
        return [ColumnIndex.NAME.value,
                ColumnIndex.TAG.value,
                ColumnIndex.SCORE.value,
                ColumnIndex.AVERAGE.value,
                ColumnIndex.ROLE.value]


class WarLogsManager(DataExtractor):
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        super().__init__(api_connection_manager, accessor)

    def __fetch_members_from_war_logs(self, df: DataFrame):
        wars_log = self._get_wars_log()
        participants_tag = dict()
        for record in wars_log:
            participant = record["war_record"]["clan"]["participants"]
            participants_tag = participants_tag | { x["tag"]: { ColumnIndex.NAME.value: x["name"] } for x in participant }

        for (key, value) in participants_tag.items():
            if key not in df.values:
                df = pd.concat([df, pd.DataFrame({
                    ColumnIndex.NAME.value: [value[ColumnIndex.NAME.value]],
                    ColumnIndex.TAG.value : [key]
                })], ignore_index=True).sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value])
        return df

    def __update_members_role(self, df: DataFrame):
        df[ColumnIndex.ROLE.value] = ""
        for member in self.members_data["items"]:
            role = Role.get_french_function(member["role"]).value
            df.loc[df[df["Tag"] == member["tag"]].index, "Grade"] = role
        return df

    def __get_previous_wars_data(self):
        previous_war_logs = self.sheet_accessor.get(index=1, first_column="F",
                                                    last_column=self.sheet_accessor.next_available_col(1, True))
        return previous_war_logs

    def __fetch_war_results(self, df: DataFrame):
        wars_log = self._get_wars_log()
        wars_df = DataFrame(columns=[ColumnIndex.TAG.value])
        for war in wars_log:
            participants = [x["tag"] for x in war["war_record"]["clan"]["participants"]]
            fame = [x["fame"] for x in war["war_record"]["clan"]["participants"]]
            war_df = DataFrame({ ColumnIndex.TAG.value: participants, war["id"]: fame })
            wars_df = wars_df.merge(war_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")
        return df.merge(wars_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")

    def __fetch_boat_results(self, df: DataFrame):
        boats_log = self._get_wars_log()
        boats_df = DataFrame(columns=[ColumnIndex.TAG.value])
        for war in boats_log:
            participants = [x["tag"] for x in war["war_record"]["clan"]["participants"]]
            boat_attacks = [x["boatAttacks"] for x in war["war_record"]["clan"]["participants"]]
            boat_df = DataFrame({ ColumnIndex.TAG.value: participants, war["id"]: boat_attacks })
            boats_df = boats_df.merge(boat_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")
        return df.merge(boats_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")

    def get_boat_sheet_update(self):
        self.sheet_accessor.get_gc().get_worksheet(2).update(f'A1:E1', [ColumnIndex.ordered_col_indexes()])
        df = self.sheet_accessor.get(index=2, last_column="E")
        df = self.__fetch_members_from_war_logs(df=df)
        df = df.sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value], key=lambda col: col.str.lower())
        df = self.__update_members_role(df=df)
        df = self.__fetch_boat_results(df=df)

        for i, row in df.iterrows():
            tag = row[ColumnIndex.TAG.value]
            df.at[i, ColumnIndex.TAG.value] = f'=HYPERLINK("https://royaleapi.com/player/{tag[1:]}";"{tag}")'
            # noinspection PyTypeChecker
            df.at[i, ColumnIndex.SCORE] = f'=SUM($F{i + 2}:$O{i + 2}) * 60'
            # noinspection PyTypeChecker
            df.at[i, ColumnIndex.AVERAGE] = f'=IFERROR(ROUND(C{i + 2}/COUNT($F{i + 2}:$O{i + 2});0);0)'
        df = df.fillna("")
        return df

    def get_war_sheet_update(self):
        self.sheet_accessor.get_gc().get_worksheet(1).update(f'A1:E1', [ColumnIndex.ordered_col_indexes()])
        df = self.sheet_accessor.get(index=1, last_column="E")
        df = self.__fetch_members_from_war_logs(df=df)
        df = df.sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value], key=lambda col: col.str.lower())
        df = self.__update_members_role(df=df)
        df = self.__fetch_war_results(df=df)

        # FIXME: Merge previous wars

        for i, row in df.iterrows():
            tag = row[ColumnIndex.TAG.value]
            df.at[i, ColumnIndex.TAG.value] = f'=HYPERLINK("https://royaleapi.com/player/{tag[1:]}";"{tag}")'
            # noinspection PyTypeChecker
            df.at[i, ColumnIndex.SCORE] = f'=SUM($F{i + 2}:$O{i + 2})'
            # noinspection PyTypeChecker
            df.at[i, ColumnIndex.AVERAGE] = f'=IFERROR(ROUND(C{i + 2}/COUNT($F{i + 2}:$O{i + 2});0);0)'
        df = df.fillna("")
        return df

    def __hide_non_members(self, list_row_indexes: list, sheet_index: int):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(sheet_index).id
        body = { "requests": [
            {
                "updateDimensionProperties": {
                    "properties": {
                        "hiddenByUser": False
                    },
                    "range"     : {
                        "sheetId"   : sheet_id,
                        "dimension" : "ROWS",
                        "startIndex": 0,
                        "endIndex"  : self.sheet_accessor.next_available_row(1)
                    },
                    "fields"    : "hiddenByUser"
                }
            }
        ] }

        for k, g in groupby(enumerate(list_row_indexes), lambda ix: ix[0] - ix[1]):
            indexes = list(map(itemgetter(1), g))
            request = {
                "updateDimensionProperties": {
                    "properties": {
                        "hiddenByUser": True
                    },
                    "range"     : {
                        "sheetId"   : sheet_id,
                        "dimension" : "ROWS",
                        "startIndex": int(indexes[0]) + 1,
                        "endIndex"  : int(indexes[-1]) + 2
                    },
                    "fields"    : "hiddenByUser"
                }
            }
            body["requests"].append(request)
        self.sheet_accessor.get_gc().batch_update(body)

    def update_war_results(self):
        df = self.get_war_sheet_update()
        _range = f'A2:{chr(ord("A") + df.shape[1] - 1)}'
        _values = df.values.tolist()

        try:
            self.sheet_accessor.get_gc().get_worksheet(1).update(f'A1:{chr(ord("A") + df.shape[1] - 1)}1',
                                                                 [df.keys().tolist()])
            self.sheet_accessor.get_gc().get_worksheet(1).update(_range, _values, value_input_option='USER_ENTERED')
            self.__hide_non_members(df.index[df['Grade'] == ""].tolist(), sheet_index=1)
            return f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : Updated War Logs (Sheet #2)'
        except APIError:
            # time.sleep(60)
            # self.update_war_results()
            return "Error. Try in a few minutes."

    def update_boat_results(self):
        df = self.get_boat_sheet_update()
        _range = f'A2:{chr(ord("A") + df.shape[1] - 1)}'
        _values = df.values.tolist()

        try:
            self.sheet_accessor.get_gc().get_worksheet(2).update(f'A1:{chr(ord("A") + df.shape[1] - 1)}1',
                                                                 [df.keys().tolist()])
            self.sheet_accessor.get_gc().get_worksheet(2).update(_range, _values, value_input_option='USER_ENTERED')

            self.__hide_non_members(df.index[df['Grade'] == ""].tolist(), sheet_index=2)

            return f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : Updated Boat Attacks (Sheet #3)'
        except APIError:
            return "Error. Try in a few minutes."
