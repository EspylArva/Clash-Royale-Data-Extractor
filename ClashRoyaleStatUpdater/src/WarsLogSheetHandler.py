from datetime import datetime
from enum import Enum
from itertools import groupby
from operator import itemgetter

import numpy as np
import pandas as pd
from gspread.exceptions import APIError
from pandas import DataFrame

from src.ClashRoyaleAPI import ApiConnectionManager, DataExtractor, Role
from src.SpreadsheetLoader import SpreadsheetLoader


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

    def _clear_colors(self, sheet_index: int, df: DataFrame):
        row_count, col_count = df.shape

        sheet_id = self.sheet_accessor.get_gc().get_worksheet(sheet_index).id
        clear_format_request = {
            "repeatCell": {
                "range" : {
                    "sheetId"         : sheet_id,
                    "startRowIndex"   : 0, "endRowIndex": row_count,
                    "startColumnIndex": 0, "endColumnIndex": col_count
                },
                "cell"  : { "userEnteredFormat": {
                    "backgroundColor": {
                        "alpha": 0,
                        "red"  : 1,
                        "green": 1,
                        "blue" : 1
                    }
                } },
                "fields": "userEnteredFormat.backgroundColor"
            }
        }
        body = {
            "requests": [clear_format_request]
        }
        self.sheet_accessor.get_gc().batch_update(body)

    def _fetch_boat_results(self, df: DataFrame):
        boats_log = self.get_wars_log()
        boats_df = DataFrame(columns=[ColumnIndex.TAG.value])
        for war in boats_log:
            participants = [x["tag"] for x in war["war_record"]["clan"]["participants"]]
            boat_attacks = [x["boatAttacks"] for x in war["war_record"]["clan"]["participants"]]
            boat_df = DataFrame({ ColumnIndex.TAG.value: participants, war["id"]: boat_attacks })
            boats_df = boats_df.merge(boat_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")
        return df.merge(boats_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")

    def _fetch_members_from_war_logs(self, df: DataFrame):
        wars_log = self.get_wars_log()
        participants_tag = dict()
        for record in wars_log:
            participant = record["war_record"]["clan"]["participants"]
            participants_tag = participants_tag | { x["tag"]: { ColumnIndex.NAME.value: x["name"] } for x in
                                                    participant }

        for (key, value) in participants_tag.items():
            if key not in df.values:
                df = pd.concat([df, pd.DataFrame({
                    ColumnIndex.NAME.value: [value[ColumnIndex.NAME.value]],
                    ColumnIndex.TAG.value : [key]
                })], ignore_index=True).sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value])
        return df

    def _fetch_war_results(self, df: DataFrame):
        wars_log = self.get_wars_log()
        wars_df = DataFrame(columns=[ColumnIndex.TAG.value])
        for war in wars_log:
            participants = [x["tag"] for x in war["war_record"]["clan"]["participants"]]
            fame = [x["fame"] for x in war["war_record"]["clan"]["participants"]]
            war_df = DataFrame({ ColumnIndex.TAG.value: participants, war["id"]: fame })
            wars_df = wars_df.merge(war_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")
        return df.merge(wars_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")

    def _get_previous_wars_data(self):
        previous_war_logs = self.sheet_accessor.get(index=1, first_column="F",
                                                    last_column=self.sheet_accessor.next_available_col(1, True))
        return previous_war_logs

    def _hide_non_members(self, list_row_indexes: list, sheet_index: int):
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

    def _highlight_zeroes(self, sheet_index: int, df: DataFrame):
        self._clear_colors(sheet_index, df)
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(sheet_index).id
        body = { "requests": [] }

        r, g, b = (0.35, 0.35, 0.35)
        if sheet_index == 1:
            r = 0.8
        elif sheet_index == 2:
            g = 0.8

        for i, col in df.items():
            col_index = df.columns.get_loc(i)
            if col_index > 4:
                zeroes = []
                if sheet_index == 1:
                    zeroes = col.where(col == 0).dropna().keys()
                elif sheet_index == 2:
                    zeroes = col.where((col != 0) & (col != "")).dropna().keys()
                for k, gr in groupby(enumerate(zeroes), lambda ix: ix[0] - ix[1]):
                    indexes = list(map(itemgetter(1), gr))
                    request = SpreadsheetLoader.change_color(sheet_id=sheet_id,
                                                             start_row=int(indexes[0] + 1),
                                                             end_row=int(indexes[-1]) + 2,
                                                             start_col=col_index, end_col=col_index + 1, r=r, g=g, b=b)
                    body["requests"].append(request)
        for role in list(Role):
            raw_indexes = df.index[df[ColumnIndex.ROLE.value] == role.value].tolist()
            for k, gr in groupby(enumerate(raw_indexes), lambda ix: ix[0] - ix[1]):
                indexes = list(map(itemgetter(1), gr))
                role_request = SpreadsheetLoader.change_color(sheet_id=sheet_id,
                                                              start_row=int(indexes[0] + 1),
                                                              end_row=int(indexes[-1]) + 2,
                                                              start_col=4, end_col=5, r=role.r, g=role.g, b=role.b)
                body["requests"].append(role_request)
        header_request = SpreadsheetLoader.change_color(sheet_id=sheet_id, start_row=0, end_row=1,
                                                        start_col=0, end_col=df.shape[1], r=0.4, g=0.2, b=0.5)
        body["requests"].append(header_request)
        self.sheet_accessor.get_gc().batch_update(body)

    def _update_members_role(self, df: DataFrame):
        df[ColumnIndex.ROLE.value] = ""
        for member in self.members_data["items"]:
            role = Role.get_french_function(member["role"]).value
            df.loc[df[df["Tag"] == member["tag"]].index, "Grade"] = role
        return df

    def get_boat_sheet_update(self):
        self.sheet_accessor.get_gc().get_worksheet(2).update(f'A1:E1', [ColumnIndex.ordered_col_indexes()])
        df = self.sheet_accessor.get(index=2, last_column="E")
        df = self._fetch_members_from_war_logs(df=df)
        df = df.sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value], key=lambda col: col.str.lower())
        df = self._update_members_role(df=df)
        df = self._fetch_boat_results(df=df)

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
        df = self._fetch_members_from_war_logs(df=df)
        df = df.sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value], key=lambda col: col.str.lower())
        df = self._update_members_role(df=df)
        df = self._fetch_war_results(df=df)

        for i, row in df.iterrows():
            tag = row[ColumnIndex.TAG.value]
            df.at[i, ColumnIndex.TAG.value] = f'=HYPERLINK("https://royaleapi.com/player/{tag[1:]}";"{tag}")'
            # noinspection PyTypeChecker
            df.at[i, ColumnIndex.SCORE] = f'=SUM($F{i + 2}:$O{i + 2})'
            # noinspection PyTypeChecker
            df.at[i, ColumnIndex.AVERAGE] = f'=IFERROR(ROUND(C{i + 2}/COUNT($F{i + 2}:$O{i + 2});0);0)'
        df = df.fillna("")
        return df

    def update_boat_results(self):
        try:
            df = self.get_boat_sheet_update()

            self._clear_colors(0)

            self._insert_alpha_members(df=df, sheet_id=self.sheet_accessor.get_gc().get_worksheet(2).id)
            self._insert_missing_data(df=df, sheet_id=self.sheet_accessor.get_gc().get_worksheet(2).id)
            self._insert_members_data(df=df, sheet_id=self.sheet_accessor.get_gc().get_worksheet(2).id)
            self._highlight_zeroes(2, df)
            self._hide_non_members(df.index[(df[ColumnIndex.ROLE.value] == "")].tolist(), sheet_index=2)  # & (df[ColumnIndex.ROLE.value] != "Total")

            return f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : Updated Boat Attacks (Sheet #3)'
        except APIError:
            return "Error. Try in a few minutes."

    def update_war_results(self):
        try:
            df = self.get_war_sheet_update()

            self._clear_colors(1)

            self._insert_alpha_members(df=df, sheet_id=self.sheet_accessor.get_gc().get_worksheet(1).id)
            self._insert_missing_data(df=df, sheet_id=self.sheet_accessor.get_gc().get_worksheet(1).id)
            self._insert_members_data(df=df, sheet_id=self.sheet_accessor.get_gc().get_worksheet(1).id)
            self._highlight_zeroes(sheet_index=1, df=df)
            self._hide_non_members(df.index[df[ColumnIndex.ROLE.value] == ""].tolist(), sheet_index=1)

            return f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : Updated War Logs (Sheet #2)'
        except APIError:
            # time.sleep(60)
            # self.update_war_results()
            return "Error. Try in a few minutes."

    def _insert_alpha_members(self, df: DataFrame, sheet_id: int):
        _df = df.filter(regex="[a-zA-Z]")
        tags = self.sheet_accessor.get_gc().get_worksheet_by_id(sheet_id).get_values("B2:B")
        for i, val in _df.iterrows():
            if val[1].split(";")[1][1:-2] not in [tag[0] for tag in tags]:
                # noinspection PyTypeChecker
                body = {
                    'requests': [{
                        "insertDimension": {
                            "range"            : {
                                "sheetId"   : sheet_id,
                                "dimension" : "ROWS",
                                "startIndex": i + 1,
                                "endIndex"  : i + 2
                            },
                            "inheritFromBefore": False
                        }
                    }]
                }
                self.sheet_accessor.get_gc().batch_update(body)

    def _insert_members_data(self, df: DataFrame, sheet_id: int):
        _df = df.filter(regex="[a-zA-Z]")
        _range = f'A1:E'
        _values = [_df.columns.tolist()] + _df.values.tolist()

        self.sheet_accessor.get_gc().get_worksheet_by_id(sheet_id).update(_range, _values, value_input_option='USER_ENTERED')

    def _insert_missing_data(self, df: DataFrame, sheet_id: int):
        _df = df.copy(deep=True)
        for col in _df.columns[4:]:
            _df[col] = np.where((df[ColumnIndex.ROLE] != "") & (df[col] == ""), 0, df[col])
        _df = _df.filter(regex="[0-9]+:[0-9]+")
        history = self.sheet_accessor.get_gc().get_worksheet_by_id(sheet_id).get_values("F1:1")[0]
        _df = _df.drop(columns=history, errors='ignore')

        for col in _df.columns:
            df[col] = _df[col]

        if _df.shape[1] > 0:

            body = { 'requests': [] }
            body["requests"].append({
                "insertDimension": {
                    "range"            : {
                        "sheetId"   : sheet_id,
                        "dimension" : "COLUMNS",
                        "startIndex": 5,
                        "endIndex"  : 5 + _df.shape[1]
                    },
                    "inheritFromBefore": False
                }
            })
            self.sheet_accessor.get_gc().batch_update(body)

            _range = f'F1:{chr(ord("F") + _df.shape[1] - 1)}'
            _values = [_df.columns.tolist()] + _df.values.tolist()

            self.sheet_accessor.get_gc().get_worksheet_by_id(sheet_id).update(_range, _values, value_input_option='USER_ENTERED')
