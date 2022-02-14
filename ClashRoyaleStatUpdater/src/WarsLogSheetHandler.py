import time
from datetime import datetime
from enum import Enum
from itertools import groupby
from operator import itemgetter

from gspread.exceptions import APIError
from pandas import DataFrame

import ClashRoyaleAPI
import pandas as pd

from ClashRoyaleAPI import Role, ApiConnectionManager
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


class WarLogsManager(ClashRoyaleAPI.DataExtractor):
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        super().__init__(api_connection_manager, accessor)

    def __fetch_members_from_war_logs(self, df: DataFrame):
        wars_log = self._get_wars_log()
        participants_tag = dict()
        for record in wars_log:
            participant = record["war_record"]["clan"]["participants"]
            participants_tag = participants_tag | {x["tag"]: {ColumnIndex.NAME.value: x["name"]} for x in participant}

        for (key, value) in participants_tag.items():
            if key not in df.values:
                df = pd.concat([df, pd.DataFrame({
                    ColumnIndex.NAME.value: [value[ColumnIndex.NAME.value]],
                    ColumnIndex.TAG.value: [key]
                })], ignore_index=True).sort_values(by=[ColumnIndex.NAME.value, ColumnIndex.TAG.value])
        return df

    def __update_members_role(self, df: DataFrame):
        for member in self.members_data["items"]:
            role = Role.get_french_function(member["role"]).value
            df.loc[df[df["Tag"] == member["tag"]].index, "Grade"] = role
        return df

    def __get_previous_wars_data(self):
        previous_war_logs = self.sheet_accessor.get(index=1, first_column="F",
                                                    last_column=self.sheet_accessor.next_available_col(1,
                                                                                                       penultimate=True))
        return previous_war_logs

    def __fetch_war_results(self, df: DataFrame):
        wars_log = self._get_wars_log()
        wars_df = DataFrame(columns=[ColumnIndex.TAG.value])
        for war in wars_log:
            participants = [x["tag"] for x in war["war_record"]["clan"]["participants"]]
            fame = [x["fame"] for x in war["war_record"]["clan"]["participants"]]
            war_df = DataFrame({ColumnIndex.TAG.value: participants, war["id"]: fame})
            wars_df = wars_df.merge(war_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")
        return df.merge(wars_df.set_index(ColumnIndex.TAG.value), on=ColumnIndex.TAG.value, how="outer")

    def __fetch_boat_results(self, df: DataFrame):
        boats_log = self._get_wars_log()
        boats_df = DataFrame(columns=[ColumnIndex.TAG.value])
        for war in boats_log:
            participants = [x["tag"] for x in war["war_record"]["clan"]["participants"]]
            boat_attacks = [x["boatAttacks"] for x in war["war_record"]["clan"]["participants"]]
            boat_df = DataFrame({ColumnIndex.TAG.value: participants, war["id"]: boat_attacks})
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
            # noinspection PyTypeChecker
            total = f'=SUM($F{i + 2}:$O{i + 2}) * 60'
            # noinspection PyTypeChecker
            moyenne = f'=IFERROR(ROUND(C{i + 2}/COUNT($F{i + 2}:$O{i + 2});0);0)'
            df.at[i, 'Total'] = total
            df.at[i, 'Moyenne'] = moyenne
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
            # noinspection PyTypeChecker
            total = f'=SUM($F{i + 2}:$O{i + 2})'
            # noinspection PyTypeChecker
            moyenne = f'=IFERROR(ROUND(C{i + 2}/COUNT($F{i + 2}:$O{i + 2});0);0)'
            df.at[i, 'Total'] = total
            df.at[i, 'Moyenne'] = moyenne
        df = df.fillna("")
        return df

    def __hide_non_members(self, list_row_indexes: list, sheet_index: int):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(sheet_index).id
        body = {"requests": [
            {
                "updateDimensionProperties": {
                    "properties": {
                        "hiddenByUser": False
                    },
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": 0,
                        "endIndex": self.sheet_accessor.next_available_row(1)
                    },
                    "fields": "hiddenByUser"
                }
            }
        ]}

        for k, g in groupby(enumerate(list_row_indexes), lambda ix: ix[0] - ix[1]):
            indexes = list(map(itemgetter(1), g))
            request = {
                "updateDimensionProperties": {
                    "properties": {
                        "hiddenByUser": True
                    },
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": int(indexes[0]) + 1,
                        "endIndex": int(indexes[-1]) + 2
                    },
                    "fields": "hiddenByUser"
                }
            }
            body["requests"].append(request)
        self.sheet_accessor.get_gc().batch_update(body)

    def _clear_colors(self, sheet_index: int, df: DataFrame):
        row_count, col_count = df.shape

        sheet_id = self.sheet_accessor.get_gc().get_worksheet(sheet_index).id
        clear_format_request = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": row_count,
                    "startColumnIndex": 0, "endColumnIndex": col_count
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {
                        "alpha": 0,
                        "red": 1,
                        "green": 1,
                        "blue": 1
                    }
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        }
        body = {
            "requests": [clear_format_request]
        }
        self.sheet_accessor.get_gc().batch_update(body)

    def highlight_zeroes(self, sheet_index: int, df: DataFrame):
        self._clear_colors(sheet_index, df)
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(sheet_index).id
        body = {"requests": []}

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
                                                             start_row=int(indexes[0]+1), end_row=int(indexes[-1]) + 2,
                                                             start_col=col_index, end_col=col_index+1, r=r, g=g, b=b)
                    body["requests"].append(request)
        for role in list(Role):
            raw_indexes = df.index[df[ColumnIndex.ROLE.value] == role.value].tolist()
            for k, gr in groupby(enumerate(raw_indexes), lambda ix: ix[0] - ix[1]):
                indexes = list(map(itemgetter(1), gr))
                role_request = SpreadsheetLoader.change_color(sheet_id=sheet_id,
                                                              start_row=int(indexes[0]+1), end_row=int(indexes[-1]) + 2,
                                                              start_col=4, end_col=5, r=role.r, g=role.g, b=role.b)
                body["requests"].append(role_request)
        header_request = SpreadsheetLoader.change_color(sheet_id=sheet_id, start_row=0, end_row=1,
                                                        start_col=0, end_col=df.shape[1], r=0.4, g=0.2, b=0.5)
        body["requests"].append(header_request)
        self.sheet_accessor.get_gc().batch_update(body)

    def update_war_results(self):
        try:
            df = self.get_war_sheet_update()
            _range = f'A2:{chr(ord("A") + df.shape[1] - 1)}'
            _values = df.values.tolist()

            self.highlight_zeroes(1, df)

            self.sheet_accessor.get_gc().get_worksheet(1).update(f'A1:{chr(ord("A") + df.shape[1] - 1)}1',
                                                                 [df.keys().tolist()])
            self.sheet_accessor.get_gc().get_worksheet(1).update(_range, _values, value_input_option='USER_ENTERED')
            self.__hide_non_members(df.index[df['Grade'] == ""].tolist(), sheet_index=1)

            return f'{datetime.now()} : Updated War Logs (Sheet #2)'
        except APIError:
            # time.sleep(60)
            # self.update_war_results()
            return "Error. Try in a few minutes."

    def test(self):
        df = self.get_boat_sheet_update()

        total_row = ["", "Total", "=SUM(C3:C)", "=SUM(D3:D)", ""]
        for i in range(5, df.shape[1]):
            total_row.append(f'=SUM({chr(ord("A") + i)}3:{chr(ord("A") + i)})')
        print(total_row)

    def update_boat_results(self):
        try:
            df = self.get_boat_sheet_update()

            # total_row = ["", "Total", "=SUM(C3:C)", "=SUM(D3:D)", ""]
            # for i in range(5, df.shape[1]):
            #     total_row.append(f'=SUM({chr(ord("A") + i)}3:{chr(ord("A") + i)})')
            # df.loc[-1] = total_row  # adding a row
            # df.index = df.index + 1  # shifting index
            # df = df.sort_index()  # sorting by index

            _range = f'A2:{chr(ord("A") + df.shape[1] - 1)}'  # Fixme: if inserting total, will need to shift A2 to A3
            _values = df.values.tolist()

            self.highlight_zeroes(2, df)

            self.sheet_accessor.get_gc().get_worksheet(2).update(f'A1:{chr(ord("A") + df.shape[1] - 1)}1',
                                                                 [df.keys().tolist()])
            self.sheet_accessor.get_gc().get_worksheet(2).update(_range, _values, value_input_option='USER_ENTERED')

            self.__hide_non_members(df.index[(df[ColumnIndex.ROLE.value] == "")].tolist(), sheet_index=2)  #  & (df[ColumnIndex.ROLE.value] != "Total")

            return f'{datetime.now()} : Updated Boat Attacks (Sheet #3)'
        except APIError:
            return "Error. Try in a few minutes."
