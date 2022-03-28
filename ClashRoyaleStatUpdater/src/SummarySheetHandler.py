from datetime import datetime
from enum import Enum
from itertools import groupby
from operator import itemgetter

import pandas as pd
from gspread.exceptions import APIError
from pandas import DataFrame

from src.ClashRoyaleAPI import ApiConnectionManager, DataExtractor, Role
from src.SpreadsheetLoader import SpreadsheetLoader


class ColumnIndex(str, Enum):
    RANK = "Rang"
    NAME = "Pseudo"
    ROLE = "Grade"
    POINTS = "Points de clan"
    CASTLE_LEVEL = "Niveau du château"
    RATIO = "Ratio Points/Niveau"
    AVERAGE = "Moyenne"
    TAG = "Tag"
    INACTIVITY = "Durée d'inactivité (en semaines)"

    @staticmethod
    def ordered_col_indexes():
        return [
            ColumnIndex.RANK.value,
            ColumnIndex.NAME.value,
            ColumnIndex.ROLE.value,
            ColumnIndex.POINTS.value,
            ColumnIndex.CASTLE_LEVEL.value,
            ColumnIndex.RATIO.value,
            ColumnIndex.AVERAGE.value,
            ColumnIndex.TAG.value,
            ColumnIndex.INACTIVITY.value]


class SummaryManager(DataExtractor):
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        super().__init__(api_connection_manager, accessor)

    def _build_summary(self):
        inactivities = self._get_inactivity()
        members = self.get_current_members()

        pseudos = list()
        grades = list()
        niveaux_chateau = list()
        tags = list()
        inactivity = list()

        for member in members:
            pseudos.append(member["name"])
            grades.append(member["role"].value)
            niveaux_chateau.append(member["castleLevel"])
            tags.append(member["tag"])
            inactivity.append(inactivities.get(member['tag'], '0'))
        df = DataFrame({
            ColumnIndex.NAME.value        : pseudos,
            ColumnIndex.ROLE.value        : grades,
            ColumnIndex.CASTLE_LEVEL.value: niveaux_chateau,
            ColumnIndex.RATIO.value       : None,
            ColumnIndex.AVERAGE.value     : None,
            ColumnIndex.TAG.value         : tags,
            ColumnIndex.INACTIVITY.value  : inactivity
        })
        df = self._import_war_results(df=df)
        df = df.rename(columns={ "Total": ColumnIndex.POINTS.value })
        return df

    @staticmethod
    def _check_role(df: DataFrame, role: Role, exclusion_threshold: int, promotion_threshold: int,
                    inactivity_threshold: int):
        map_changes = { "promotion": { }, "retrogradation": { } }
        for i, member in df.iterrows():
            if member[ColumnIndex.ROLE] == role.value:
                points = int(member[ColumnIndex.AVERAGE]) if member[ColumnIndex.AVERAGE] != "" else 0
                inactivity = int(member[ColumnIndex.INACTIVITY].split("/")[0]) if member[ColumnIndex.INACTIVITY] != "" else 0
                elderness = int(member[ColumnIndex.INACTIVITY].split("/")[1]) if member[ColumnIndex.INACTIVITY] != "" else 0

                if points < exclusion_threshold and elderness > 3:
                    map_changes["retrogradation"][int(member[ColumnIndex.RANK])] = f'Participation insuffisante : {points}<{exclusion_threshold} requis'
                elif inactivity > inactivity_threshold:
                    map_changes["retrogradation"][int(member[ColumnIndex.RANK])] = f'Inactif : {inactivity}>{inactivity_threshold} acceptables'
                elif int(member[ColumnIndex.RANK]) <= promotion_threshold:
                    map_changes["promotion"][int(member[ColumnIndex.RANK])] = f'Participation de {points} points, et au rang {member["Rang"]}'
        return map_changes

    def _clear_colors(self):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(0).id
        clear_format_request = {
            "repeatCell": {
                "range" : {
                    "sheetId"         : sheet_id,
                    "startRowIndex"   : 0, "endRowIndex": 51,
                    "startColumnIndex": 0, "endColumnIndex": 10
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

    @staticmethod
    def _color_header(body: dict, sheet_id: str):
        request = SpreadsheetLoader.change_color(sheet_id=sheet_id, start_row=0, end_row=1,
                                                 start_col=0, end_col=10, r=0.4, g=0.2, b=0.5)
        body["requests"].append(request)
        return

    @staticmethod
    def _color_inactivity(df: DataFrame, body: dict, sheet_id: str):
        indexes = df.index[df[ColumnIndex.INACTIVITY.value] != "0"].tolist()
        for index in indexes:
            inactivity = int(df[ColumnIndex.INACTIVITY.value][index].split('/')[0])
            gb_levels = 1 - (inactivity * 0.11)
            if inactivity > 0:
                request = SpreadsheetLoader.change_color(sheet_id=sheet_id,
                                                         start_row=index, end_row=index + 1,
                                                         start_col=8, end_col=9, r=1, g=gb_levels, b=gb_levels)
                body["requests"].append(request)

    @staticmethod
    def _color_roles(df: DataFrame, body: dict, sheet_id: str):
        for role in list(Role):
            raw_indexes = df.index[df[ColumnIndex.ROLE.value] == role.value].tolist()
            for k, g in groupby(enumerate(raw_indexes), lambda ix: ix[0] - ix[1]):
                indexes = list(map(itemgetter(1), g))
                request = SpreadsheetLoader.change_color(sheet_id=sheet_id,
                                                         start_row=int(indexes[0]), end_row=int(indexes[-1]) + 1,
                                                         start_col=2, end_col=3, r=role.r, g=role.g, b=role.b)
                body["requests"].append(request)

    def _color_sheet(self):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(0).id
        body = { "requests": [] }

        df = self.sheet_accessor.get(index=0, first_column='A', last_column='J')

        self._color_thresholds(body, sheet_id)
        self._color_header(body, sheet_id)
        self._color_roles(df, body, sheet_id)
        self._color_inactivity(df, body, sheet_id)

        self.sheet_accessor.get_gc().batch_update(body)

    @staticmethod
    def _color_thresholds(body: dict, sheet_id: str):
        top_three = SpreadsheetLoader.change_color(sheet_id=sheet_id, start_row=1, end_row=4,
                                                   start_col=0, end_col=9, r=1, g=1, b=0.3)
        top_twenty = SpreadsheetLoader.change_color(sheet_id=sheet_id, start_row=4, end_row=21,
                                                    start_col=0, end_col=9, r=0.9, g=0.9, b=0.9)
        body["requests"].append(top_three)
        body["requests"].append(top_twenty)

    def _get_inactivity(self):
        max_col_index = self.sheet_accessor.next_available_col_index(spreadsheet_index=1)
        max_col_letter = self.sheet_accessor.next_available_col(spreadsheet_index=1, penultimate=True)
        df = self.sheet_accessor.get(index=1, last_column=max_col_letter)
        inactivities = dict()
        for i, row in df.iterrows():
            df.at[i, ColumnIndex.TAG] = f'=HYPERLINK("https://royaleapi.com/player/{row["Tag"][1:]}";"{row["Tag"]}")'
            inactivity = 0
            for it in range(5, max_col_index):
                if row[it] == "0":
                    inactivity += 1
                else:
                    break
            inactivities[row["Tag"]] = inactivity
        return inactivities

    def _import_war_results(self, df: DataFrame):
        war_results = self.sheet_accessor.get_gc().get_worksheet(1).get_values("B:C")
        df2 = pd.DataFrame(war_results)
        df2.columns = df2.iloc[0]
        df2.drop(df2.index[0], inplace=True)
        df2["Total"] = pd.to_numeric(df2["Total"])
        df = df.join(df2.set_index("Tag"), on="Tag")
        return df

    @staticmethod
    def _merge(dict1: dict, dict2: dict):
        return { "promotion"     : { **dict1["promotion"], **dict2["promotion"] },
                 "retrogradation": { **dict1["retrogradation"], **dict2["retrogradation"] } }

    @staticmethod
    def _reorder(df: DataFrame):
        df.index += 1
        for i, row in df.iterrows():
            # noinspection PyTypeChecker
            points = f"""=IFERROR(VLOOKUP(H{i + 1};'Score de Guerre'!B2:C;2; FALSE);0) + IFERROR(VLOOKUP(H{i + 1};'Score de Bateau'!B2:C;2; FALSE);0)"""
            # noinspection PyTypeChecker
            ratio = f'=IFERROR(ROUND($D{i + 1}/(MID($E{i + 1};4;2)-2);0);0)'
            # noinspection PyTypeChecker
            moyenne = f"""=IFERROR(ROUND($D{i + 1} / COUNT(INDIRECT("'Score de Guerre'!F"&MATCH($H{i + 1};'Score de Guerre'!B:B;0)&":O"&MATCH($H{i + 1};'Score de Guerre'!B:B;0)));0);0)"""
            # noinspection PyTypeChecker
            inactivity = f"""={row[ColumnIndex.INACTIVITY.value]}&"/"&COUNT(INDIRECT("'Score de Guerre'!F"&MATCH(H{i + 1};'Score de Guerre'!B:B; 0)):INDIRECT("'Score de Guerre'!O"&MATCH(H{i + 1};'Score de Guerre'!B:B; 0)))"""

            df.at[i, ColumnIndex.POINTS.value] = points
            df.at[i, ColumnIndex.RATIO.value] = ratio
            df.at[i, ColumnIndex.INACTIVITY.value] = inactivity
            df.at[i, ColumnIndex.AVERAGE.value] = moyenne
        return df

    def analyse_roles(self):
        """
             Aucune bataille pendant 4 semaines 🗙
                    ou score moyen < XXX points
                                                    ╔═════════════╗
                                                    ║ member(21)  ║
                                                    ╚═════════════╝
               Dans les 15 premiers dans les 10                       ▲ Aucune bataille pendant 6 semaines
                              dernières guerres ▼                       ou score moyen < XXX points
                                                    ╔═════════════╗
                                                    ║  elder(18)  ║
                                                    ╚═════════════╝
                Dans les 3 premiers dans les 10                       ▲ Aucune bataille pendant 8 semaines
                              dernières guerres ▼                       ou score moyen < XXX points
                                                    ╔═════════════╗
                                                    ║ coLeader(9) ║
                                                    ╚═════════════╝
                         Sur décision du leader ▼                     ▲ Sur décision du leader
                                                    ╔═════════════╗
                                                    ║  leader(1)  ║
                                                    ╚═════════════╝
        """
        try:
            df = self.sheet_accessor.get(index=0, last_column="I")

            changes = self._check_role(df, role=Role.MEMBER,
                                       exclusion_threshold=500, promotion_threshold=20,
                                       inactivity_threshold=4)  # FIXME should be 500 20 4
            changes = self._merge(changes, self._check_role(df, Role.ELDER, 1000, 3, 10))  # FIXME should be 1000 3 10
            changes = self._merge(changes, self._check_role(df, Role.CO_LEADER, 1000, 0, 18))
            return changes

        except APIError:
            return "Error. Try in a few minutes."

    def update_summary(self):
        try:
            df = self._build_summary()
            df = self._reorder(df)
            ranks = [[i] for i in range(1, df.shape[0] + 1)]
            df = df[ColumnIndex.ordered_col_indexes()[1:]]

            _range = f'B2:I'
            _values = df.values.tolist()

            self._clear_colors()

            self.sheet_accessor.get_gc().get_worksheet(0).clear()
            self.sheet_accessor.get_gc().get_worksheet(0).update(f'A1:J1', [ColumnIndex.ordered_col_indexes() + ["Indication sur le grade"]])
            self.sheet_accessor.get_gc().get_worksheet(0).update(_range, _values, value_input_option='USER_ENTERED')
            self.sheet_accessor.get_gc().get_worksheet(0).sort((6, 'des'), range='A2:I51')  # FIXME: should not have to sort...
            self.sheet_accessor.get_gc().get_worksheet(0).update('A2:A51', ranks)  # FIXME: should not have to force ranks
            self._update_role_changes()

            self.sheet_accessor.get_gc().get_worksheet(0).update_acell("L1", f"Dernière mise à jour : {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

            self._color_sheet()

            return f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : Updated Summary (Sheet #1)'
        except APIError:
            return "Error. Try in a few minutes."

    @staticmethod
    def _print_role_change(sheet_id: int, background_color: dict, role_change: dict, body: dict):
        for k, g in groupby(enumerate([x for x in role_change]), lambda ix: ix[0] - ix[1]):
            indexes = list(map(itemgetter(1), g))
            the_range = {
                        "sheetId"         : sheet_id,
                        "startRowIndex"   : indexes[0], "endRowIndex": indexes[-1]+1,
                        "startColumnIndex": 9, "endColumnIndex": 10
                    }
            body["requests"].append({
                "repeatCell": {
                    "range" : the_range,
                    "cell"  : {
                        "userEnteredFormat": {
                            "backgroundColor": background_color
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
            body["requests"].append({
                "updateCells": {
                    "range": the_range,
                    "rows": [{"values": [{"userEnteredValue": {"stringValue": role_change[x]}}]} for x in indexes ],
                    "fields": "userEnteredValue"
                }
            })

    def _update_role_changes(self):
        self._clear_colors()
        body = { "requests": [] }
        changes = self.analyse_roles()
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(0).id

        self._print_role_change(sheet_id, {"red"  : 0.4, "green": 0.8, "blue" : 0.4}, changes["promotion"], body)
        self._print_role_change(sheet_id, {"red"  : 0.8, "green": 0.4, "blue" : 0.4}, changes["retrogradation"], body)

        self.sheet_accessor.get_gc().batch_update(body)

