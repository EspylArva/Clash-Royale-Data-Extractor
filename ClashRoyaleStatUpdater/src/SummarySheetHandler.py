from datetime import datetime
from enum import Enum
from itertools import groupby
from operator import itemgetter

import pandas as pd
from pandas import DataFrame

from src import ClashRoyaleAPI
from src.ClashRoyaleAPI import Role, ApiConnectionManager
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


class SummaryManager(ClashRoyaleAPI.DataExtractor):
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        super().__init__(api_connection_manager, accessor)

    def __get_inactivity(self):
        max_col_index = self.sheet_accessor.next_available_col_index(spreadsheet_index=1)
        max_col_letter = self.sheet_accessor.next_available_col(spreadsheet_index=1, penultimate=True)

        df = self.sheet_accessor.get(index=1, last_column=max_col_letter)
        inactivities = dict()
        for i, row in df.iterrows():
            inactivity = 0
            for it in range(5, max_col_index):
                if row[it] == "0":
                    inactivity += 1
                else:
                    break
            inactivities[row["Tag"]] = inactivity
        return inactivities

    def __build_summary(self):
        inactivities = self.__get_inactivity()
        members = self._get_current_members()

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
            inactivity.append(inactivities.get(member["tag"], 0))
        df = DataFrame({
            ColumnIndex.NAME.value: pseudos,
            ColumnIndex.ROLE.value: grades,
            ColumnIndex.CASTLE_LEVEL.value: niveaux_chateau,
            ColumnIndex.RATIO.value: None,
            ColumnIndex.AVERAGE.value: None,
            ColumnIndex.TAG.value: tags,
            ColumnIndex.INACTIVITY.value: inactivity
        })
        df = self.__import_war_results(df=df)
        df = df.rename(columns={"Total": ColumnIndex.POINTS.value})
        return df

    def __import_war_results(self, df: DataFrame):
        war_results = self.sheet_accessor.get_gc().get_worksheet(1).get_values("B:C")
        df2 = pd.DataFrame(war_results)
        df2.columns = df2.iloc[0]
        df2.drop(df2.index[0], inplace=True)
        df2["Total"] = pd.to_numeric(df2["Total"])
        df = df.join(df2.set_index("Tag"), on="Tag")
        return df

    @staticmethod
    def __reorder(df: DataFrame):
        df.index += 1
        for i, row in df.iterrows():
            # noinspection PyTypeChecker
            points = f"""=IFERROR(VLOOKUP(B{i + 1};'Score de Guerre'!A2:C;3; FALSE);0) + IFERROR(VLOOKUP(B{i + 1};'Score de Bateau'!A2:C;3; FALSE);0)"""
            ratio = f'=IFERROR(ROUND($D{i + 1}/($E{i + 1}-2);0);0)'
            moyenne = f"""=IFERROR(ROUND($D{i+1} / COUNT(INDIRECT("'Score de Guerre'!F"&MATCH($B{i+1};'Score de Guerre'!A:A;0)&":O"&MATCH($B{i+1};'Score de Guerre'!A:A;0)));0);0)"""
            df.at[i, ColumnIndex.POINTS.value] = points
            df.at[i, ColumnIndex.RATIO.value] = ratio
            df.at[i, ColumnIndex.AVERAGE.value] = moyenne
        return df

    @staticmethod
    def _color_header(body: dict, sheet_id: str):
        request = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 1,
                    "startColumnIndex": 0, "endColumnIndex": 9
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.4,
                        "green": 0.2,
                        "blue": 0.5
                    }
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        }
        body["requests"].append(request)
        return

    @staticmethod
    def _color_roles(df: DataFrame, body: dict, sheet_id: str):
        for role in list(Role):
            raw_indexes = df.index[df['Grade'] == role.value].tolist()
            for k, g in groupby(enumerate(raw_indexes), lambda ix: ix[0] - ix[1]):
                indexes = list(map(itemgetter(1), g))
                print(f'{role.value}: {indexes} -- {int(indexes[0]) + 1}|{int(indexes[-1]) + 2}')
                request = {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": int(indexes[0]), "endRowIndex": int(indexes[-1]) + 1,
                            "startColumnIndex": 2, "endColumnIndex": 3
                        },
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": {
                                "red": role.r,
                                "green": role.g,
                                "blue": role.b
                            }
                        }},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                }
                body["requests"].append(request)

    @staticmethod
    def _color_thresholds(df: DataFrame, body: dict, sheet_id: str):
        top_three = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1, "endRowIndex": 4,
                    "startColumnIndex": 0, "endColumnIndex": 9
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {
                        "red": 1,
                        "green": 1,
                        "blue": 0.3
                    }
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        }
        top_fifteen = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 4, "endRowIndex": 16,
                    "startColumnIndex": 0, "endColumnIndex": 9
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.8,
                        "green": 0.8,
                        "blue": 0.8
                    }
                }},
                "fields": "userEnteredFormat.backgroundColor"
            }
        }
        body["requests"].append(top_three)
        body["requests"].append(top_fifteen)

    @staticmethod
    def _color_inactivity(df: DataFrame, body: dict, sheet_id: str):
        indexes = df.index[df[ColumnIndex.INACTIVITY.value] != "0"].tolist()
        for index in indexes:
            inactivity = int(df[ColumnIndex.INACTIVITY.value][index])
            print(f'{index} :: {inactivity}')
            request = {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": index, "endRowIndex": index+1,
                        "startColumnIndex": 8, "endColumnIndex": 9
                    },
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": {
                            "red": 1,
                            "green":  1 - (inactivity*0.11),
                            "blue": 1 - (inactivity*0.11)

                        }
                    }},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            }
            body["requests"].append(request)

    def _clear_colors(self):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(0).id
        clear_format_request = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 51,
                    "startColumnIndex": 0, "endColumnIndex": 10
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


    def _color_sheet(self):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(0).id
        body = {"requests": []}

        df = self.sheet_accessor.get(index=0, first_column='A', last_column='I')

        self._color_thresholds(df, body, sheet_id)
        self._color_header(body, sheet_id)
        self._color_roles(df, body, sheet_id)
        self._color_inactivity(df, body, sheet_id)

        self.sheet_accessor.get_gc().batch_update(body)

    def update_summary(self):
        df = self.__build_summary()
        df = self.__reorder(df)
        ranks = [[i] for i in range(1, df.shape[0]+1)]
        df = df[ColumnIndex.ordered_col_indexes()[1:]]

        _range = f'B2:I'
        _values = df.values.tolist()

        self._clear_colors()

        self.sheet_accessor.get_gc().get_worksheet(0).clear()
        self.sheet_accessor.get_gc().get_worksheet(0).update(f'A1:I1', [ColumnIndex.ordered_col_indexes()])
        self.sheet_accessor.get_gc().get_worksheet(0).update(_range, _values, value_input_option='USER_ENTERED')
        self.sheet_accessor.get_gc().get_worksheet(0).sort((6, 'des'), range='A2:H51')
        self.sheet_accessor.get_gc().get_worksheet(0).update('A2:A51', ranks)

        self._color_sheet()

        return f'{datetime.now()} : Updated Summary (Sheet #1)'

    @staticmethod
    def __merge(dict1: dict, dict2: dict):
        return {"promotion": dict1["promotion"] + dict2["promotion"],
                "retrogradation": dict1["retrogradation"] + dict2["retrogradation"]}

    @staticmethod
    def __check_role(df: DataFrame, role: Role, exclusion_threshold: int, promotion_threshold: int,
                     inactivity_threshold: int):
        map_changes = {"promotion": [], "retrogradation": []}
        for i, member in df.iterrows():
            if member["Grade"] == role.value:
                points = int(member["Points de clan"]) if member["Points de clan"] != "" else 0
                inactivity = int(member["Durée d'inactivité (en semaines)"]) if member[
                                                                                    "Durée d'inactivité (en semaines)"] != "" else 0

                if points < exclusion_threshold or inactivity > inactivity_threshold:
                    map_changes["retrogradation"].append(
                        f'{member["Pseudo"]} (participation: {points} (<{exclusion_threshold}) or inactivity: {inactivity} (>{inactivity_threshold}))')
                elif int(member["Rang"]) <= promotion_threshold:
                    map_changes["promotion"].append(
                        f'{member["Pseudo"]} (scored {points}, and ranked {member["Rang"]}')
        return map_changes

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
        df = self.sheet_accessor.get(index=0, last_column="I")

        changes = self.__check_role(df, Role.MEMBER, 300, 4, 15)
        changes = self.__merge(changes, self.__check_role(df, Role.ELDER, 600, 6, 999))
        changes = self.__merge(changes, self.__check_role(df, Role.CO_LEADER, 900, 8, 999))
        return changes


