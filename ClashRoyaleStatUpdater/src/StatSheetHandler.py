from datetime import datetime

from gspread.exceptions import APIError
from pyxtension.streams import stream

from src.ClashRoyaleAPI import ApiConnectionManager, DataExtractor
from src.SpreadsheetLoader import SpreadsheetLoader


class StatManager(DataExtractor):
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        super().__init__(api_connection_manager, accessor)

    def get_statistics(self):
        warlogs = self._get_wars_log()
        statistics = stream(warlogs).map(lambda x: {
            "war_id"                  : x["id"],

            "participation_percentage": len(
                    list(filter(lambda y: (y["fame"] != 0), x["war_record"]["clan"]["participants"]))) /
                                        len(x["war_record"]["clan"]["participants"]),
            "participation"           : f'={len(list(filter(lambda y: (y["fame"] != 0), x["war_record"]["clan"]["participants"])))}&"/"&{len(x["war_record"]["clan"]["participants"])}',
            "points"                  : sum([y["fame"] for y in x["war_record"]["clan"]["participants"]]),

            "result"                  : x["war_record"]["rank"],
            "variation"               : x["war_record"]["trophyChange"],
            "score"                   : x["war_record"]["clan"]["clanScore"]
        }).toList()
        return statistics

    def update_statistics(self):
        try:
            statistics = self.get_statistics()

            last_col_index = chr(ord("F") + len(statistics) - 1)

            _participation_range = f'F2:{last_col_index}4'
            _participation = [
                [x["participation_percentage"] for x in statistics],
                [x["participation"] for x in statistics],
                [x["points"] for x in statistics]
            ]

            _results_range = f'F7:{last_col_index}9'
            _results = [
                [f'={x["result"]}&"/5"' for x in statistics],
                [x["variation"] for x in statistics],
                [x["score"] for x in statistics]
            ]

            self.sheet_accessor.get_gc().get_worksheet(3).update(f'F1:{last_col_index}1',
                                                                 [[x["war_id"] for x in statistics]],
                                                                 value_input_option='USER_ENTERED')
            self.sheet_accessor.get_gc().get_worksheet(3).update(_participation_range, _participation,
                                                                 value_input_option='USER_ENTERED')
            self.sheet_accessor.get_gc().get_worksheet(3).update(_results_range, _results,
                                                                 value_input_option='USER_ENTERED')

            return f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} : Updated Statistics (Sheet #4)'
        except APIError:
            # time.sleep(60)
            return "Error. Try in a few minutes."

    def insert_missing_data(self, statistics: list):
        sheet_id = self.sheet_accessor.get_gc().get_worksheet(3).id
        #history = self.sheet_accessor.get_gc().get_worksheet(3).get_values("F1:1")[0]
        history = []
        new_stats = stream(statistics).filter(lambda war: war["war_id"] not in history).toList()
        if len(new_stats) > 0:
            body = { 'requests': [] }
            body["requests"].append({
                "insertDimension": {
                    "range"            : {
                        "sheetId"   : sheet_id,
                        "dimension" : "COLUMNS",
                        "startIndex": 5,
                        "endIndex"  : 5 + len(new_stats)
                    },
                    "inheritFromBefore": False
                }
            })
            self.sheet_accessor.get_gc().batch_update(body)

            last_col_index = chr(ord("F") + len(new_stats) - 1)
            _participation_range = f'F2:{last_col_index}4'
            _participation = [
                [x["participation_percentage"] for x in new_stats],
                [x["participation"] for x in new_stats],
                [x["points"] for x in new_stats]
            ]

            _results_range = f'F7:{last_col_index}9'
            _results = [
                [f'={x["result"]}&"/5"' for x in new_stats],
                [x["variation"] for x in new_stats],
                [x["score"] for x in new_stats]
            ]

            self.sheet_accessor.get_gc().get_worksheet(3).update(f'F1:{last_col_index}1',
                                                                 [[x["war_id"] for x in new_stats]],
                                                                 value_input_option='USER_ENTERED')
            self.sheet_accessor.get_gc().get_worksheet(3).update(_participation_range, _participation,
                                                                 value_input_option='USER_ENTERED')
            self.sheet_accessor.get_gc().get_worksheet(3).update(_results_range, _results,
                                                                 value_input_option='USER_ENTERED')
