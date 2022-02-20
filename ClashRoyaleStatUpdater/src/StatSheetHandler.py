from ClashRoyaleAPI import DataExtractor, ApiConnectionManager
from SpreadsheetLoader import SpreadsheetLoader


class StatManager(DataExtractor):
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        super().__init__(api_connection_manager, accessor)
        warlogs = self._get_wars_log()
        print(warlogs)
        """
        [
            {
                "id": "81:1",
                "war_record": {
                    "rank": 2,
                    "trophyChange": 10,
                    "clan": {
                        "tag": "#8YJPUR",
                        "name": "L'armée",
                        "badgeId": 16000078,
                        "fame": 7960,
                        "repairPoints": 0,
                        "finishTime": "19691231T235959.000Z",
                        "participants": [],
                        "periodPoints": 0,
                        "clanScore": 3015
                    }
                }
            }
        ]
        """
        pass

