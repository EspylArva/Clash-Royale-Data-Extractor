import http.client
import json

from aenum import Enum
from pyxtension.streams import stream

from src.SpreadsheetLoader import SpreadsheetLoader


class Role(Enum):
    _init_ = "value r g b"
    MEMBER = "Membre", 0.5, 0.9, 1
    ELDER = "Aîné", 0.3, 0.6, 0.9
    CO_LEADER = "Adjoint", 0.1, 0.8, 0.4
    LEADER = "Chef", 0.6, 0.4, 0.6

    @staticmethod
    def get_french_function(function: str):
        if function == "member":
            return Role.MEMBER
        elif function == "elder":
            return Role.ELDER
        elif function == "coLeader":
            return Role.CO_LEADER
        elif function == "leader":
            return Role.LEADER
        else:
            return None


class ApiConnectionManager:
    CLAN_ID = "8YJPUR"
    OFFICIAL_API_URL = "api.clashroyale.com"
    PROXY_API_URL = "proxy.royaleapi.dev"
    PYTHONANYWHERE_PROXY = "proxy.server"
    PYTHONANYWHERE_PORT = 3128

    def __init__(self):
        filename = './resources/cr-api-key.txt'
        with open(filename, 'r') as file:
            self.token = file.read()
            self.conn = http.client.HTTPSConnection(self.PYTHONANYWHERE_PROXY, self.PYTHONANYWHERE_PORT)
            self.conn.set_tunnel(self.PROXY_API_URL)
            self.headers = { 'Authorization': 'Bearer {0}'.format(self.token) }
            self.WAR_LOG_ENDPOINT = f"/v1/clans/%23{self.CLAN_ID}/riverracelog"
            self.MEMBERS_ENDPOINT = f"/v1/clans/%23{self.CLAN_ID}/members"

    def get_member_data(self):
        response = ""
        for i in range(10):
            try:
                self.conn.request("GET", self.MEMBERS_ENDPOINT, None, self.headers)
                response = self.conn.getresponse().read().decode("utf-8")
                print(response)
                if response != "":
                    break
            except Exception:
                print("Failed request to get member data. Try again.")
        members = json.loads(response)
        return members

    def get_war_data(self):
        response = ""
        for i in range(10):
            try:
                self.conn.request("GET", self.WAR_LOG_ENDPOINT, None, self.headers)
                response = self.conn.getresponse().read().decode("utf-8")
                if response != "":
                    break
            except Exception:
                print("Failed request to get war data. Try again.")
        data = json.loads(response)
        return data


class DataExtractor:
    def __init__(self, api_connection_manager: ApiConnectionManager, accessor: SpreadsheetLoader):
        self.clan_id = api_connection_manager.CLAN_ID
        self.war_data = api_connection_manager.get_war_data()
        self.members_data = api_connection_manager.get_member_data()
        self.sheet_accessor = accessor

    def _get_current_members(self):
        clan_members = stream(self.members_data["items"]) \
            .sorted(lambda member: member["name"]) \
            .map(
                lambda member: {
                    "name"       : member["name"],
                    "role"       : Role.get_french_function(member["role"]),
                    "tag"        : f'=HYPERLINK("https://royaleapi.com/player/{member["tag"][1:]}";"{member["tag"]}")',
                    "castleLevel": f'👑 {member["expLevel"]}'
                }) \
            .toJson()
        return clan_members

    def _get_wars_log(self):
        return stream(self.war_data["items"]) \
            .map(
                lambda season: {
                    "id"        : f'{season["seasonId"]}:{int(season["sectionIndex"]) + 1}',
                    "war_record": stream(season["standings"]).filter(
                            lambda clan: clan["clan"]["tag"] == f'#{self.clan_id}'
                    ).toList()[0] }) \
            .toList()
