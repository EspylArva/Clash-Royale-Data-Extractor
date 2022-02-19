import http.client
import json
from enum import Enum

from pyxtension.streams import stream

from src.SpreadsheetLoader import SpreadsheetLoader


class Role(str, Enum):
    MEMBER = "Membre"
    ELDER = "Aîné"
    CO_LEADER = "Adjoint"
    LEADER = "Chef"

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
    TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjAwM2ZmZTU0LTRlNGQtNGYzZi1iODA5LTlhM2MxYzA3MDdkNiIsImlhdCI6MTYzNzU5MTM0NSwic3ViIjoiZGV2ZWxvcGVyLzBhN2ZjMTg1LWZmMmEtMThjMC1iNTFlLWY3MmMyZmM3MzJmMSIsInNjb3BlcyI6WyJyb3lhbGUiXSwibGltaXRzIjpbeyJ0aWVyIjoiZGV2ZWxvcGVyL3NpbHZlciIsInR5cGUiOiJ0aHJvdHRsaW5nIn0seyJjaWRycyI6WyIxOTIuNTQuMTQ1LjE0MyIsIjEyOC4xMjguMTI4LjEyOCIsIjkzLjI2LjY2LjE1NSJdLCJ0eXBlIjoiY2xpZW50In1dfQ.dzY8ytb12EGdw_AJs8El9oPnE-9vs6boTXxwR9IUabLksyBNfxDwxcQQ22aEjHa85IA3qNgw6tDLtt1KFfbqsw"
    CLAN_ID = "8YJPUR"
    OFFICIAL_API_URL = "api.clashroyale.com"
    PROXY_API_URL = "proxy.royaleapi.dev"

    def __init__(self):
        self.conn = http.client.HTTPSConnection(self.PROXY_API_URL)
        self.headers = { 'Authorization': 'Bearer {0}'.format(self.TOKEN) }
        self.WAR_LOG_ENDPOINT = f"/v1/clans/%23{self.CLAN_ID}/riverracelog"
        self.MEMBERS_ENDPOINT = f"/v1/clans/%23{self.CLAN_ID}/members"

    def get_war_data(self):
        self.conn.request("GET", self.WAR_LOG_ENDPOINT, None, self.headers)
        response = self.conn.getresponse().read().decode("utf-8")
        data = json.loads(response)
        return data

    def get_member_data(self):
        self.conn.request("GET", self.MEMBERS_ENDPOINT, None, self.headers)
        response = self.conn.getresponse().read().decode("utf-8")
        members = json.loads(response)
        return members


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
