import os

import pandas as pd
from flask import Flask

from src.ClashRoyaleAPI import ApiConnectionManager
from src.SpreadsheetLoader import SpreadsheetLoader, SpreadsheetLoaderSettings
from src.StatSheetHandler import StatManager
from src.SummarySheetHandler import SummaryManager
from src.WarsLogSheetHandler import WarLogsManager

app = Flask(__name__)

print(os.path.abspath(__file__))
filename = './resources/google-api-key.txt'
with open(filename, 'r') as file:
    data = file.read()
    fd = open(filename, 'rb')

    pd.set_option('display.width', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

loader = SpreadsheetLoader(settings=SpreadsheetLoaderSettings(data))
api_connection_manager = ApiConnectionManager()

summary_manager = SummaryManager(api_connection_manager, loader)
warlogs_manager = WarLogsManager(api_connection_manager, loader)
stat_manager = StatManager(api_connection_manager, loader)

ip = "127.0.0.1"
port = 8080
localhost = f'http://{ip}:{port}'

@app.route('/')
def hello():
    return {
        "Update Summary"     : f'{localhost}/update/summary',
        "Update War logs"    : f'{localhost}/update/war-logs',
        "Update Boat attacks": f'{localhost}/update/boat-attacks',
        "Update Statistics"  : f'{localhost}/update/statistics',
        "Update Everything"  : f'{localhost}/update/all'
    }


@app.route('/update/summary')
def update_summary():
    return summary_manager.update_summary()


@app.route('/update/war-logs')
def update_warlogs():
    return warlogs_manager.update_war_results()


@app.route('/update/boat-attacks')
def update_boats():
    return warlogs_manager.update_boat_results()


@app.route('/update/statistics')
def update_statistics():
    return stat_manager.update_statistics()


@app.route('/update/all')
def update_everything():
    mark_update()
    stat_manager.update_statistics()
    warlogs_manager.update_war_results()
    warlogs_manager.update_boat_results()
    summary_manager.update_summary()
    return "Update finished"


# @app.route('/test')
def test():
    stat_manager.insert_missing_data(stat_manager.get_statistics())
    return "Test fini"


def mark_update():
    loader.get_gc().get_worksheet(0).update_acell("L1", "Mise-à-jour en cours... Merci de patienter...")


if __name__ == '__main__':
    app.run(host=ip, port=port, debug=True)

    # https://cloud.google.com/appengine/docs/standard/python3/quickstart#windows
    # Warning: http://127.0.0.1:8080 != https://127.0.0.1:8080 (use HTTP, not HTTPS)
"""
        root = Tk()
        root.iconbitmap('resources/icon.ico')
        gui = GUI(root, summary_manager, warlogs_manager)
        root.mainloop()
"""

# pyinstaller --onefile --noconsole src/main.py -i ./resources/icon.ico --hidden-import pyxtension

# cd C:\Users\Iteration\PycharmProject\Clash-Royale-Data-Extractor\ClashRoyaleStatUpdater
# /venv/Script/activate

# pip freeze > requirements.txt

# gcloud app deploy
# gcloud app browse


# TODO
# - Totaux sur page 2 et 3
# - Historiques des guerres
# - Afficher top X des participants sur chaque guerre
# - Affichage des updates de role
