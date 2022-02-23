import os

import pandas as pd
from flask import Flask

from ClashRoyaleAPI import ApiConnectionManager
from SpreadsheetLoader import SpreadsheetLoader, SpreadsheetLoaderSettings
from StatSheetHandler import StatManager
from SummarySheetHandler import SummaryManager
from WarsLogSheetHandler import WarLogsManager

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


@app.route('/')
def hello():
    return 'Hello World!'


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
    return summary_manager.update_summary()


def mark_update():
    loader.get_gc().get_worksheet(0).update_acell("L1", "Mise-à-jour en cours... Merci de patienter...")


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
    # https://cloud.google.com/appengine/docs/standard/python3/quickstart#windows
"""
        root = Tk()
        root.iconbitmap('resources/icon.ico')
        gui = GUI(root, summary_manager, warlogs_manager)
        root.mainloop()
"""

# pyinstaller --onefile --noconsole src/main.py -i ./resources/icon.ico --hidden-import pyxtension

# cd C:\Users\Iteration\PycharmProject\Clash-Royale-Data-Extractor\ClashRoyaleStatUpdater
# /venv/Script/activate
# gcloud app deploy
# gcloud app brwose


# TODO
# - Totaux sur page 2 et 3
# - Historiques des guerres
# - Afficher top X des participants sur chaque guerre
# - Affichage des updates de role
