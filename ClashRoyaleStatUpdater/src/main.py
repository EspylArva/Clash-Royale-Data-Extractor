import os

import pandas as pd

from ClashRoyaleAPI import ApiConnectionManager
from SpreadsheetLoader import SpreadsheetLoader, SpreadsheetLoaderSettings
from StatSheetHandler import StatManager
from SummarySheetHandler import SummaryManager
from WarsLogSheetHandler import WarLogsManager

if __name__ == '__main__':
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
        stat_manager.update_statistics()

"""
        root = Tk()
        root.iconbitmap('resources/icon.ico')
        gui = GUI(root, summary_manager, warlogs_manager)
        root.mainloop()
"""

# pyinstaller --onefile --noconsole src/main.py -i ./resources/icon.ico --hidden-import pyxtension

# TODO
# - Totaux sur page 2 et 3
# - Historiques des guerres
# - Stats utiles (moyennes, min, max)
#     - % de participation par guerre
#     - nombre d'attaque de bateau
#     - gain de points
#     - position
# - Afficher top X des participants sur chaque guerre
# - Affichage des updates de role
