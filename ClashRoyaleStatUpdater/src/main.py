<<<<<<< HEAD
import os
=======
from SpreadsheetLoader import SpreadsheetLoader, SpreadsheetLoaderSettings
import pandas as pd
>>>>>>> d281a87... Fixed some color issues on Summary. Updated todo list.
from tkinter import Tk

from ClashRoyaleAPI import ApiConnectionManager
from GUI import GUI
from SpreadsheetLoader import SpreadsheetLoader, SpreadsheetLoaderSettings
from src import SummarySheetHandler, WarsLogSheetHandler

if __name__ == '__main__':
    print(os.path.abspath(__file__))
    filename = './resources/google-api-key.txt'
    with open(filename, 'r') as file:
        data = file.read()
        fd = open(filename, 'rb')

    loader = SpreadsheetLoader(settings=SpreadsheetLoaderSettings())
    api_connection_manager = ApiConnectionManager()

    summary_manager = SummarySheetHandler.SummaryManager(api_connection_manager, loader)
    warlogs_manager = WarsLogSheetHandler.WarLogsManager(api_connection_manager, loader)

    root = Tk()
    root.iconbitmap('./../resources/icon.ico')
    # root.tk.call('wm', 'iconphoto', root._w, tkinter.PhotoImage(file='./../resources/icon.ico'))
    gui = GUI(root, summary_manager, warlogs_manager)
    root.mainloop()

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