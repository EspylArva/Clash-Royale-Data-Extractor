import datetime
import os
import tkinter

from SpreadsheetLoader import SpreadsheetLoader, SpreadsheetLoaderSettings

import pandas as pd
from tkinter import Tk
from ClashRoyaleAPI import ApiConnectionManager
from src import SummarySheetHandler, WarsLogSheetHandler
from GUI import GUI

if __name__ == '__main__':
    pd.set_option('display.width', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    loader = SpreadsheetLoader(settings=SpreadsheetLoaderSettings())
    api_connection_manager = ApiConnectionManager()

    summary_manager = SummarySheetHandler.SummaryManager(api_connection_manager, loader)
    warlogs_manager = WarsLogSheetHandler.WarLogsManager(api_connection_manager, loader)
    # TODO : Totaux sur page 2 et 3
    # loader.change_color()

    root = Tk()
    root.iconbitmap('./../resources/icon.ico')
    gui = GUI(root, summary_manager, warlogs_manager)
    root.mainloop()


