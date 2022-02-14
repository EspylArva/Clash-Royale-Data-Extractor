import tkinter
from tkinter import Tk, END
from tkinter import Button
from tkinter.scrolledtext import ScrolledText
import json

from SummarySheetHandler import SummaryManager
from WarsLogSheetHandler import WarLogsManager


def format_json(string: str):
    formatted = json.dumps(string, indent=4, ensure_ascii=False).encode("utf-8").decode()
    print(formatted)


class GUI:
    def __init__(self, root: Tk, summary_manager: SummaryManager, warlogs_manager: WarLogsManager):
        self.summary_manager = summary_manager
        self.warlogs_manager = warlogs_manager

        root.title("Clash Royale Sheet Updater (CRSU)")

        frame1 = tkinter.Frame(root)
        frame2 = tkinter.Frame(root, height=frame1.winfo_height())

        btn_refresh_summary = Button(frame1, text="Refresh Summary (Sheet #1)",
                                     command=lambda: self.print_on_console(self.summary_manager.update_summary()))
        btn_refresh_warlogs = Button(frame1, text="Refresh War Logs (Sheet #2)",
                                     command=lambda: self.print_on_console(self.warlogs_manager.update_war_results()))
        btn_refresh_boatatk = Button(frame1, text="Refresh Boat Logs (Sheet #3)",
                                     command=lambda: self.print_on_console(self.warlogs_manager.update_boat_results()))
        btn_refresh_all = Button(frame1, text="Refresh Everything (Sheet #1-3)",
                                 command=lambda: self.refresh_all())

        btn_get_actions = Button(frame1, text="Get weekly Actions & Warnings",
                                 command=lambda: self.print_json_on_console(summary_manager.analyse_roles()))

        self.console = ScrolledText(frame2)

        btn_refresh_summary.grid(column=0, row=0, sticky="ew", padx=2, pady=2)
        btn_refresh_warlogs.grid(column=0, row=1, sticky="ew", padx=2, pady=2)
        btn_refresh_boatatk.grid(column=0, row=2, sticky="ew", padx=2, pady=2)
        btn_refresh_all.grid(column=0, row=3, sticky="ew", padx=2, pady=2)
        btn_get_actions.grid(column=0, row=4, sticky="ew", padx=2, pady=(8, 2))
        self.console.grid(sticky="nsew")

        frame1.grid(row=0, column=0, sticky="news", padx=4, pady=4)
        frame2.grid(row=0, column=1, sticky="nes", padx=4, pady=4)

    def refresh_all(self):
        self.console.delete('1.0', END)
        self.console.insert(1.0, self.warlogs_manager.update_war_results() + '\n')
        self.console.insert(1.0, self.warlogs_manager.update_boat_results() + '\n')
        self.console.insert(1.0, self.summary_manager.update_summary() + '\n')

    def print_json_on_console(self, string: str):
        formatted = json.dumps(string, indent=4, ensure_ascii=False).encode("utf-8").decode()
        self.console.delete('1.0', END)
        self.console.insert(1.0, formatted)

    def print_on_console(self, string: str):
        self.console.delete('1.0', END)
        self.console.insert(1.0, string)
