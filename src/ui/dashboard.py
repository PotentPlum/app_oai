import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime
import csv

import matplotlib
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from src.app_service import AppService
from src.config import config

matplotlib.use("TkAgg")


class Dashboard(tk.Tk):
    def __init__(self, service: AppService) -> None:
        super().__init__()
        self.title("EcoPulse Dashboard")
        self.geometry("1000x700")
        self.service = service
        self.service.set_status_callback(self._update_status)
        self._status_var = tk.StringVar(value="Ready")

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True)

        self.env_tab = ttk.Frame(self.notebook)
        self.macro_tab = ttk.Frame(self.notebook)
        self.ops_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.env_tab, text="Environment")
        self.notebook.add(self.macro_tab, text="Macro Compare")
        self.notebook.add(self.ops_tab, text="Data Ops")

        self._build_env_tab()
        self._build_macro_tab()
        self._build_ops_tab()
        self._refresh_env()
        self._refresh_macro()

    def _build_env_tab(self) -> None:
        top = ttk.Frame(self.env_tab)
        top.pack(fill="x", padx=10, pady=5)
        ttk.Label(top, text="Location:").pack(side="left")
        self.env_location = tk.StringVar(value=config.LOCATIONS[0].key)
        loc_names = [f"{loc.name} ({loc.key})" for loc in config.LOCATIONS]
        self.loc_map = {loc.key: loc.name for loc in config.LOCATIONS}
        self.loc_dropdown = ttk.Combobox(top, textvariable=self.env_location, values=[loc.key for loc in config.LOCATIONS], state="readonly")
        self.loc_dropdown.pack(side="left", padx=5)
        self.loc_dropdown.bind("<<ComboboxSelected>>", lambda e: self._refresh_env())

        self.kpi_frame = ttk.Frame(self.env_tab)
        self.kpi_frame.pack(fill="x", padx=10, pady=5)
        self.kpi_labels = {
            "ts": ttk.Label(self.kpi_frame, text="Last update: -"),
            "temp": ttk.Label(self.kpi_frame, text="Temp: -"),
            "wind": ttk.Label(self.kpi_frame, text="Wind: -"),
            "precip": ttk.Label(self.kpi_frame, text="Precip: -"),
            "pm25": ttk.Label(self.kpi_frame, text="PM2.5: -"),
            "pm10": ttk.Label(self.kpi_frame, text="PM10: -"),
            "aqi": ttk.Label(self.kpi_frame, text="AQI(EU/US): -"),
        }
        for lbl in self.kpi_labels.values():
            lbl.pack(side="left", padx=5)

        table_frame = ttk.Frame(self.env_tab)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)
        cols = ("ts", "temp", "wind", "precip", "pm25", "pm10", "aqi_eu", "aqi_us")
        self.env_table = ttk.Treeview(table_frame, columns=cols, show="headings", height=10)
        for col in cols:
            self.env_table.heading(col, text=col)
            self.env_table.column(col, width=100)
        self.env_table.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.env_table.yview)
        self.env_table.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")

        chart_frame = ttk.Frame(self.env_tab)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=5)
        series_frame = ttk.Frame(chart_frame)
        series_frame.pack(fill="x")
        ttk.Label(series_frame, text="Series:").pack(side="left")
        self.env_series = tk.StringVar(value="european_aqi")
        ttk.Combobox(series_frame, textvariable=self.env_series, values=["european_aqi", "pm2_5"], state="readonly").pack(side="left", padx=5)
        self.env_series.trace_add("write", lambda *_: self._refresh_env_chart())

        self.env_fig = Figure(figsize=(6, 3))
        self.env_ax = self.env_fig.add_subplot(111)
        self.env_canvas = FigureCanvasTkAgg(self.env_fig, master=chart_frame)
        self.env_canvas.get_tk_widget().pack(fill="both", expand=True)

    def _build_macro_tab(self) -> None:
        top = ttk.Frame(self.macro_tab)
        top.pack(fill="x", padx=10, pady=5)
        ttk.Label(top, text="Indicator:").pack(side="left")
        self.macro_indicator = tk.StringVar(value=list(config.WB_INDICATORS.keys())[0])
        self.ind_dropdown = ttk.Combobox(top, textvariable=self.macro_indicator, values=list(config.WB_INDICATORS.keys()), state="readonly")
        self.ind_dropdown.pack(side="left", padx=5)
        self.ind_dropdown.bind("<<ComboboxSelected>>", lambda e: self._refresh_macro())

        ttk.Label(top, text="Start Year:").pack(side="left")
        self.start_year = tk.IntVar(value=2000)
        ttk.Spinbox(top, from_=1960, to=2050, textvariable=self.start_year, width=6, command=self._refresh_macro).pack(side="left", padx=2)
        ttk.Label(top, text="End Year:").pack(side="left")
        self.end_year = tk.IntVar(value=datetime.now().year)
        ttk.Spinbox(top, from_=1960, to=2050, textvariable=self.end_year, width=6, command=self._refresh_macro).pack(side="left", padx=2)

        chart_frame = ttk.Frame(self.macro_tab)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=5)
        self.macro_fig = Figure(figsize=(6, 3))
        self.macro_ax = self.macro_fig.add_subplot(111)
        self.macro_canvas = FigureCanvasTkAgg(self.macro_fig, master=chart_frame)
        self.macro_canvas.get_tk_widget().pack(fill="both", expand=True)

        table_frame = ttk.Frame(self.macro_tab)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)
        cols = ("region", "year", "value", "delta")
        self.macro_table = ttk.Treeview(table_frame, columns=cols, show="headings", height=8)
        for col in cols:
            self.macro_table.heading(col, text=col)
            self.macro_table.column(col, width=120)
        self.macro_table.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.macro_table.yview)
        self.macro_table.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")

    def _build_ops_tab(self) -> None:
        frame = ttk.Frame(self.ops_tab)
        frame.pack(fill="x", padx=10, pady=10)
        ttk.Button(frame, text="Fetch Now (All)", command=self._run_fetch_async).pack(side="left", padx=5)
        ttk.Button(frame, text="Start Scheduler", command=self._start_scheduler).pack(side="left", padx=5)
        ttk.Button(frame, text="Stop Scheduler", command=self._stop_scheduler).pack(side="left", padx=5)
        ttk.Button(frame, text="Export CSV", command=self._export_csv).pack(side="left", padx=5)

        self.status_label = ttk.Label(self.ops_tab, textvariable=self._status_var, anchor="w")
        self.status_label.pack(fill="x", padx=10, pady=5)

        self.mongo_status = ttk.Label(self.ops_tab, text="Mongo: unknown")
        self.mongo_status.pack(fill="x", padx=10)
        self.sqlite_status = ttk.Label(self.ops_tab, text="SQLite: ok")
        self.sqlite_status.pack(fill="x", padx=10)
        self._refresh_db_status()

        interval_frame = ttk.LabelFrame(self.ops_tab, text="Scheduler intervals (minutes)")
        interval_frame.pack(fill="x", padx=10, pady=5)
        self.env_interval_min = tk.IntVar(value=int(self.service.env_interval / 60))
        self.macro_interval_min = tk.IntVar(value=int(self.service.macro_interval / 60))
        self.wiki_interval_min = tk.IntVar(value=int(self.service.wiki_interval / 60))
        ttk.Label(interval_frame, text="Environment:").grid(row=0, column=0, padx=5, pady=2)
        ttk.Spinbox(interval_frame, from_=5, to=24 * 60, textvariable=self.env_interval_min, width=6).grid(
            row=0, column=1, padx=5
        )
        ttk.Label(interval_frame, text="Macro:").grid(row=0, column=2, padx=5, pady=2)
        ttk.Spinbox(interval_frame, from_=60, to=7 * 24 * 60, textvariable=self.macro_interval_min, width=6).grid(
            row=0, column=3, padx=5
        )
        ttk.Label(interval_frame, text="Wikipedia:").grid(row=0, column=4, padx=5, pady=2)
        ttk.Spinbox(interval_frame, from_=60, to=14 * 24 * 60, textvariable=self.wiki_interval_min, width=6).grid(
            row=0, column=5, padx=5
        )
        ttk.Button(interval_frame, text="Apply intervals", command=self._apply_intervals).grid(row=0, column=6, padx=5)

        log_frame = ttk.LabelFrame(self.ops_tab, text="Source activity log")
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        cols = ("source", "started", "finished", "ok", "message", "items")
        self.source_log = ttk.Treeview(log_frame, columns=cols, show="headings", height=10)
        for col in cols:
            self.source_log.heading(col, text=col)
            self.source_log.column(col, width=120, anchor="w")
        self.source_log.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.source_log.yview)
        self.source_log.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        ttk.Button(self.ops_tab, text="Refresh log", command=self._refresh_source_log).pack(anchor="e", padx=10, pady=5)
        self._refresh_source_log()

    def _update_status(self, text: str) -> None:
        if threading.current_thread() is threading.main_thread():
            self._status_var.set(text)
        else:
            # Marshal updates to the Tk event loop to avoid cross-thread Tkinter access
            self.after(0, lambda: self._status_var.set(text))

    def _refresh_db_status(self) -> None:
        self.mongo_status.config(text=f"Mongo: {'connected' if self.service.mongo.available else 'unavailable'}")
        self.sqlite_status.config(text="SQLite: connected")

    def _run_fetch_async(self) -> None:
        threading.Thread(target=self.service.fetch_all, daemon=True).start()

    def _start_scheduler(self) -> None:
        if not self.service.mongo.available:
            messagebox.showerror("MongoDB required", "MongoDB is unavailable. Start docker-compose first.")
            return
        self._apply_intervals()
        self.service.start_scheduler()

    def _stop_scheduler(self) -> None:
        self.service.stop_scheduler()

    def _apply_intervals(self) -> None:
        env_minutes = max(1, self.env_interval_min.get())
        macro_minutes = max(1, self.macro_interval_min.get())
        wiki_minutes = max(1, self.wiki_interval_min.get())
        self.service.update_intervals(env_minutes * 60, macro_minutes * 60, wiki_minutes * 60)

    def _refresh_source_log(self) -> None:
        self.source_log.delete(*self.source_log.get_children())
        rows = self.service.sqlite.latest_source_runs(limit=100)
        for row in rows:
            source, started, finished, ok, message, count = row
            self.source_log.insert(
                "",
                "end",
                values=(source, started, finished, "ok" if ok else "fail", message, count),
            )

    def _export_csv(self) -> None:
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not file_path:
            return
        if self.notebook.index(self.notebook.select()) == 0:
            loc = self.env_location.get()
            rows = self.service.sqlite.latest_env_rows(loc, limit=200)
            headers = ["ts_utc", "temp_c", "wind_kph", "precip_mm", "pm2_5", "pm10", "european_aqi", "us_aqi"]
        else:
            indicator = self.macro_indicator.get()
            rows = self.service.sqlite.macro_series(indicator, self.start_year.get(), self.end_year.get())
            headers = ["region_code", "year", "value"]
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
        messagebox.showinfo("Export", f"Saved to {file_path}")

    def _refresh_env(self) -> None:
        loc_key = self.env_location.get()
        self.env_table.delete(*self.env_table.get_children())
        rows = self.service.sqlite.latest_env_rows(loc_key, limit=48)
        for row in rows:
            self.env_table.insert("", "end", values=row)
        kpi = self.service.sqlite.latest_env_kpis(loc_key)
        if kpi:
            ts, temp, wind, precip, pm25, pm10, aqi_eu, aqi_us = kpi
            self.kpi_labels["ts"].config(text=f"Last update: {ts}")
            self.kpi_labels["temp"].config(text=f"Temp: {temp} C")
            self.kpi_labels["wind"].config(text=f"Wind: {wind} kph")
            self.kpi_labels["precip"].config(text=f"Precip: {precip} mm")
            self.kpi_labels["pm25"].config(text=f"PM2.5: {pm25}")
            self.kpi_labels["pm10"].config(text=f"PM10: {pm10}")
            self.kpi_labels["aqi"].config(text=f"AQI(EU/US): {aqi_eu}/{aqi_us}")
        self._refresh_env_chart()

    def _refresh_env_chart(self) -> None:
        loc_key = self.env_location.get()
        series = self.env_series.get()
        rows = list(reversed(self.service.sqlite.latest_env_rows(loc_key, limit=48)))
        times = [row[0] for row in rows]
        values = [row[6] if series == "european_aqi" else row[4] for row in rows]
        temp_series = [row[1] for row in rows]
        self.env_ax.clear()
        self.env_ax.plot(times, values, label=series)
        self.env_ax.plot(times, temp_series, label="temp_c", linestyle="--")
        self.env_ax.set_xticklabels(times, rotation=45, ha="right")
        self.env_ax.legend()
        self.env_ax.set_title(f"{series} vs temp")
        self.env_canvas.draw()

    def _refresh_macro(self) -> None:
        indicator = self.macro_indicator.get()
        start = self.start_year.get()
        end = self.end_year.get()
        rows = self.service.sqlite.macro_series(indicator, start, end)
        series_by_region = {code: [] for code in config.WB_REGIONS.keys()}
        for region, year, value in rows:
            series_by_region.setdefault(region, []).append((year, value))
        self.macro_ax.clear()
        years = list(range(start, end + 1))
        for region, data in series_by_region.items():
            data_dict = {y: v for y, v in data}
            vals = [data_dict.get(y) for y in years]
            self.macro_ax.plot(years, vals, label=region)
        self.macro_ax.legend()
        self.macro_ax.set_title(indicator)
        self.macro_canvas.draw()

        self.macro_table.delete(*self.macro_table.get_children())
        latest_rows = self.service.sqlite.macro_latest(indicator)
        seen = set()
        for region, year, value in latest_rows:
            if region in seen:
                continue
            prev_year = year - 1
            prev = next((v for r, y, v in latest_rows if r == region and y == prev_year), None)
            delta = value - prev if prev is not None and value is not None else None
            self.macro_table.insert("", "end", values=(region, year, value, delta))
            seen.add(region)

    def refresh_all(self) -> None:
        self._refresh_env()
        self._refresh_macro()
