import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import webbrowser
import os
import time
from datetime import datetime, timedelta
from databases import MongoManager, RedisManager, analyze_county_day_night, get_date_range, get_counties_with_station_count
from mapka import map_creator

class AnalysisGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Analiza Pomiarów Meteorologicznych")
        self.root.geometry("700x600")
        
        self.mongo = MongoManager()
        self.redis = RedisManager()
        
        self.county_counts = get_counties_with_station_count(self.mongo, self.redis)
        self.counties = [f"{name} [{count}]" for name, count in sorted(self.county_counts.items()) if count > 0]
        self.min_date, self.max_date = get_date_range(self.mongo)
        
        self.create_widgets()
        
    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.grid(sticky="nsew")
        
        ttk.Label(main_frame, text="Analiza Danych Meteorologicznych", 
                 font=('Arial', 14, 'bold')).grid(columnspan=2, pady=10)
        
        analysis_frame = ttk.LabelFrame(main_frame, text="Analiza Powiatu", padding=10)
        analysis_frame.grid(row=1, columnspan=2, sticky="ew", pady=5)
        
        def add_combo_row(frame, row, label_text, values, default_idx=0):
            ttk.Label(frame, text=label_text).grid(row=row, sticky="w", padx=5, pady=5)
            combo = ttk.Combobox(frame, width=23, state='readonly', values=values)
            if values and default_idx < len(values):
                combo.current(default_idx)
            combo.grid(row=row, column=1, sticky="w", padx=5, pady=5)
            return combo
        
        self.county_combo = add_combo_row(analysis_frame, 0, "Powiat:", self.counties)
        
        self.available_dates = self._generate_date_range()
        self.start_date = add_combo_row(analysis_frame, 1, "Data od:", self.available_dates, 0)
        self.end_date = add_combo_row(analysis_frame, 2, "Data do:", self.available_dates, 
                                      len(self.available_dates) - 1 if self.available_dates else 0)
        
        button_frame = ttk.Frame(analysis_frame)
        button_frame.grid(row=3, columnspan=2, pady=10)
        
        ttk.Button(button_frame, text="Wykonaj Analizę i Mapę", 
                  command=self.run_analysis).pack(side="left", padx=5)
        ttk.Button(button_frame, text="Otwórz Mapę", 
                  command=self.open_map).pack(side="left", padx=5)
        
        results_frame = ttk.LabelFrame(main_frame, text="Wyniki", padding=10)
        results_frame.grid(row=2, columnspan=2, sticky="nsew", pady=5)
        
        self.results_text = scrolledtext.ScrolledText(results_frame, width=80, height=22, 
                                                      font=('Courier', 9))
        self.results_text.grid(sticky="nsew")
        
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
    
    def _generate_date_range(self):     
        min_dt = datetime.strptime(self.min_date, '%Y-%m-%d')
        max_dt = datetime.strptime(self.max_date, '%Y-%m-%d')
        dates = []
        current = min_dt
        while current <= max_dt:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        return dates
    
    def _update_results(self, text):
        self.results_text.delete(1.0, "end")
        self.results_text.insert("end", text)
    
    def _format_results(self, result):
        output = ""
        output += f"POWIAT: {result['county']['name']}\n"
        output += f"Przedział: {result['date_range']['start']} - {result['date_range']['end']}\n"
        output += f"Liczba stacji: {len(result['stations'])}\n"
        return output
    
    def run_analysis(self):
        county = self.county_combo.get().split('[')[0].strip()
        start, end = self.start_date.get().strip(), self.end_date.get().strip()
        
        self._update_results("Wykonywanie analizy...\n\n")
        self.root.update()
        
        result = analyze_county_day_night(self.mongo, self.redis, county, start, end)
        
        output = self._format_results(result)
        output += self._format_stations(result['stations'])
        output += self._format_summary(result['stations'])
        
        self._update_results(output)
        self.results_text.insert("end", "Generowanie mapy...\n")
        self.root.update()
        
        map_creator(result)
    
    def _format_stations(self, stations):
        output = ""
        for station in stations:
            a = station['analysis']
            diff = a['avg_temp_day'] - a['avg_temp_night']
            output += (f"{station['name']} (ID: {station['station_id']})\n"
                      f"  DZIEŃ:   {a['avg_temp_day']:6.2f}°C  ({a['day_measurements']} pom.)\n"
                      f"  NOC:     {a['avg_temp_night']:6.2f}°C  ({a['night_measurements']} pom.)\n"
                      f"  RÓŻNICA: {diff:5.2f}°C\n\n")
        return output
    
    def _format_summary(self, stations):
        day_sum = day_count = night_sum = night_count = 0
        
        for s in stations:
            a = s['analysis']
            day_sum += a['avg_temp_day'] * a['day_measurements']
            day_count += a['day_measurements']
            night_sum += a['avg_temp_night'] * a['night_measurements']
            night_count += a['night_measurements']
        
        avg_day, avg_night = day_sum / day_count, night_sum / night_count
        return (f"\nPODSUMOWANIE\n"
                f"Średnia DZIEŃ: {avg_day:.2f}°C (z {day_count} pomiarów)\n"
                f"Średnia NOC:   {avg_night:.2f}°C (z {night_count} pomiarów)\n"
                f"Różnica:       {avg_day - avg_night:.2f}°C\n")
    
    def open_map(self):
        map_path = os.path.abspath('Wizualizacja.html')
        url = f"file://{map_path}?t={int(time.time())}"
        webbrowser.open(url, new=2)


def main_gui():
    root = tk.Tk()
    app = AnalysisGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main_gui()
