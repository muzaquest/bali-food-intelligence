#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import time
import sqlite3
from typing import Dict, Any
from datetime import datetime, timedelta

import requests
import pandas as pd

CACHE_DIR = os.path.join('data', 'weather_cache')
os.makedirs(CACHE_DIR, exist_ok=True)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def load_locations(path: str = os.path.join('data', 'bali_restaurant_locations.json')) -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_date_range(conn: sqlite3.Connection) -> (str, str):
    q = """
    SELECT MIN(dt) as min_date, MAX(dt) as max_date FROM (
      SELECT MIN(stat_date) as dt FROM grab_stats
      UNION ALL
      SELECT MIN(stat_date) FROM gojek_stats
      UNION ALL
      SELECT MAX(stat_date) FROM grab_stats
      UNION ALL
      SELECT MAX(stat_date) FROM gojek_stats
    )
    """
    df = pd.read_sql_query(q, conn)
    min_date = str(df.iloc[0]['min_date'])
    max_date = str(df.iloc[0]['max_date'])
    return min_date, max_date


def daterange_chunks(start: datetime, end: datetime, chunk_days: int = 31):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)


def fetch_daily_weather(lat: float, lon: float, start: str, end: str) -> Dict[str, Dict[str, float]]:
    result: Dict[str, Dict[str, float]] = {}
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    for s, e in daterange_chunks(start_dt, end_dt, 31):
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': s.strftime('%Y-%m-%d'),
            'end_date': e.strftime('%Y-%m-%d'),
            'hourly': 'temperature_2m,precipitation',
            'timezone': 'Asia/Jakarta'
        }
        r = requests.get(ARCHIVE_URL, params=params, timeout=20)
        if r.status_code != 200:
            time.sleep(1)
            continue
        data = r.json().get('hourly', {})
        times = data.get('time', [])
        temps = data.get('temperature_2m', [])
        rains = data.get('precipitation', [])
        # Aggregate per day
        daily: Dict[str, Dict[str, float]] = {}
        for t, temp, rain in zip(times, temps, rains):
            day = t.split('T')[0]
            if day not in daily:
                daily[day] = {'temperature_sum': 0.0, 'temperature_cnt': 0, 'precipitation_sum': 0.0}
            daily[day]['temperature_sum'] += float(temp)
            daily[day]['temperature_cnt'] += 1
            daily[day]['precipitation_sum'] += float(rain)
        for day, agg in daily.items():
            result[day] = {
                'temperature': agg['temperature_sum'] / max(1, agg['temperature_cnt']),
                'precipitation': agg['precipitation_sum']
            }
        time.sleep(0.2)  # be gentle
    return result


def main():
    conn = sqlite3.connect('database.sqlite')
    start, end = get_date_range(conn)
    locations = load_locations()

    # Expect either {name: {latitude, longitude}} or {"restaurants": [{name, latitude, longitude}]}
    name_to_coords: Dict[str, Dict[str, float]] = {}
    if 'restaurants' in locations:
        for r in locations['restaurants']:
            if 'name' in r and 'latitude' in r and 'longitude' in r:
                name_to_coords[r['name']] = {'lat': r['latitude'], 'lon': r['longitude']}
    else:
        for name, v in locations.items():
            if isinstance(v, dict) and 'latitude' in v and 'longitude' in v:
                name_to_coords[name] = {'lat': v['latitude'], 'lon': v['longitude']}

    # Map restaurant_id -> name
    id_name = pd.read_sql_query("SELECT id, name FROM restaurants", conn)

    for _, row in id_name.iterrows():
        rid = int(row['id'])
        name = row['name']
        coords = name_to_coords.get(name)
        if not coords:
            continue
        out_path = os.path.join(CACHE_DIR, f"daily_{rid}.json")
        if os.path.exists(out_path):
            continue
        print(f"Fetching weather for {name} ({rid}) {start}..{end}")
        try:
            daily = fetch_daily_weather(coords['lat'], coords['lon'], start, end)
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(daily, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print("Failed:", e)
            continue

    print("Done. Weather cache in", CACHE_DIR)

if __name__ == '__main__':
    main()