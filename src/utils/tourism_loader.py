import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict

DATA_DIR = os.path.join('data', 'tourism')
INDEX_PATH = os.path.join('data', 'tourism_index.json')


def _list_tourism_files() -> list:
	files = []
	if os.path.isdir(DATA_DIR):
		for fn in os.listdir(DATA_DIR):
			if fn.lower().endswith(('.xls', '.xlsx')):
				files.append(os.path.join(DATA_DIR, fn))
	return sorted(files)


def build_tourism_index(save_path: str = INDEX_PATH) -> Dict[str, float]:
	"""Reads tourism Excel files and builds a daily index mapping.
	
	Expected columns: a date-like or month/year with counts. If only monthly,
	spread counts uniformly across days of month; then apply 7-day moving average.
	"""
	dfs = []
	for fp in _list_tourism_files():
		try:
			df = pd.read_excel(fp)
			dfs.append(df)
		except Exception:
			continue
	if not dfs:
		return {}
	raw = pd.concat(dfs, ignore_index=True)
	# Try to infer a date column
	date_col = None
	for c in raw.columns:
		lc = str(c).lower()
		if 'date' in lc or 'tanggal' in lc or 'month' in lc or 'bulan' in lc:
			date_col = c
			break
	count_col = None
	for c in raw.columns:
		if c == date_col:
			continue
		if str(raw[c].dtype).startswith(('int', 'float')):
			count_col = c
			break
	if date_col is None or count_col is None:
		return {}
	# Parse dates; if monthly period, set to first of month
	raw['_date'] = pd.to_datetime(raw[date_col], errors='coerce', dayfirst=True)
	raw = raw.dropna(subset=['_date'])
	raw['_count'] = pd.to_numeric(raw[count_col], errors='coerce').fillna(0)
	# Build daily frame between min and max
	start = raw['_date'].min().date().replace(day=1)
	end = (raw['_date'].max().date().replace(day=1) + timedelta(days=40)).replace(day=1) - timedelta(days=1)
	all_days = pd.date_range(start, end, freq='D')
	idx_df = pd.DataFrame({'date': all_days})
	# Monthly sum per month
	raw['month'] = raw['_date'].dt.to_period('M')
	monthly = raw.groupby('month')['_count'].sum().reset_index()
	monthly['date'] = monthly['month'].dt.to_timestamp()
	# Spread monthly counts across days
	idx_df['month'] = idx_df['date'].dt.to_period('M')
	idx_df = idx_df.merge(monthly[['month', '_count']], on='month', how='left').rename(columns={'_count': 'monthly_count'})
	# Days in month
	idx_df['days_in_month'] = idx_df['date'].dt.days_in_month
	idx_df['daily'] = (idx_df['monthly_count'] / idx_df['days_in_month']).fillna(0)
	# Smooth with 7-day moving average
	idx_df['index'] = idx_df['daily'].rolling(window=7, min_periods=1, center=True).mean()
	idx_df['date_str'] = idx_df['date'].dt.strftime('%Y-%m-%d')
	mapping = dict(zip(idx_df['date_str'], idx_df['index'].astype(float)))
	# Normalize to 0..1 for stability
	vals = pd.Series(mapping)
	vmin, vmax = vals.min(), vals.max()
	if pd.notna(vmin) and pd.notna(vmax) and vmax > vmin:
		for k in list(mapping.keys()):
			mapping[k] = float((mapping[k] - vmin) / (vmax - vmin))
	# Save
	os.makedirs(os.path.dirname(save_path), exist_ok=True)
	with open(save_path, 'w', encoding='utf-8') as f:
		json.dump(mapping, f, ensure_ascii=False)
	return mapping


def load_tourism_index(path: str = INDEX_PATH) -> Dict[str, float]:
	"""Loads tourism index from json, building if missing."""
	if not os.path.exists(path):
		return build_tourism_index(path)
	try:
		with open(path, 'r', encoding='utf-8') as f:
			return json.load(f)
	except Exception:
		return {}