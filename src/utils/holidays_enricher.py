from __future__ import annotations

import os
import json
from typing import Dict, Any, List

import requests


def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _fetch_calendarific(year: int, country: str = "ID") -> List[Dict[str, Any]]:
    api_key = os.getenv("CALENDARIFIC_API_KEY") or os.getenv("CALENDAR_API_KEY")
    if not api_key:
        return []
    try:
        url = "https://calendarific.com/api/v2/holidays"
        params = {
            "api_key": api_key,
            "country": country,
            "year": year,
            "type": "national,religious,observance"
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json().get("response", {}).get("holidays", [])
        out = []
        for h in data:
            out.append({
                "date": h.get("date", {}).get("iso"),
                "name": h.get("name"),
                "type": h.get("type", ["observance"])[:1][0]
            })
        return out
    except Exception:
        return []


def build_enhanced_holidays(start_year: int, end_year: int) -> Dict[str, Dict[str, Any]]:
    """Merge existing comprehensive file, Calendarific fetch and user overrides.
    Returns dict: {date: {name, type, category}}
    """
    # Base from comprehensive file
    comp = _load_json(os.path.join("data", "comprehensive_holiday_analysis.json"))
    if isinstance(comp, dict) and "results" in comp:
        base_map = comp["results"].copy()
    elif isinstance(comp, dict):
        base_map = comp.copy()
    else:
        base_map = {}

    # Normalize base entries to have type/category
    norm_map: Dict[str, Dict[str, Any]] = {}
    for d, info in base_map.items():
        if not isinstance(info, dict):
            info = {"name": str(info)}
        entry = {
            "name": info.get("name", "Holiday"),
            "type": info.get("type", info.get("category", "observance")),
            "category": info.get("category", info.get("type", "observance"))
        }
        norm_map[str(d)] = entry

    # Calendarific
    for year in range(start_year, end_year + 1):
        for h in _fetch_calendarific(year):
            d = str(h.get("date"))
            if not d:
                continue
            norm_map.setdefault(d, {"name": h.get("name", "Holiday"), "type": h.get("type", "observance"), "category": h.get("type", "observance")})

    # User overrides (to add Balinese-specific if missing)
    overrides = _load_json(os.path.join("data", "holiday_overrides.json")) or {"holidays": []}
    for h in overrides.get("holidays", []):
        d = str(h.get("date"))
        if not d:
            continue
        entry = {
            "name": h.get("name", "Holiday"),
            "type": h.get("type", h.get("category", "observance")),
            "category": h.get("category", h.get("type", "observance"))
        }
        norm_map[d] = entry

    # Persist enhanced cache
    _save_json(os.path.join("data", "enhanced_holidays.json"), norm_map)
    return norm_map