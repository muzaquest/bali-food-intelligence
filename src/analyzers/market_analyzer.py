from __future__ import annotations

import sqlite3
import pandas as pd
from dataclasses import dataclass
from typing import Dict, Any, Tuple
from datetime import datetime, timedelta


@dataclass
class Period:
    start: str
    end: str


def _prev_period(period: Period) -> Period:
    sd = pd.to_datetime(period.start)
    ed = pd.to_datetime(period.end)
    length = (ed - sd).days + 1
    prev_end = sd - timedelta(days=1)
    prev_start = prev_end - timedelta(days=length - 1)
    return Period(prev_start.strftime('%Y-%m-%d'), prev_end.strftime('%Y-%m-%d'))


def _sum(df: pd.DataFrame, col: str) -> float:
    return float(df[col].sum()) if col in df.columns else 0.0


def _kpi_delta(cur: float, prev: float) -> Tuple[float, float]:
    if prev == 0:
        return cur, 0.0 if cur == 0 else 100.0
    return cur, (cur - prev) / prev * 100.0


def analyze_market_period(db_path: str, period: Period) -> Dict[str, Any]:
    prev = _prev_period(period)
    with sqlite3.connect(db_path) as conn:
        # Aggregate whole base per day then sum
        q = """
        WITH grab AS (
          SELECT stat_date as date,
                 COALESCE(sales,0) as g_sales,
                 COALESCE(orders,0) as g_orders,
                 COALESCE(payouts,0) as g_payouts,
                 COALESCE(ads_spend,0) as g_ads_spend,
                 COALESCE(ads_sales,0) as g_ads_sales,
                 COALESCE(rating,0) as g_rating
          FROM grab_stats
          WHERE stat_date BETWEEN ? AND ?
        ),
        gojek AS (
          SELECT stat_date as date,
                 COALESCE(sales,0) as j_sales,
                 COALESCE(orders,0) as j_orders,
                 COALESCE(payouts,0) as j_payouts,
                 COALESCE(ads_spend,0) as j_ads_spend,
                 COALESCE(ads_sales,0) as j_ads_sales,
                 COALESCE(rating,0) as j_rating
          FROM gojek_stats
          WHERE stat_date BETWEEN ? AND ?
        ),
        merged AS (
          SELECT COALESCE(g.date, j.date) as date,
                 COALESCE(g.g_sales,0) as g_sales,
                 COALESCE(j.j_sales,0) as j_sales,
                 COALESCE(g.g_orders,0) as g_orders,
                 COALESCE(j.j_orders,0) as j_orders,
                 COALESCE(g.g_payouts,0) as g_payouts,
                 COALESCE(j.j_payouts,0) as j_payouts,
                 COALESCE(g.g_ads_spend,0) as g_ads_spend,
                 COALESCE(j.j_ads_spend,0) as j_ads_spend,
                 COALESCE(g.g_ads_sales,0) as g_ads_sales,
                 COALESCE(j.j_ads_sales,0) as j_ads_sales,
                 COALESCE(g.g_rating,0) as g_rating,
                 COALESCE(j.j_rating,0) as j_rating
          FROM grab g FULL OUTER JOIN gojek j
          ON g.date=j.date
        )
        SELECT * FROM merged
        """
        cur_df = pd.read_sql_query(q, conn, params=(period.start, period.end, period.start, period.end))
        prev_df = pd.read_sql_query(q, conn, params=(prev.start, prev.end, prev.start, prev.end))

    # compute KPIs
    def kpis(df: pd.DataFrame) -> Dict[str, float]:
        total_sales = _sum(df, 'g_sales') + _sum(df, 'j_sales')
        total_payouts = _sum(df, 'g_payouts') + _sum(df, 'j_payouts')
        g_sales = _sum(df, 'g_sales'); j_sales = _sum(df, 'j_sales')
        g_payouts = _sum(df, 'g_payouts'); j_payouts = _sum(df, 'j_payouts')
        g_orders = _sum(df, 'g_orders'); j_orders = _sum(df, 'j_orders')
        g_ads_spend = _sum(df, 'g_ads_spend'); j_ads_spend = _sum(df, 'j_ads_spend')
        g_ads_sales = _sum(df, 'g_ads_sales'); j_ads_sales = _sum(df, 'j_ads_sales')
        g_roas = (g_ads_sales / g_ads_spend) if g_ads_spend > 0 else 0.0
        j_roas = (j_ads_sales / j_ads_spend) if j_ads_spend > 0 else 0.0
        g_avg_check = (g_sales / g_orders) if g_orders > 0 else 0.0
        j_avg_check = (j_sales / j_orders) if j_orders > 0 else 0.0
        ads_share = ((g_ads_spend + j_ads_spend) / total_sales * 100.0) if total_sales > 0 else 0.0
        return dict(
            total_sales=total_sales,
            total_payouts=total_payouts,
            g_sales=g_sales, j_sales=j_sales,
            g_payouts=g_payouts, j_payouts=j_payouts,
            g_orders=g_orders, j_orders=j_orders,
            g_ads_spend=g_ads_spend, j_ads_spend=j_ads_spend,
            g_ads_sales=g_ads_sales, j_ads_sales=j_ads_sales,
            g_roas=g_roas, j_roas=j_roas,
            g_avg_check=g_avg_check, j_avg_check=j_avg_check,
            ads_share=ads_share
        )

    cur = kpis(cur_df); prv = kpis(prev_df)
    # Attach deltas
    out: Dict[str, Any] = { 'period': period.__dict__, 'prev_period': prev.__dict__, 'current': cur, 'prev': prv, 'delta': {} }
    for key in cur.keys():
        out['delta'][key] = _kpi_delta(cur[key], prv[key])
    return out