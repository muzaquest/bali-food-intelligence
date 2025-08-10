#!/usr/bin/env python3
import argparse
from datetime import datetime
from src.analyzers import ProductionSalesAnalyzer


def main():
    parser = argparse.ArgumentParser(description="Generate restaurant analysis report in README format")
    parser.add_argument("--restaurant", required=True, help="Restaurant name")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    restaurant = args.restaurant
    start_date = args.start
    end_date = args.end

    # Header like README
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    num_days = (end_dt - start_dt).days + 1

    print(f"🏪 АНАЛИЗ РЕСТОРАНА: {restaurant}")
    print(f"🗓️ ПЕРИОД: {start_date} — {end_date} ({num_days} дней)")
    print("\n" + "═" * 79 + "\n")

    analyzer = ProductionSalesAnalyzer()
    lines = analyzer.generate_executive_summary(restaurant, start_date, end_date)
    print("\n".join(lines))


if __name__ == "__main__":
    main()