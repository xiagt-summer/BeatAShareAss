#!/usr/bin/env python3
"""Stock Price Boundary Analysis System

Calculates statistical price boundaries for A-share stocks based on historical
minute-frequency trading data. Analyzes 14-day price movements to establish
dynamic upper and lower boundaries using volatility patterns.
"""

import argparse
import polars as pl
from datetime import datetime
from pathlib import Path

def is_trading_time(timestamp):
    """Validate if timestamp falls within A-share market trading hours.
    
    Args:
        timestamp: Time string in HH:MM:SS format
        
    Returns:
        bool: True if within trading hours (9:30-11:30 or 13:00-15:00)
    """
    time = datetime.strptime(timestamp, "%H:%M:%S").time()
    morning_start = datetime.strptime("09:30:00", "%H:%M:%S").time()
    morning_end = datetime.strptime("11:30:00", "%H:%M:%S").time()
    afternoon_start = datetime.strptime("13:00:00", "%H:%M:%S").time()
    afternoon_end = datetime.strptime("15:00:00", "%H:%M:%S").time()
    
    return (morning_start <= time <= morning_end) or (afternoon_start <= time <= afternoon_end)

def calculate_bounds(file_path, open_price):
    """Calculate statistical price boundaries based on historical volatility.
    
    Processes minute-level trading data from the most recent 14 trading days,
    computing price movements relative to daily opening prices. Generates
    boundaries using volatility-adjusted factors.
    
    Args:
        file_path (str): Path to CSV file containing stock trading data
        open_price (float): Today's opening price for boundary calculation
    
    Returns:
        pl.DataFrame: Contains TimeStamp, lowerbound, and upperbound columns
                     with prices formatted to 2 decimal places
    
    Algorithm:
        1. Extract 14 most recent trading days
        2. Calculate movement: |Close[i,j] / Open[i,09:31] - 1|
        3. Compute volatility: σ[j] = mean(movement) across days
        4. Generate boundaries using min/max of opening and closing prices
    """
    # Load trading data from CSV
    df = pl.read_csv(file_path)
    
    # Select 14 most recent trading days for analysis
    unique_dates = df.select("Date").unique().sort("Date")
    recent_dates = unique_dates.tail(14)["Date"].to_list()
    
    if len(recent_dates) < 14:
        print(f"Warning: Only {len(recent_dates)} days available, less than 14 days requested")
    
    # Extract recent trading period data
    df_recent = df.filter(pl.col("Date").is_in(recent_dates))
    
    # Capture market opening prices (9:31 represents first trading minute)
    daily_open = df_recent.filter(pl.col("TimeStamp") == "09:31:00").select([
        "Date", 
        pl.col("OpenPrice").alias("DailyOpen")
    ])
    
    # Merge daily opening prices with minute-level data
    df_with_open = df_recent.join(daily_open, on="Date", how="left")
    
    # Compute price movement relative to daily opening
    df_with_move = df_with_open.with_columns([
        ((pl.col("ClosePrice") / pl.col("DailyOpen") - 1).abs()).alias("move")
    ])
    
    # Filter to standard trading hours only
    df_trading = df_with_move.filter(
        pl.col("TimeStamp").map_elements(is_trading_time, return_dtype=pl.Boolean)
    )
    
    # Compute time-specific volatility across trading days
    sigma = df_trading.group_by("TimeStamp").agg([
        pl.col("move").mean().alias("sigma")
    ]).sort("TimeStamp")
    
    # Retrieve previous day's market close (15:00)
    latest_date = recent_dates[-1] if recent_dates else None
    if latest_date:
        latest_close = df_recent.filter(
            (pl.col("Date") == latest_date) & (pl.col("TimeStamp") == "15:00:00")
        )["ClosePrice"][0]
    else:
        print("Warning: No data available for recent close price")
        latest_close = open_price
    
    # Apply volatility-based boundary calculation
    # Lower: conservative estimate using minimum reference price
    # Upper: expanded estimate using maximum reference price
    min_price = min(open_price, latest_close)
    max_price = max(open_price, latest_close)
    
    bounds = sigma.with_columns([
        (min_price * (1 - pl.col("sigma"))).alias("lowerbound"),
        (max_price * (1 + pl.col("sigma"))).alias("upperbound")
    ])
    
    # Format output with precise decimal handling
    # Floor operation for lower bounds (conservative)
    # Ceiling operation for upper bounds (expansive)
    result = bounds.with_columns([
        ((pl.col("lowerbound") * 100).floor() / 100).alias("lowerbound"),
        ((pl.col("upperbound") * 100).ceil() / 100).alias("upperbound")
    ]).select([
        "TimeStamp",
        pl.col("lowerbound").map_elements(lambda x: round(x, 2), return_dtype=pl.Float64).alias("lowerbound"),
        pl.col("upperbound").map_elements(lambda x: round(x, 2), return_dtype=pl.Float64).alias("upperbound")
    ])
    
    return result

def main():
    """Main entry point for the stock boundary analysis system."""
    parser = argparse.ArgumentParser(
        description="Statistical price boundary calculator for A-share stocks",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "infile", 
        type=str, 
        help="Input CSV file containing stock trading data"
    )
    parser.add_argument(
        "open_price", 
        type=float, 
        help="Today's opening price for boundary calculation"
    )
    parser.add_argument(
        "--output", "-o", 
        type=str, 
        help="Custom output file path (default: recent_STOCKCODE.csv)"
    )
    
    args = parser.parse_args()
    
    # Validate input file existence
    file_path = Path(args.infile)
    if not file_path.exists():
        # Attempt fallback to data directory
        file_path = Path("data") / args.infile
        if not file_path.exists():
            print(f"Error: Input file {args.infile} not found")
            return 1
    
    # Execute boundary calculation
    result = calculate_bounds(str(file_path), args.open_price)
    
    # Display analysis summary
    print(f"Stock Analysis for {file_path.name}")
    print(f"Opening Price: {args.open_price}")
    print("\nBounds by TimeStamp:")
    print(result)
    
    # Configure output destination
    if args.output:
        output_file = args.output
    else:
        # Generate default filename using stock code
        stock_code = file_path.stem
        output_file = f"recent_{stock_code}.csv"
    
    # Export results with precision formatting
    result.write_csv(output_file, float_precision=2)
    print(f"\nResults saved to {output_file}")
    
    return 0

if __name__ == "__main__":
    exit(main())