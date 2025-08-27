#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

def calculate_bounds(file_path, open_price, security_id=None):
    """Calculate statistical price boundaries based on historical volatility.
    
    Processes minute-level trading data from the most recent 14 trading days,
    computing price movements relative to daily opening prices. Generates
    boundaries using volatility-adjusted factors.
    
    Args:
        file_path (str): Path to CSV file containing stock trading data
        open_price (float): Today's opening price for boundary calculation
        security_id (str): Optional SecurityID to filter data for specific stock
    
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
    
    # Check format and normalize columns
    if "SecurityCode" in df.columns:
        # New format (etf1min.csv): TimeStamp contains datetime, SecurityCode as identifier
        df = df.with_columns([
            pl.col("TimeStamp").str.slice(0, 10).str.replace("-", "").alias("Date"),
            pl.col("TimeStamp").str.slice(11, 19).alias("TimeStamp_new"),
            pl.col("SecurityCode").cast(pl.Utf8).str.zfill(6).alias("SecurityID")
        ])
        df = df.drop("TimeStamp").rename({"TimeStamp_new": "TimeStamp"})
    elif "SecurityID" in df.columns:
        # Old format (002714.csv): Date and TimeStamp separate, SecurityID as identifier
        # Drop unnamed index column if exists
        if "" in df.columns or df.columns[0] == "":
            df = df.drop(df.columns[0])
        # Ensure SecurityID is string and format as 6 digits
        df = df.with_columns(pl.col("SecurityID").cast(pl.Utf8).str.zfill(6).alias("SecurityID"))
    
    # Filter by SecurityID if specified
    if security_id is not None:
        df = df.filter(pl.col("SecurityID") == str(security_id).zfill(6))
    
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
    
    # Format output with precise decimal handling to 3 decimal places
    # Floor operation for lower bounds (conservative)
    # Ceiling operation for upper bounds (expansive)
    result = bounds.with_columns([
        ((pl.col("lowerbound") * 1000).floor() / 1000).alias("lowerbound"),
        ((pl.col("upperbound") * 1000).ceil() / 1000).alias("upperbound")
    ]).select([
        "TimeStamp",
        pl.col("lowerbound").map_elements(lambda x: round(x, 3), return_dtype=pl.Float64).alias("lowerbound"),
        pl.col("upperbound").map_elements(lambda x: round(x, 3), return_dtype=pl.Float64).alias("upperbound")
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
        type=str, 
        help="Today's opening price (number) or CSV file with SecurityCode,OpenPrice columns"
    )
    parser.add_argument(
        "--sc",
        type=str,
        required=True,
        help="Security code to analyze (use 'ALL' to process all stocks)"
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
    
    # Process open_price parameter - can be a number or CSV file
    open_price_map = {}
    try:
        # Try to parse as float first
        open_price_default = float(args.open_price)
        # If successful, use this value for all securities
        open_price_map = None  # Signal to use default value
    except ValueError:
        # Not a number, try to load as CSV file
        open_price_path = Path(args.open_price)
        if not open_price_path.exists():
            open_price_path = Path("data") / args.open_price
            if not open_price_path.exists():
                print(f"Error: Open price file {args.open_price} not found")
                return 1
        
        # Load open prices from CSV
        open_df = pl.read_csv(str(open_price_path))
        # Convert SecurityCode to string and format as 6 digits
        open_df = open_df.with_columns(pl.col("SecurityCode").cast(pl.Utf8).str.zfill(6))
        # Create dictionary mapping SecurityCode to OpenPrice
        open_price_map = dict(zip(open_df["SecurityCode"], open_df["OpenPrice"]))
        open_price_default = None
    
    # Load data to get available security codes
    df = pl.read_csv(str(file_path))
    
    # Handle different column names based on format
    if "SecurityCode" in df.columns:
        # New format: use SecurityCode, format as 6 digits
        df = df.with_columns(pl.col("SecurityCode").cast(pl.Utf8).str.zfill(6).alias("SecurityID"))
    elif "SecurityID" in df.columns:
        # Old format: use SecurityID, format as 6 digits
        # Drop unnamed index column if exists
        if "" in df.columns or df.columns[0] == "":
            df = df.drop(df.columns[0])
        df = df.with_columns(pl.col("SecurityID").cast(pl.Utf8).str.zfill(6).alias("SecurityID"))
    
    available_codes = df.select("SecurityID").unique()["SecurityID"].to_list()
    
    # Determine which security codes to process
    if args.sc.upper() == "ALL":
        security_codes = available_codes
        print(f"Processing all {len(security_codes)} security codes: {security_codes}")
    else:
        # Convert to string and format as 6 digits
        security_code = str(args.sc).zfill(6)
        if security_code in available_codes:
            security_codes = [security_code]
        else:
            print(f"Error: Security code {security_code} not found in data")
            print(f"Available codes: {available_codes}")
            return 1
    
    # Process each security code
    for security_code in security_codes:
        print(f"\n{'='*50}")
        print(f"Processing Security Code: {security_code}")
        print(f"{'='*50}")
        
        # Get opening price for this security
        if open_price_map is None:
            # Use the same default price for all securities
            current_open_price = open_price_default
        else:
            # Look up security-specific open price
            if security_code not in open_price_map:
                print(f"Warning: Security code {security_code} not found in open price file, skipping...")
                continue
            current_open_price = open_price_map[security_code]
        
        # Execute boundary calculation for this security
        result = calculate_bounds(str(file_path), current_open_price, security_code)
        
        # Display analysis summary
        print(f"Opening Price: {current_open_price}")
        print("\nBounds by TimeStamp:")
        print(result)
        
        # Configure output destination
        if args.output and len(security_codes) == 1:
            output_file = args.output
        else:
            # Generate default filename using stock code
            output_file = f"recent_{security_code}.csv"
        
        # Export results with precision formatting (3 decimal places)
        result.write_csv(output_file, float_precision=3)
        print(f"\nResults saved to {output_file}")
    
    return 0

if __name__ == "__main__":
    exit(main())