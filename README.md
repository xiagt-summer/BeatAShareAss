# Stock Price Boundary Analysis System

A tool for calculating dynamic price boundaries of Chinese A-share stocks using historical minute-frequency trading data and statistical volatility analysis.

## Overview

This system analyzes intraday price movements to establish statistical boundaries for stock prices. It processes minute-level trading data to compute volatility patterns and generate upper/lower price boundaries based on historical behavior.

## Usage

```bash
python3 src/main.py <csv_file> <opening_price> [-o output_file]
```

### Parameters

- `csv_file`: Path to the stock data CSV file
- `opening_price`: Today's opening price for boundary calculation
- `-o, --output`: Optional output file path (default: `recent_XXXXXX.csv`)

### Examples

```bash
# Basic usage with default output
python3 src/main.py data/002714.csv 55.06
```

## Data Format

### Input CSV Structure

| Column | Description | Format |
|--------|-------------|--------|
| Date | Trading date | YYYYMMDD |
| SecurityID | Stock code | String |
| TimeStamp | Time of data point | HH:MM:SS |
| ClosePrice | Closing price at minute | Float |
| LowPrice | Lowest price in minute | Float |
| HighPrice | Highest price in minute | Float |
| OpenPrice | Opening price at minute | Float |
| TurnoverVol | Trading volume | Float |
| TurnoverValue | Trading value | Float |

### Output CSV Structure

| Column | Description | Format |
|--------|-------------|--------|
| TimeStamp | Trading time | HH:MM:SS |
| lowerbound | Statistical lower boundary | Float (2 decimals) |
| upperbound | Statistical upper boundary | Float (2 decimals) |

## Methodology

### Statistical Boundary Calculation

1. **Data Selection**: Extract the most recent 14 trading days
2. **Movement Calculation**: For each minute j on day i:
   ```
   move[i,j] = |ClosePrice[i,j] / OpenPrice[i,09:31] - 1|
   ```
3. **Volatility Estimation**: Calculate average movement across days:
   ```
   σ[j] = Σ(move[i,j]) / 14
   ```
4. **Boundary Generation**:
   ```
   lowerbound[j] = min(x, y) × (1 - σ[j])
   upperbound[j] = max(x, y) × (1 + σ[j])
   ```
   Where x = today's opening price, y = yesterday's closing price

### Trading Hours

Analysis covers standard A-share market hours:
- Morning session: 09:30 - 11:30
- Afternoon session: 13:00 - 15:00