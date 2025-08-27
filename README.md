# Stock Boundary Analysis System

Statistical price boundary calculator for A-share stocks using 14-day volatility patterns.

## Usage

```bash
python3 src/main.py <data_file> <open_price> --sc <security_code> [options]
```

### Required Arguments

- `data_file`: CSV file with minute-level trading data
- `open_price`: Opening price (number) or CSV file mapping SecurityCode to OpenPrice
- `--sc`: Security code (6-digit) or "ALL" to process all stocks

### Optional Arguments

- `-o, --output`: Custom output file (default: `recent_XXXXXX.csv`)

### Examples

```bash
# Single stock with fixed opening price
python3 src/main.py data/etf1min.csv 0.85 --sc 159713

# Single stock with opening prices from CSV
python3 src/main.py data/etf1min.csv open.csv --sc 159713

# Process all stocks in file
python3 src/main.py data/etf1min.csv open.csv --sc ALL
```

## Data Formats

### Input: Trading Data (etf1min.csv)
```csv
TimeStamp,SecurityCode,ClosePrice,HighPrice,LowPrice,OpenPrice,TurnoverValue,TurnoverVol
2025-07-01 09:25:00,159713,0.854,0.854,0.854,0.854,300608.0,352000.0
```

### Input: Opening Prices (open.csv)
```csv
SecurityCode,OpenPrice
159713,1.235
002714,54.500
```

### Output: Boundaries (recent_XXXXXX.csv)
```csv
TimeStamp,lowerbound,upperbound
09:31:00,49.712,54.814
```

## Algorithm

Calculates time-specific volatility from 14 trading days:

1. **Movement**: `|Close[t] / Open[09:31] - 1|` for each minute
2. **Volatility**: Average movement across days for each timestamp
3. **Boundaries**: 
   - Lower: `min(open, prev_close) × (1 - σ)`
   - Upper: `max(open, prev_close) × (1 + σ)`

Trading hours: 09:30-11:30, 13:00-15:00 (A-share market)