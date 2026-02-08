"""
Load Raw Orders to PostgreSQL (Simple Version)
- Full Load: Load all Excel files
- Incremental: Add new files only
"""

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import sys

import os

DATA_DIR = Path('raw_data/data')
# Use Docker host (postgres:5432) when in container, else localhost:5433
DB_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://postgres:postgres@postgres:5432/ecommerce_db'
)


def full_load():
    """Load all Excel files (truncate + reload)"""
    engine = create_engine(DB_URL)
    
    # Get all Excel files
    excel_files = sorted(DATA_DIR.glob('*.xlsx'))
    print(f"Found {len(excel_files)} files")
    
    # Load and merge all files
    all_dfs = []
    for f in excel_files:
        df = pd.read_excel(f, dtype=str)
        df['source_file'] = f.name
        all_dfs.append(df)
        print(f"  {f.name}: {len(df)} rows")
    
    combined = pd.concat(all_dfs, ignore_index=True)
    
    # Create schema and load
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    
    combined.to_sql('raw_orders', engine, schema='raw', if_exists='replace', index=False)
    print(f" Loaded {len(combined)} rows to raw.raw_orders")
    return len(combined)


def incremental_load(new_file: str):
    """Add a single new Excel file to existing data"""
    engine = create_engine(DB_URL)
    file_path = DATA_DIR / new_file
    
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return 0
    
    df = pd.read_excel(file_path, dtype=str)
    df['source_file'] = file_path.name
    
    df.to_sql('raw_orders', engine, schema='raw', if_exists='append', index=False)
    print(f" Appended {len(df)} rows from {new_file}")
    return len(df)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Incremental: python load_raw_orders.py "Order.completed.20230101.xlsx"
        incremental_load(sys.argv[1])
    else:
        # Full load: python load_raw_orders.py
        full_load()
