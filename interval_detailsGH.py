# interval_detailsGH.py - FIXED VERSION (works with your actual Trinity Gas file)

import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(filename='drilling_intervals_import_log.txt', level=logging.INFO,
                    format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger()

console = logging.StreamHandler()
console.setLevel(logging.WARNING)
logger.addHandler(console)

# ── Neon connection ─────────────────────────────────────────────────────
NEON_HOST = "ep-blue-wind-anin6o30-pooler.c-6.us-east-1.aws.neon.tech"
NEON_PORT = 5432
NEON_DATABASE = "neondb"
NEON_USER = "neondb_owner"
NEON_PASSWORD = "npg_uIt2cPJTE4aL"

def get_neon_connection():
    return psycopg2.connect(host=NEON_HOST, port=NEON_PORT, database=NEON_DATABASE,
                            user=NEON_USER, password=NEON_PASSWORD, sslmode="require")

def clean_value(val):
    if pd.isna(val) or val == '': return None
    return str(val).strip()

def safe_float(val):
    if val is None or pd.isna(val): return None
    try: return float(val)
    except: return None

def safe_int(val):
    if val is None or pd.isna(val): return None
    try: return int(float(val))
    except: return None

def parse_depth_range(s):
    if not s or '-' not in str(s): return None, None
    parts = str(s).split('-')
    return safe_float(parts[0].strip()), safe_float(parts[1].strip()) if len(parts)>1 else None

def parse_date_range(s):
    if not s or '-' not in str(s): return None, None
    parts = str(s).split('-')
    return clean_value(parts[0].strip()), clean_value(parts[1].strip()) if len(parts)>1 else None

def process_interval_folder(folder_path):
    logger.info(f"=== Starting interval import: {folder_path} ===")
    print(f"\n=== Importing Drilling Intervals: {folder_path} ===")

    files = [os.path.join(root, f) for root, dirs, fs in os.walk(folder_path)
             for f in fs if f.lower().endswith('.xlsx')]

    total = len(files)
    processed = 0

    with tqdm(total=total, desc="Interval Details", unit="file") as pbar:
        for fpath in files:
            fname = os.path.basename(fpath)
            logger.info(f"Processing {fname}")

            try:
                inserted = upload_interval_details(fpath)
                processed += 1
                logger.info(f"SUCCESS: {fname} → {inserted} rows inserted")
            except Exception as e:
                logger.error(f"FAILED {fname}: {e}")
                print(f"FAILED {fname}: {e}")

            pbar.update(1)

    print(f"\n=== Complete ===\nProcessed {processed} files")
    logger.info(f"Batch complete. Processed {processed} files")

def upload_interval_details(file_path):
    df = pd.read_excel(file_path, sheet_name='Sheet1', header=None)

    # INTERVAL NAMES ARE ON ROW 5 (0-based index 4), every 4 columns starting from column C (index 2)
    interval_row = 4
    interval_names = []
    interval_cols = []

    for c in range(2, df.shape[1], 4):   # columns 2, 6, 10, 14, ...
        name = clean_value(df.iloc[interval_row, c])
        if name and len(str(name)) > 2:
            interval_names.append(name)
            interval_cols.append(c)

    logger.info(f"Detected intervals: {interval_names}")

    conn = get_neon_connection()
    cur = conn.cursor()

    total_inserted = 0

    for i, interval_name in enumerate(interval_names):
        col = interval_cols[i]

        # Metadata (rows below interval name)
        fluid_type = clean_value(df.iloc[interval_row + 1, col])
        depth_range = clean_value(df.iloc[interval_row + 2, col])
        length_ft = safe_float(df.iloc[interval_row + 3, col])
        date_range = clean_value(df.iloc[interval_row + 4, col])
        days = safe_int(df.iloc[interval_row + 5, col])
        drilling_days = safe_int(df.iloc[interval_row + 6, col])

        start_depth, end_depth = parse_depth_range(depth_range)
        start_date, end_date = parse_date_range(date_range)

        # Insert Interval
        data = (well_id, interval_name, fluid_type, start_depth, end_depth,
                length_ft, start_date, end_date, days, drilling_days)

        cur.execute("""
            INSERT INTO "DrillingIntervals" (
                well_id, interval_name, fluid_type, start_depth_ft, end_depth_ft,
                length_ft, start_date, end_date, days, drilling_days
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (well_id, interval_name) DO NOTHING
        """, data)
        conn.commit()

        # ── Products ─────────────────────────────────────────────────────
        product_batch = []
        for r in range(interval_row + 12, len(df)):
            product = clean_value(df.iloc[r, col])
            if not product or product.lower() in ['total', 'summary', 'cost']:
                break
            qty = safe_float(df.iloc[r, col + 1])
            cost = safe_float(df.iloc[r, col + 4])

            if qty is None and cost is None:
                continue

            product_batch.append((well_id, interval_name, product, qty, cost))

        if product_batch:
            try:
                execute_values(cur, """
                    INSERT INTO "IntervalProducts" (well_id, interval_name, product, quantity, cost)
                    VALUES %s
                    ON CONFLICT (well_id, interval_name, product) DO NOTHING
                """, product_batch)
                conn.commit()
                logger.info(f"Inserted {len(product_batch)} products for {interval_name}")
            except Exception as e:
                logger.error(f"Product insert failed for {interval_name}: {e}")
                conn.rollback()

        total_inserted += 1

    cur.close()
    conn.close()
    return total_inserted

def run_interval_import():
    folder = os.path.join("uploads", "interval_details")
    process_interval_folder(folder)
    return "Interval details import completed successfully"

if __name__ == "__main__":
    run_interval_import()
