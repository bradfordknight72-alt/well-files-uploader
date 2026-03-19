# interval_detailsGH.py - COMPLETE FINAL MERGE SCRIPT (all issues fixed)

import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2
from psycopg2.extras import execute_values
from shutil import move

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
    processed_dir = os.path.join(folder_path, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    logger.info(f"=== Starting interval import: {folder_path} ===")
    print(f"\n=== Importing Drilling Intervals: {folder_path} ===")

    files = [os.path.join(root, f) for root, dirs, fs in os.walk(folder_path)
             for f in fs if f.lower().endswith('.xlsx') and "processed" not in root]

    with tqdm(total=len(files), desc="Interval Details", unit="file") as pbar:
        for fpath in files:
            fname = os.path.basename(fpath)
            try:
                inserted = upload_interval_details(fpath)
                logger.info(f"SUCCESS: {fname} → {inserted} intervals")
                move(fpath, os.path.join(processed_dir, fname))
                logger.info(f"Moved {fname} to processed folder")
            except Exception as e:
                logger.error(f"FAILED {fname}: {e}")
                print(f"FAILED {fname}: {e}")
            pbar.update(1)

def upload_interval_details(file_path):
    df = pd.read_excel(file_path, sheet_name='Sheet1', header=None)
    logger.info(f"Loaded {len(df)} rows from Sheet1")

    # Extract well_id from row 2, column H
    well_name_raw = clean_value(df.iloc[1, 7])
    if not well_name_raw:
        logger.warning("No well name found")
        return 0

    conn = get_neon_connection()
    cur = conn.cursor()
    cur.execute('SELECT id FROM "Wells" WHERE lower(well_name) = lower(%s)', (well_name_raw.strip(),))
    row = cur.fetchone()
    if not row:
        cur.execute('SELECT id FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f"%{well_name_raw}%",))
        row = cur.fetchone()
    if not row:
        logger.warning(f"No well match for '{well_name_raw}'")
        cur.close()
        conn.close()
        return 0
    well_id = row[0]
    cur.close()
    conn.close()

    # Interval names on row 5, starting column D, every 4 columns
    interval_row = 4
    interval_cols = [c for c in range(3, df.shape[1], 4) if clean_value(df.iloc[interval_row, c])]

    logger.info(f"Detected {len(interval_cols)} intervals")

    conn = get_neon_connection()
    cur = conn.cursor()
    total_inserted = 0

    for col in interval_cols:
        interval_name = clean_value(df.iloc[interval_row, col])
        if not interval_name or len(str(interval_name)) < 3:
            continue

        if 'mobilization' in interval_name.lower():
            logger.info(f"Skipping products for Mobilization: {interval_name}")
            continue

        fluid_type = clean_value(df.iloc[interval_row + 1, col])
        depth_range = clean_value(df.iloc[interval_row + 2, col])
        length_ft = safe_float(df.iloc[interval_row + 3, col])
        date_range = clean_value(df.iloc[interval_row + 4, col])
        days = safe_int(df.iloc[interval_row + 5, col])
        drilling_days = safe_int(df.iloc[interval_row + 6, col])

        start_depth, end_depth = parse_depth_range(depth_range)
        start_date, end_date = parse_date_range(date_range)

        data = (well_id, interval_name, fluid_type, start_depth, end_depth,
                length_ft, start_date, end_date, days, drilling_days)

        cur.execute("""
            INSERT INTO "DrillingIntervals" (
                well_id, interval_name, fluid_type, start_depth_ft, end_depth_ft,
                length_ft, start_date, end_date, days, drilling_days
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (well_id, interval_name) DO NOTHING
            RETURNING id
        """, data)
        result = cur.fetchone()
        conn.commit()

        if result:
            interval_id = result[0]
        else:
            cur.execute('SELECT id FROM "DrillingIntervals" WHERE well_id = %s AND interval_name = %s', (well_id, interval_name))
            interval_id = cur.fetchone()[0]

        total_inserted += 1

        # Products - skip blank/"0" and summary rows but continue for page 2
        product_batch = []
        for r in range(interval_row + 12, len(df)):
            product = clean_value(df.iloc[r, col])
            if not product:
                break
            lower = product.lower()
            if lower in ['', '0'] or any(term in lower for term in ['product cost', 'mud volume', 'total cost', 'initial volume', 'end volume', 'mud treated', 'mud consumption']):
                continue

            uom = clean_value(df.iloc[r, col - 1])
            qty = safe_float(df.iloc[r, col + 3])
            cost = safe_float(df.iloc[r, col + 6])

            if qty is None and cost is None:
                continue
            product_batch.append((well_id, interval_id, interval_name, product, uom, qty, cost))

        if product_batch:
            try:
                execute_values(cur, """
                    INSERT INTO "IntervalProducts" 
                    (well_id, interval_id, interval_name, product, uom, quantity, cost)
                    VALUES %s
                """, product_batch)
                conn.commit()
                logger.info(f"Inserted {len(product_batch)} products for {interval_name}")
            except Exception as e:
                logger.error(f"Product insert failed for {interval_name}: {e}")

    cur.close()
    conn.close()
    return total_inserted

def run_interval_import():
    folder = os.path.join("uploads", "interval_details")
    process_interval_folder(folder)
    return "Interval details import completed successfully"

if __name__ == "__main__":
    run_interval_import()
