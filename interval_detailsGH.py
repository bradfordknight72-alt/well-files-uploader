# import_drilling_intervals.py
# Standalone: import interval summary from Sheet1 into DrillingIntervals + IntervalProducts

import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2
from psycopg2.extras import execute_values

# ── Logging setup ────────────────────────────────────────────────────────
logging.basicConfig(
    filename='drilling_intervals_import_log.txt',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

console = logging.StreamHandler()
console.setLevel(logging.WARNING)
logger.addHandler(console)

# ── Neon database connection details ─────────────────────────────────────
NEON_HOST = "ep-blue-wind-anin6o30-pooler.c-6.us-east-1.aws.neon.tech"
NEON_PORT = 5432
NEON_DATABASE = "neondb"
NEON_USER = "neondb_owner"
NEON_PASSWORD = "npg_uIt2cPJTE4aL"

def get_neon_connection():
    return psycopg2.connect(
        host=NEON_HOST,
        port=NEON_PORT,
        database=NEON_DATABASE,
        user=NEON_USER,
        password=NEON_PASSWORD,
        sslmode="require"
    )

# ── Helpers ──────────────────────────────────────────────────────────────
def clean_value(val):
    if pd.isna(val) or val == '':
        return None
    return str(val).strip()

def safe_float(val):
    if val is None or pd.isna(val):
        return None
    try:
        return float(val)
    except:
        return None

def safe_int(val):
    if val is None or pd.isna(val):
        return None
    try:
        return int(float(val))
    except:
        return None

def parse_depth_range(depth_str):
    if not depth_str or '-' not in str(depth_str):
        return None, None
    parts = str(depth_str).split('-')
    start = safe_float(parts[0].strip())
    end = safe_float(parts[1].strip()) if len(parts) > 1 else None
    return start, end

def parse_date_range(date_str):
    if not date_str or '-' not in str(date_str):
        return None, None
    parts = str(date_str).split('-')
    start = clean_value(parts[0].strip())
    end = clean_value(parts[1].strip()) if len(parts) > 1 else None
    return start, end

def find_section_start(df, keyword, column=0, case_insensitive=True):
    """Find the first row where the specified column contains the keyword."""
    flags = 0 if case_insensitive else None
    mask = df.iloc[:, column].astype(str).str.contains(keyword, na=False, case=case_insensitive, flags=flags)
    if mask.any():
        return mask.idxmax()
    return None

def process_interval_folder(folder_path):
    print(f"\n=== Importing Drilling Intervals from Sheet1: {folder_path} ===")
    logger.info(f"Batch started for DrillingIntervals: {folder_path}")

    excel_files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
                   for f in files if f.lower().endswith('.xlsx')]

    total_files = len(excel_files)
    print(f"Found {total_files} files")
    logger.info(f"Found {total_files} files")

    if total_files == 0:
        print("No files found.")
        return

    processed = 0
    failed = 0

    with tqdm(total=total_files, desc="Drilling Intervals", unit="file") as pbar:
        for file_path in excel_files:
            filename = os.path.basename(file_path)

            try:
                df_temp = pd.read_excel(file_path, sheet_name='Sheet1', header=None)
                operator = clean_value(df_temp.iloc[0, 7])
                well_name_raw = clean_value(df_temp.iloc[1, 7])

                if not well_name_raw:
                    logger.warning(f"No well name found for {filename} — skipping")
                    pbar.update(1)
                    failed += 1
                    continue
            except Exception as e:
                logger.error(f"Failed to read well name from {filename}: {e}")
                pbar.update(1)
                failed += 1
                continue

            # Find well_id
            try:
                well_name_norm = ' '.join(well_name_raw.upper().split())
                conn = get_neon_connection()
                cur = conn.cursor()
                cur.execute('SELECT id FROM "Wells" WHERE lower(well_name) = lower(%s)', (well_name_raw.strip(),))
                row = cur.fetchone()
                if not row:
                    cur.execute('SELECT id FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f"%{well_name_norm}%",))
                    row = cur.fetchone()
                if not row:
                    logger.warning(f"No match for '{well_name_raw}' in Wells")
                    cur.close()
                    conn.close()
                    pbar.update(1)
                    failed += 1
                    continue
                well_id = row[0]
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Failed to find well_id for {filename}: {e}")
                pbar.update(1)
                failed += 1
                continue

            # Import
            try:
                inserted = upload_interval_details(file_path, well_id)
                processed += 1
                logger.info(f"Success: {filename} - inserted {inserted} rows")
            except Exception as e:
                logger.error(f"FAILED {filename}: {str(e)}")
                print(f"FAILED {filename}: {e}")
                failed += 1

            pbar.update(1)

    summary = f"""
=== Batch Complete ===
Processed successfully: {processed}
Failed: {failed}
"""
    print(summary)
    logger.info(summary.strip())

def upload_interval_details(file_path, well_id):
    filename = os.path.basename(file_path)
    logger.info(f"Processing intervals for {filename} (well_id {well_id})")

    try:
        df = pd.read_excel(file_path, sheet_name='Sheet1', header=None)
        logger.info(f"Loaded {len(df)} rows from Sheet1")

        # Find interval header row
        interval_header_row = find_section_start(df, 'Interval', column=0)
        if interval_header_row is None:
            logger.warning(f"No 'Interval' header found in {filename}")
            return 0

        logger.info(f"Interval header found at row {interval_header_row}")

        inserted = 0
        start_col = 2

        conn = get_neon_connection()
        cur = conn.cursor()

        for col_idx in range(start_col, df.shape[1]):
            interval_name = clean_value(df.iloc[interval_header_row, col_idx])
            if not interval_name or len(str(interval_name)) < 3:
                continue

            # Extract metadata from the column
            fluid_type = clean_value(df.iloc[interval_header_row + 1, col_idx]) if interval_header_row + 1 < len(df) else None
            depth_range = clean_value(df.iloc[interval_header_row + 2, col_idx]) if interval_header_row + 2 < len(df) else None
            length_ft = safe_float(df.iloc[interval_header_row + 3, col_idx]) if interval_header_row + 3 < len(df) else None
            date_range = clean_value(df.iloc[interval_header_row + 4, col_idx]) if interval_header_row + 4 < len(df) else None
            days = safe_int(df.iloc[interval_header_row + 5, col_idx]) if interval_header_row + 5 < len(df) else None
            drilling_days = safe_int(df.iloc[interval_header_row + 6, col_idx]) if interval_header_row + 6 < len(df) else None

            start_depth, end_depth = parse_depth_range(depth_range)
            start_date, end_date = parse_date_range(date_range)

            data = (
                well_id,
                interval_name,
                fluid_type,
                start_depth,
                end_depth,
                length_ft,
                start_date,
                end_date,
                days,
                drilling_days,
            )

            # Insert interval
            try:
                cur.execute(
                    """
                    INSERT INTO "DrillingIntervals" (
                        well_id, interval_name, fluid_type, start_depth_ft, end_depth_ft,
                        length_ft, start_date, end_date, days, drilling_days
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (well_id, interval_name) DO NOTHING
                    """,
                    data
                )
                conn.commit()
                inserted += 1
            except Exception as e:
                logger.error(f"Interval insert failed for {interval_name}: {e}")
                conn.rollback()

            # ── Product Usage (IntervalProducts) ─────────────────────────────────
            product_start_row = interval_header_row + 12
            product_batch = []

            for r in range(product_start_row, len(df)):
                product_cell = clean_value(df.iloc[r, col_idx])
                if not product_cell:
                    break

                qty = safe_float(df.iloc[r, col_idx + 1])
                cost = safe_float(df.iloc[r, col_idx + 4])  # adjust column if needed

                if qty is None and cost is None:
                    continue

                product_data = (
                    well_id,
                    interval_name,
                    product_cell,
                    qty,
                    cost,
                )

                product_batch.append(product_data)

            # Batch insert products
            if product_batch:
                try:
                    execute_values(cur,
                        """
                        INSERT INTO "IntervalProducts" (
                            well_id, interval_name, product, quantity, cost
                        ) VALUES %s
                        ON CONFLICT (well_id, interval_name, product) DO NOTHING
                        """,
                        product_batch
                    )
                    conn.commit()
                    logger.info(f"Inserted {len(product_batch)} products for interval {interval_name}")
                except Exception as e:
                    logger.error(f"Product batch failed for {interval_name}: {e}")
                    conn.rollback()

        cur.close()
        conn.close()
        return inserted

    except Exception as e:
        logger.error(f"Failed to process {filename}: {str(e)}")
        return 0

def run_interval_import():
    folder = os.path.join("uploads", "interval_details")
    process_interval_folder(folder)
    return "Interval details import completed successfully"

if __name__ == "__main__":
    run_interval_import()
