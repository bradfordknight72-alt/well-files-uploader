# import_drilling_intervals.py
# Standalone: import interval summary from Sheet1 into DrillingIntervals table
# Now using psycopg2 + Neon database connection

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
NEON_HOST = "ep-blue-wind-anin6o30-pooler.c-6.us-east-1.aws.neon.tech"      # ← your Neon host
NEON_PORT = 5432
NEON_DATABASE = "neondb"                                        # ← your database name
NEON_USER = "neondb_owner"                                # ← your Neon user
NEON_PASSWORD = "npg_uIt2cPJTE4aL"                  # ← your Neon password

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
    skipped = 0
    failed = 0

    with tqdm(total=total_files, desc="Drilling Intervals", unit="file") as pbar:
        for file_path in excel_files:
            filename = os.path.basename(file_path)

            # Extract well_name and operator from Sheet1, column H (index 7), rows 1–2 (0-based rows 0–1)
            try:
                df_temp = pd.read_excel(file_path, sheet_name='Sheet1', header=None)
                operator = clean_value(df_temp.iloc[0, 7])
                well_name_raw = clean_value(df_temp.iloc[1, 7])

                if not well_name_raw:
                    logger.warning(f"No well name found in Sheet1 col H row 3 for {filename} — skipping")
                    pbar.update(1)
                    failed += 1
                    continue

                logger.info(f"Extracted from Sheet1 col H: Operator '{operator}', Well '{well_name_raw}'")
            except Exception as e:
                logger.error(f"Failed to read well/operator from Sheet1 col H for {filename}: {e}")
                pbar.update(1)
                failed += 1
                continue

            # Improved well matching (using Neon)
            try:
                well_name_norm = ' '.join(well_name_raw.upper().split())
                conn = get_neon_connection()
                cur = conn.cursor()

                cur.execute('SELECT id FROM "Wells" WHERE lower(well_name) = lower(%s)', (well_name_raw.strip(),))
                row = cur.fetchone()
                if row:
                    well_id = row[0]
                    logger.info(f"Exact case-insensitive match: '{well_name_raw}' → ID {well_id}")
                else:
                    cur.execute('SELECT id FROM "Wells" WHERE well_name = %s', (well_name_norm,))
                    row = cur.fetchone()
                    if row:
                        well_id = row[0]
                        logger.info(f"Normalized match: '{well_name_raw}' → ID {well_id}")
                    else:
                        cur.execute('SELECT id FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f"%{well_name_norm}%",))
                        row = cur.fetchone()
                        if row:
                            well_id = row[0]
                            logger.info(f"Partial match: '{well_name_raw}' → ID {well_id}")
                        else:
                            logger.warning(f"No match for '{well_name_raw}' in Wells")
                            cur.close()
                            conn.close()
                            pbar.update(1)
                            failed += 1
                            continue

                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Failed to find well_id for {filename}: {e}")
                pbar.update(1)
                failed += 1
                continue

            # Import intervals and products
            try:
                inserted = upload_interval_details(file_path, well_id)
                processed += 1
                logger.info(f"Success: {filename} - inserted {inserted}")
            except Exception as e:
                logger.error(f"FAILED {filename}: {str(e)}")
                print(f"FAILED {filename}: {e}")
                failed += 1

            pbar.update(1)

    # Final summary
    summary = f"""
=== Batch Complete ===
Processed successfully: {processed}
Failed: {failed}
"""
    print(summary)
    logger.info(summary.strip())

def upload_interval_details(file_path, well_id):
    filename = os.path.basename(file_path)
    logger.info(f"Processing Sheet1 intervals for {filename} (well_id {well_id})")

    try:
        df = pd.read_excel(file_path, sheet_name='Sheet1', header=None)
        logger.info(f"Loaded {len(df)} rows from Sheet1 in {filename}")
        if len(df) < 12:
            logger.warning(f"Sheet1 too short in {filename} — skipping intervals")
            return 0

        # Fixed Interval row: always row 5 (0-based index 4)
        interval_row_idx = 4
        if len(df) <= interval_row_idx:
            logger.warning(f"Sheet1 too short to reach row 5 ({len(df)} rows) — skipping")
            return 0

        # Quick validation: confirm it's the Interval row
        row_text = ' '.join(str(df.iloc[interval_row_idx, j]) for j in range(10) if pd.notna(df.iloc[interval_row_idx, j])).lower()
        if 'interval' not in row_text:
            logger.warning(f"Row 5 is not an Interval row in {filename} — skipping")
            return 0

        logger.info(f"Interval row index: {interval_row_idx}, total rows: {len(df)}")

        inserted = 0
        start_col = 2

        conn = get_neon_connection()
        cur = conn.cursor()

        for col_idx in range(start_col, df.shape[1]):
            interval_name = clean_value(df.iloc[interval_row_idx, col_idx])
            if not interval_name or len(str(interval_name)) < 3:
                continue

            # Skip product parsing for Mobilization columns
            if 'mobilization' in interval_name.lower():
                logger.info(f"Skipping product parsing for Mobilization interval: {interval_name}")
                # Insert metadata anyway
                fluid_type = clean_value(df.iloc[interval_row_idx + 1, col_idx]) if interval_row_idx + 1 < len(df) else None
                depth_range = clean_value(df.iloc[interval_row_idx + 2, col_idx]) if interval_row_idx + 2 < len(df) else None
                length_ft = safe_float(df.iloc[interval_row_idx + 3, col_idx]) if interval_row_idx + 3 < len(df) else None
                date_range = clean_value(df.iloc[interval_row_idx + 4, col_idx]) if interval_row_idx + 4 < len(df) else None
                days = safe_int(df.iloc[interval_row_idx + 5, col_idx]) if interval_row_idx + 5 < len(df) else None
                drilling_days = safe_int(df.iloc[interval_row_idx + 6, col_idx]) if interval_row_idx + 6 < len(df) else None

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

                # Duplicate check
                cur.execute(
                    'SELECT id FROM "DrillingIntervals" WHERE well_id = %s AND interval_name = %s',
                    (well_id, interval_name)
                )
                if cur.fetchone():
                    continue

                try:
                    cur.execute(
                        """
                        INSERT INTO "DrillingIntervals" (
                            well_id, interval_name, fluid_type, start_depth_ft, end_depth_ft,
                            length_ft, start_date, end_date, days, drilling_days
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        data
                    )
                    conn.commit()
                    inserted += 1
                except Exception as e:
                    logger.error(f"Insert failed for Mobilization {interval_name}: {e}")
                    conn.rollback()

                continue  # skip products for Mobilization

            # Dynamic metadata lookup - search down column A for labels (no fixed +N offsets)
            fluid_type = None
            depth_range = None
            length_ft = None
            date_range = None
            days = None
            drilling_days = None

            max_search_rows = 30  # safety limit
            for r in range(interval_row_idx + 1, min(interval_row_idx + max_search_rows + 1, len(df))):
                label = str(df.iloc[r, 0]).strip().lower()  # column A = labels
                value = clean_value(df.iloc[r, col_idx])

                if 'mud' in label or 'fluid' in label:
                    fluid_type = value
                elif 'depth' in label:
                    depth_range = value
                elif 'length' in label:
                    length_ft = safe_float(value)
                elif 'date' in label:
                    date_range = value
                elif 'days' in label and 'drilling' not in label:
                    days = safe_int(value)
                elif 'drilling days' in label:
                    drilling_days = safe_int(value)

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

            # Duplicate check
            cur.execute(
                'SELECT id FROM "DrillingIntervals" WHERE well_id = %s AND interval_name = %s',
                (well_id, interval_name)
            )
            if cur.fetchone():
                logger.info(f"Skipping duplicate: {interval_name}")
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO "DrillingIntervals" (
                        well_id, interval_name, fluid_type, start_depth_ft, end_depth_ft,
                        length_ft, start_date, end_date, days, drilling_days
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    data
                )
                conn.commit()
                inserted += 1
                logger.info(f"Inserted interval: {interval_name}")
            except Exception as e:
                logger.error(f"Insert failed for {interval_name}: {e}")
                conn.rollback()

            # ── Interval Products ──────────────────────────────────────────────
            product_start_row = interval_row_idx + 12

            product_batch = []

            for row_offset in range(product_start_row, len(df)):
                product_cell = clean_value(df.iloc[row_offset, col_idx])
                if not product_cell:
                    break

                cell_lower = product_cell.lower().strip()
                if any(kw in cell_lower for kw in ['total', 'summary', 'cost($)', 'used', 'conc.', 'vol.']):
                    break

                qty = safe_float(df.iloc[row_offset, col_idx + 1])
                conc = safe_float(df.iloc[row_offset, col_idx + 2])
                vol = safe_float(df.iloc[row_offset, col_idx + 3])
                cost = safe_float(df.iloc[row_offset, col_idx + 4])

                product_data = (
                    well_id,
                    interval_name,
                    product_cell,
                    qty,
                    cost,
                )

                # Duplicate check
                cur.execute(
                    'SELECT id FROM "IntervalProducts" WHERE well_id = %s AND interval_name = %s AND product = %s',
                    (well_id, interval_name, product_cell)
                )
                if cur.fetchone():
                    continue

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
                    inserted += len(product_batch)
                except Exception as e:
                    logger.error(f"Product batch insert failed for {interval_name}: {e}")
                    conn.rollback()

        cur.close()
        conn.close()
        return inserted

    except Exception as e:
        logger.error(f"Failed Sheet1 intervals in {filename}: {str(e)}")
        print(f"Interval import failed for {filename}: {e}")
        return 0

def run_interval_import():
    # This is the function app.py will call
    folder = os.path.join("uploads", "interval_details")
    process_interval_folder(folder)
    return "Interval details import completed successfully"

if __name__ == "__main__":
    run_interval_import()
