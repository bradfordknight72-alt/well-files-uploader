# import_pason_codesGH.py
# FINAL DEBUG + ACCURATE UI MESSAGE VERSION

import pandas as pd
import os
from tqdm import tqdm
import logging
from Levenshtein import distance as lev_distance
import psycopg2
from psycopg2.extras import execute_values

# ── Logging setup ────────────────────────────────────────────────────────
logging.basicConfig(
    filename='pason_codes_import_log.txt',
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
        host=NEON_HOST, port=NEON_PORT, database=NEON_DATABASE,
        user=NEON_USER, password=NEON_PASSWORD, sslmode="require"
    )

# ── Helpers ──────────────────────────────────────────────────────────────
def clean_value(val):
    if pd.isna(val) or val == '': return None
    return str(val).strip()

def normalize_well_name(name):
    if not name: return ''
    name = str(name).strip().upper()
    name = ' '.join(name.split())
    for prefix in ['BPX_', 'BPX ', 'FME_', 'FME ', 'BRAVO KILO ']:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    return name

def find_well_id(well_name_raw):
    well_name_norm = normalize_well_name(well_name_raw)
    logger.info(f"Normalized well name: '{well_name_norm}' (original: '{well_name_raw}')")
    conn = get_neon_connection()
    cur = conn.cursor()
    cur.execute('SELECT id, well_name FROM "Wells" WHERE lower(well_name) = lower(%s)', (well_name_raw.strip(),))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close(); return row[0], row[1], "exact original"
    cur.execute('SELECT id, well_name FROM "Wells" WHERE well_name = %s', (well_name_norm,))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close(); return row[0], row[1], "exact normalized"
    cur.execute('SELECT id, well_name FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f"%{well_name_norm}%",))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close(); return row[0], row[1], "partial"
    cur.close(); conn.close()
    return None, None, None

def upload_pason_codes(file_path):
    filename = os.path.basename(file_path)
    logger.info(f"Processing Pason file: {filename}")

    well_name_raw = filename.replace('.xlsx','').replace('.csv','').replace('_',' ').strip()
    well_id, matched_name, match_type = find_well_id(well_name_raw)
    if well_id is None:
        logger.warning(f"No match for '{well_name_raw}' in Wells")
        return 0
    logger.info(f"Matched '{well_name_raw}' → '{matched_name}' (ID {well_id}) via {match_type}")

    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(file_path, header=0)
        else:
            df = pd.read_excel(file_path, sheet_name='Sheet1', header=0)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        logger.info(f"Loaded {len(df)} Pason records from {filename}")
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        return 0

    inserted = 0
    skipped = 0
    batch = []
    for idx, row in df.iterrows():
        rig_name = clean_value(row.get('Rig name'))
        well_name_from_file = clean_value(row.get('Well name'))
        pason_date = row.get('Date') if 'Date' in df.columns else None
        shift = clean_value(row.get('Shift'))
        sequence = row.get('Sequence')
        from_time = clean_value(row.get('From time'))
        to_time = clean_value(row.get('To time'))
        hours = row.get('Hours')
        time_code = clean_value(row.get('Time code'))
        time_code_desc = clean_value(row.get('Time Code Description'))
        sub_code = clean_value(row.get('Sub code'))
        sub_code_desc = clean_value(row.get('Sub Code Description'))
        details = clean_value(row.get('Details'))

        if pd.isna(pason_date):
            skipped += 1
            continue

        if rig_name and well_name_from_file:
            well_name_from_file = f"{rig_name} {well_name_from_file}".strip()

        pason_data = (well_id, rig_name, well_name_from_file, pason_date, shift, sequence,
                      from_time, to_time, hours, time_code, time_code_desc, sub_code,
                      sub_code_desc, details)

        batch.append(pason_data)

        if len(batch) >= 100:
            conn = get_neon_connection()
            cur = conn.cursor()
            try:
                execute_values(cur,
                    """
                    INSERT INTO "PasonCodes" (
                        well_id, rig_name, well_name, date, shift, sequence,
                        from_time, to_time, hours, time_code, time_code_desc,
                        sub_code, sub_code_desc, details
                    ) VALUES %s
                    ON CONFLICT ON CONSTRAINT unique_pason_time_block DO NOTHING
                    """,
                    batch)
                conn.commit()
                inserted += len(batch)
            except Exception as e:
                logger.error(f"Batch insert failed: {str(e)}")
                skipped += len(batch)
            finally:
                cur.close(); conn.close()
            batch = []

    if batch:
        conn = get_neon_connection()
        cur = conn.cursor()
        try:
            execute_values(cur,
                """
                INSERT INTO "PasonCodes" (
                    well_id, rig_name, well_name, date, shift, sequence,
                    from_time, to_time, hours, time_code, time_code_desc,
                    sub_code, sub_code_desc, details
                ) VALUES %s
                ON CONFLICT ON CONSTRAINT unique_pason_time_block DO NOTHING
                """,
                batch)
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            logger.error(f"Final batch insert failed: {str(e)}")
            skipped += len(batch)
        finally:
            cur.close(); conn.close()

    logger.info(f"Inserted {inserted} Pason rows for {filename} (well_id {well_id}), skipped {skipped}")
    return inserted

def process_folder():
    print("\n=== Importing Pason Codes ===")
    logger.info("Batch started for Pason Codes")

    # === MAXIMUM DEBUG: show everything Render sees ===
    logger.info(f"DEBUG: Current working directory = {os.getcwd()}")
    logger.info(f"DEBUG: uploads folder exists? {os.path.exists('uploads')}")
    if os.path.exists('uploads'):
        logger.info(f"DEBUG: Top-level uploads contents = {os.listdir('uploads')}")

    files = []
    for root, dirs, filenames in os.walk("uploads"):
        logger.info(f"DEBUG: Walking folder → {root} | files = {filenames}")
        for f in filenames:
            if 'pason' in f.lower() and f.lower().endswith(('.csv', '.xlsx')):
                full_path = os.path.join(root, f)
                files.append(full_path)
                logger.info(f"DEBUG: FOUND Pason file → {full_path}")

    total_files = len(files)
    print(f"Found {total_files} Pason files")
    logger.info(f"Found {total_files} Pason files")

    if total_files == 0:
        print("No files found.")
        return 0

    total_inserted = 0
    with tqdm(total=total_files, desc="Pason Codes", unit="file") as pbar:
        for file_path in files:
            inserted = upload_pason_codes(file_path)
            total_inserted += inserted
            pbar.update(1)

    print(f"\n=== Complete ===")
    print(f"Total Pason rows inserted: {total_inserted}")
    logger.info(f"Batch complete. Total inserted: {total_inserted}")
    return total_inserted

def run_pason_import():
    inserted = process_folder()
    if inserted > 0:
        msg = f"Pason codes import completed successfully ({inserted} rows inserted)"
    else:
        msg = "Pason codes import completed — but 0 files found (check Render log for DEBUG details)"
    return msg

if __name__ == "__main__":
    run_pason_import()
