# import_pason_codes.py
# Standalone: import Pason Time Code data from CSV/Excel into 'PasonCodes' table
# Now using psycopg2 + Neon database connection

import pandas as pd
import os
from tqdm import tqdm
import logging
from Levenshtein import distance as lev_distance  # pip install python-Levenshtein
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

def normalize_well_name(name):
    if not name:
        return ''
    name = name.strip().upper()
    name = ' '.join(name.split())  # collapse spaces
    for prefix in ['BPX_', 'BPX ', 'FME_', 'FME ', 'BRAVO KILO ']:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    return name

def get_existing_well_names():
    conn = get_neon_connection()
    cur = conn.cursor()
    cur.execute('SELECT well_name FROM "Wells"')
    names = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return names

def suggest_well_matches(well_name_from_file, existing_wells, top_n=3):
    distances = [(well, lev_distance(well_name_from_file.lower(), well.lower())) for well in existing_wells]
    distances.sort(key=lambda x: x[1])
    return distances[:top_n]

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

def find_well_id(well_name_raw):
    well_name_norm = normalize_well_name(well_name_raw)
    logger.info(f"Normalized well name: '{well_name_norm}' (original: '{well_name_raw}')")

    conn = get_neon_connection()
    cur = conn.cursor()

    # 1. Case-insensitive exact match on original
    cur.execute('SELECT id, well_name FROM "Wells" WHERE lower(well_name) = lower(%s)', (well_name_raw.strip(),))
    row = cur.fetchone()
    if row:
        cur.close()
        conn.close()
        return row[0], row[1], "exact original"

    # 2. Exact match on normalized
    cur.execute('SELECT id, well_name FROM "Wells" WHERE well_name = %s', (well_name_norm,))
    row = cur.fetchone()
    if row:
        cur.close()
        conn.close()
        return row[0], row[1], "exact normalized"

    # 3. Partial match
    cur.execute('SELECT id, well_name FROM "Wells" WHERE well_name ILIKE %s LIMIT 3', (f"%{well_name_norm}%",))
    rows = cur.fetchall()
    if rows:
        cur.close()
        conn.close()
        return rows[0][0], rows[0][1], "partial"

    # 4. Fuzzy fallback
    existing = get_existing_well_names()
    candidates = []
    for db_name in existing:
        db_norm = normalize_well_name(db_name)
        dist = lev_distance(well_name_norm, db_norm)
        if dist <= 8:
            candidates.append((dist, db_name))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        best_db_name = candidates[0][1]
        cur.execute('SELECT id, well_name FROM "Wells" WHERE well_name = %s', (best_db_name,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0], best_db_name, f"fuzzy (dist {candidates[0][0]})"

    cur.close()
    conn.close()
    return None, None, None

def upload_pason_codes(file_path):
    filename = os.path.basename(file_path)
    logger.info(f"Processing Pason file: {filename}")

    # Extract well name from filename
    well_name_raw = filename.replace('.xlsx', '').replace('.csv', '').strip()
    well_name_raw = well_name_raw.replace('_', ' ')
    well_id, matched_name, match_type = find_well_id(well_name_raw)

    if well_id is None:
        logger.warning(f"No match for '{well_name_raw}' in Wells")
        existing = get_existing_well_names()
        suggestions = suggest_well_matches(well_name_raw, existing)
        if suggestions:
            logger.info(f"Suggested matches:")
            for sugg, dist in suggestions:
                logger.info(f"  - {sugg} (distance {dist})")
            print(f"No match for {filename} — suggested fixes: {[s[0] for s in suggestions]}")
        return 0

    logger.info(f"Matched '{well_name_raw}' → '{matched_name}' (ID {well_id}) via {match_type}")

    # Load data
    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(file_path, header=0)
        else:
            df = pd.read_excel(file_path, sheet_name='Sheet1', header=0)

        logger.info(f"Loaded {len(df)} Pason records from {filename}")
        logger.info(f"Detected columns: {list(df.columns)}")

    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        return 0

    inserted = 0
    skipped = 0
    batch = []

    for idx, row in df.iterrows():
        rig_name = clean_value(row.get('Rig name'))
        well_name_from_file = clean_value(row.get('Well name'))
        pason_date = clean_value(row.get('Date'))
        shift = clean_value(row.get('Shift'))
        sequence = safe_int(row.get('Sequence'))
        from_time = clean_value(row.get('From time'))
        to_time = clean_value(row.get('To time'))
        hours = safe_float(row.get('Hours'))
        time_code = clean_value(row.get('Time code'))
        time_code_desc = clean_value(row.get('Time Code Description'))
        sub_code = clean_value(row.get('Sub code'))
        sub_code_desc = clean_value(row.get('Sub Code Description'))
        details = clean_value(row.get('Details'))

        if not well_name_from_file:
            logger.debug(f"Skipping row {idx} - missing well name")
            skipped += 1
            continue

        # Combine rig + well if rig exists
        if rig_name:
            well_name_from_file = f"{rig_name} {well_name_from_file}".strip()

        # Skip if duplicate (well_id + date + from_time)
        conn = get_neon_connection()
        cur = conn.cursor()
        cur.execute(
            'SELECT id FROM "PasonCodes" WHERE well_id = %s AND date = %s AND from_time = %s LIMIT 1',
            (well_id, pason_date, from_time)
        )
        if cur.fetchone():
            skipped += 1
            cur.close()
            conn.close()
            continue
        cur.close()
        conn.close()

        pason_data = (
            well_id,
            rig_name,
            well_name_from_file,
            pason_date,
            shift,
            sequence,
            from_time,
            to_time,
            hours,
            time_code,
            time_code_desc,
            sub_code,
            sub_code_desc,
            details
        )

        batch.append(pason_data)

        # Batch insert every 100 rows
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
                    ON CONFLICT (well_id, date, from_time) DO NOTHING
                    """,
                    batch
                )
                conn.commit()
                inserted += len(batch)
            except Exception as e:
                logger.error(f"Batch insert failed: {str(e)}")
                skipped += len(batch)
            finally:
                cur.close()
                conn.close()
            batch = []

    # Final batch
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
                ON CONFLICT (well_id, date, from_time) DO NOTHING
                """,
                batch
            )
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            logger.error(f"Final batch insert failed: {str(e)}")
            skipped += len(batch)
        finally:
            cur.close()
            conn.close()

    logger.info(f"Inserted {inserted} Pason rows for {filename} (well_id {well_id}), skipped {skipped}")
    return inserted

def process_folder(folder_path):
    print(f"\n=== Importing Pason Codes: {folder_path} ===")
    logger.info(f"Batch started for Pason Codes: {folder_path}")

    excel_files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
                   for f in files if f.lower().endswith(('.xlsx', '.csv'))]

    total_files = len(excel_files)
    print(f"Found {total_files} files")
    logger.info(f"Found {total_files} files")

    if total_files == 0:
        print("No files found.")
        return

    total_inserted = 0

    with tqdm(total=total_files, desc="Pason Codes", unit="file") as pbar:
        for file_path in excel_files:
            inserted = upload_pason_codes(file_path)
            total_inserted += inserted
            pbar.update(1)

    print(f"\n=== Complete ===")
    print(f"Total Pason rows inserted: {total_inserted}")
    logger.info(f"Batch complete. Total inserted: {total_inserted}")

def run_pason_import():
    # This is the function app.py will call
    folder = os.path.join("uploads", "pason")
    process_folder(folder)
    return "Pason codes import completed successfully"

if __name__ == "__main__":
    run_pason_import()
