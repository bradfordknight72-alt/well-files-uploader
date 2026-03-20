# import_timeGH.py
# Standalone: import 10-second drilling time records from CSV/Excel into 'Time' table
# Now using separate date + time columns (exactly matching your Neon table)

import pandas as pd
import os
from tqdm import tqdm
import logging
from Levenshtein import distance as lev_distance
import psycopg2
from psycopg2.extras import execute_values

# ── Logging setup ────────────────────────────────────────────────────────
logging.basicConfig(
    filename='time_import_log.txt',
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

def normalize_well_name(name):
    if not name:
        return ''
    name = name.strip().upper()
    name = ' '.join(name.split())
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

def find_well_id(well_name_raw):
    well_name_norm = normalize_well_name(well_name_raw)
    logger.info(f"Normalized well name: '{well_name_norm}' (original: '{well_name_raw}')")

    conn = get_neon_connection()
    cur = conn.cursor()

    # 1. Exact match on original
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

def upload_time_records(file_path):
    filename = os.path.basename(file_path)
    logger.info(f"Processing time file: {filename}")

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
                logger.info(f" - {sugg} (distance {dist})")
            print(f"No match for {filename} — suggested fixes: {[s[0] for s in suggestions]}")
        return 0

    logger.info(f"Matched '{well_name_raw}' → '{matched_name}' (ID {well_id}) via {match_type}")

    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(file_path, header=0)
        else:
            df = pd.read_excel(file_path, sheet_name='Sheet1', header=0)

        logger.info(f"Loaded {len(df)} time records from {filename}")
        logger.info(f"Detected columns: {list(df.columns)}")

    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        return 0

    inserted = 0
    skipped = 0
    batch = []

    for idx, row in df.iterrows():
        # ── Date & Time (split columns) ──
        date_str = clean_value(row.get('YYYY/MM/DD')) or clean_value(row.iloc[0])
        time_str = clean_value(row.get('HH:MM:SS')) or clean_value(row.iloc[1])

        if not date_str or not time_str:
            skipped += 1
            continue

        try:
            full_dt = pd.to_datetime(f"{date_str} {time_str}")
            date_val = full_dt.date()
            time_val = full_dt.time()
        except Exception as e:
            logger.warning(f"Invalid date/time in row {idx}: {e}")
            skipped += 1
            continue

        # ── Exact column mapping to your Neon Time table ──
        data_tuple = (
            well_id,
            date_val,                                      # date (DATE)
            time_val,                                      # time (TIME)
            safe_float(row.get('Days') or row.iloc[2]),    # days
            safe_float(row.get('Hole Depth (feet)') or row.iloc[3]),   # hole_depth_ft
            safe_float(row.get('Bit Depth (feet)') or row.iloc[4]),    # bit_depth_ft
            safe_float(row.get('Rate Of Penetration (ft_per_hr)') or row.iloc[5]),  # rop_ft_hr
            None,                                          # wob_klbs (not in CSV)
            None,                                          # rotary_rpm (not in CSV)
            None,                                          # standpipe_pressure_psi (not in CSV)
            safe_float(row.get('Hook Load (klbs)') or row.iloc[6]),     # hook_load_klbs
            safe_float(row.get('Differential Pressure (psi)') or row.iloc[7]),  # differential_pressure_psi
            None,                                          # flow_percent (not in CSV)
            safe_float(row.get('Total Pump Output (gal_per_min)') or row.iloc[8]),  # total_pump_output_gpm
            safe_float(row.get('Convertible Torque (kft_lb)') or row.iloc[9]),     # convertible_torque_kft_lb
            safe_float(row.get('Interpolated TVD (feet)') or row.iloc[10]),       # tvd_ft
            clean_value(row.get('Memos') or row.iloc[11])  # memos
        )

        batch.append(data_tuple)

        # Batch insert every 500 rows (fast & memory-safe)
        if len(batch) >= 500:
            conn = get_neon_connection()
            cur = conn.cursor()
            try:
                execute_values(cur,
                    """
                    INSERT INTO "Time" (
                        well_id, date, time, days, hole_depth_ft, bit_depth_ft, rop_ft_hr,
                        wob_klbs, rotary_rpm, standpipe_pressure_psi, hook_load_klbs,
                        differential_pressure_psi, flow_percent, total_pump_output_gpm,
                        convertible_torque_kft_lb, tvd_ft, memos
                    ) VALUES %s
                    ON CONFLICT (well_id, date, time) DO NOTHING
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
                INSERT INTO "Time" (
                    well_id, date, time, days, hole_depth_ft, bit_depth_ft, rop_ft_hr,
                    wob_klbs, rotary_rpm, standpipe_pressure_psi, hook_load_klbs,
                    differential_pressure_psi, flow_percent, total_pump_output_gpm,
                    convertible_torque_kft_lb, tvd_ft, memos
                ) VALUES %s
                ON CONFLICT (well_id, date, time) DO NOTHING
                """,
                batch
            )
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            logger.error(f"Final batch failed: {str(e)}")
            skipped += len(batch)
        finally:
            cur.close()
            conn.close()

    logger.info(f"Inserted {inserted} time records for {filename} (well_id {well_id}), skipped {skipped}")
    return inserted

def process_folder(folder_path):
    print(f"\n=== Importing Time Records: {folder_path} ===")
    logger.info(f"Batch started for Time records: {folder_path}")

    time_files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
                  for f in files if f.lower().endswith(('.xlsx', '.csv')) and f.lower().startswith('time_')]

    total_files = len(time_files)
    print(f"Found {total_files} Time files")

    if total_files == 0:
        print("No Time files found.")
        return

    total_inserted = 0
    with tqdm(total=total_files, desc="Time Records", unit="file") as pbar:
        for file_path in time_files:
            inserted = upload_time_records(file_path)
            total_inserted += inserted
            pbar.update(1)

    print(f"\n=== Complete ===")
    print(f"Total time records inserted: {total_inserted}")
    logger.info(f"Batch complete. Total inserted: {total_inserted}")

def run_time_import():
    folder = os.path.join("uploads", "time")
    process_folder(folder)
    return "Time import completed successfully"

if __name__ == "__main__":
    run_time_import()
