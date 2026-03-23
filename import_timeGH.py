# timeGH.py - Production importer for high-frequency drilling time records
import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2
from psycopg2.extras import execute_values

# Setup logging
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
        host=NEON_HOST, port=NEON_PORT, database=NEON_DATABASE,
        user=NEON_USER, password=NEON_PASSWORD, sslmode="require"
    )

# ── Helpers ──────────────────────────────────────────────────────────────
def clean_value(val):
    if pd.isna(val) or val == '': return None
    return str(val).strip()

def safe_float(val):
    if val is None or pd.isna(val): return None
    try: return float(val)
    except: return None

def strip_time_prefix(filename):
    name = str(filename).strip()
    for prefix in ['Time_', 'TIME_', 'time_', 'Time ', 'TIME ', 'time ']:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    name = name.replace('.csv', '').replace('.xlsx', '').strip()
    return name

def find_well_id(filename):
    conn = get_neon_connection()
    cur = conn.cursor()
    cur.execute('SELECT id FROM "Wells" WHERE filename = %s LIMIT 1', (filename,))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close(); return row[0]
    
    clean_name = strip_time_prefix(filename)
    print(f"DEBUG - Clean name from filename: '{clean_name}'")
    logger.info(f"DEBUG - Clean name from filename: '{clean_name}'")
    
    cur.execute('SELECT id FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f'%{clean_name}%',))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row else None

# ── Upload function (synthetic Days logic) ───────────────────────────────
def upload_time_records(file_path, downsample_every=1):
    filename = os.path.basename(file_path)
    print(f"  Processing: {filename}")
    well_id = find_well_id(filename)
    if not well_id:
        print(f"    WARNING: No well match — skipping")
        logger.warning(f"No well match for {filename}")
        return 0

    if file_path.lower().endswith('.xlsx'):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)

    if downsample_every > 1:
        df = df.iloc[::downsample_every]

    print(f"    Loaded {len(df)} rows")

    current_days = 0.0
    batch = []
    inserted = 0

    for _, row in df.iterrows():
        date_str = clean_value(row.get('YYYY/MM/DD'))
        time_str = clean_value(row.get('HH:MM:SS'))
        if not date_str or not time_str:
            continue

        try:
            full_dt = pd.to_datetime(f"{date_str} {time_str}", errors='coerce')
            date_val = full_dt.date()
            time_val = full_dt.time()
        except:
            continue

        days_val = current_days
        current_days += 0.006944

        batch.append((
            well_id, date_val, time_val, days_val,
            safe_float(row.get('Hole Depth (feet)')),
            safe_float(row.get('Bit Depth (feet)')),
            safe_float(row.get('Rate Of Penetration (ft_per_hr)')),
            safe_float(row.get('Hook Load (klbs)')),
            safe_float(row.get('Differential Pressure (psi)')),
            safe_float(row.get('Total Pump Output (gal_per_min)')),
            safe_float(row.get('Convertible Torque (kft_lb)')),
            safe_float(row.get('Interpolated TVD (feet)')),
            clean_value(row.get('Memos'))
        ))

        if len(batch) >= 500:
            conn = get_neon_connection()
            cur = conn.cursor()
            execute_values(cur,
                """INSERT INTO "Time" (well_id, date, time, days, hole_depth_ft, bit_depth_ft, rop_ft_hr, hook_load_klbs, differential_pressure_psi, total_pump_output_gpm, convertible_torque_kft_lb, tvd_ft, memos)
                VALUES %s ON CONFLICT (well_id, date, time) DO NOTHING""",
                batch
            )
            conn.commit()
            inserted += len(batch)
            batch = []
            cur.close()
            conn.close()

    if batch:
        conn = get_neon_connection()
        cur = conn.cursor()
        execute_values(cur, """INSERT INTO "Time" (well_id, date, time, days, hole_depth_ft, bit_depth_ft, rop_ft_hr, hook_load_klbs, differential_pressure_psi, total_pump_output_gpm, convertible_torque_kft_lb, tvd_ft, memos) VALUES %s ON CONFLICT (well_id, date, time) DO NOTHING""", batch)
        conn.commit()
        inserted += len(batch)
        cur.close()
        conn.close()

    print(f"→ Inserted {inserted} records")
    return inserted

# ── Batch processor ──────────────────────────────────────────────────────
def process_folder(folder_path, downsample_every=1):
    print(f"\n=== Importing Time Records (downsample every {downsample_every}) ===")
    files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
             for f in files if f.lower().endswith(('.xlsx', '.csv'))]
    
    total_inserted = 0
    with tqdm(total=len(files), desc="Time Records", unit="file") as pbar:
        for file_path in files:
            inserted = upload_time_records(file_path, downsample_every)
            total_inserted += inserted
            pbar.update(1)
    
    print(f"\n=== Complete ===")
    print(f"Total time records inserted: {total_inserted}")

def run_time_import(downsample_every=1):
    folder = os.path.join("uploads", "time")
    process_folder(folder, downsample_every)
    return "Time import completed successfully"

if __name__ == "__main__":
    run_time_import(downsample_every=1)
