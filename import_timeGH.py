# timeGH.py - Batch importer for high-frequency drilling time records into Neon database
import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2

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
        host=NEON_HOST,
        port=NEON_PORT,
        database=NEON_DATABASE,
        user=NEON_USER,
        password=NEON_PASSWORD,
        sslmode="require"
    )

def insert_row(table, data):
    conn = get_neon_connection()
    cur = conn.cursor()
    columns = list(data.keys())
    placeholders = ','.join(['%s'] * len(columns))
    sql = f'INSERT INTO "{table}" ({",".join(columns)}) VALUES ({placeholders})'
    cur.execute(sql, list(data.values()))
    conn.commit()
    cur.close()
    conn.close()

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
    except (ValueError, TypeError):
        return None

def find_well_id(filename):
    conn = get_neon_connection()
    cur = conn.cursor()
    # 1. Exact filename match in Wells
    cur.execute('SELECT id FROM "Wells" WHERE filename = %s LIMIT 1', (filename,))
    row = cur.fetchone()
    if row:
        cur.close()
        conn.close()
        return row[0]
    # 2. Partial well name fallback
    well_guess = filename.replace('Time_', '').replace('.csv', '').replace('.xlsx', '').strip()
    cur.execute('SELECT id FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f'%{well_guess}%',))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

# ── Upload one file ──────────────────────────────────────────────────────
def upload_time_records(file_path):
    filename = os.path.basename(file_path)
    print(f"  Processing: {filename}")
    logger.info(f"Processing time file: {filename}")

    well_id = find_well_id(filename)
    if not well_id:
        print(f"    WARNING: No matching well found for {filename} — skipping")
        logger.warning(f"No well match for {filename}")
        return 0

    try:
        if file_path.lower().endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} time records")
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        return 0

    inserted = 0
    for _, row in df.iterrows():
        data = {
            "well_id": well_id,
            "date": clean_value(row.get('YYYY/MM/DD')),
            "time": clean_value(row.get('HH:MM:SS')),
            "days": safe_float(row.get('Days')),
            "hole_depth_ft": safe_float(row.get('Hole Depth (feet)')),
            "bit_depth_ft": safe_float(row.get('Bit Depth (feet)')),
            "rop_ft_hr": safe_float(row.get('Rate Of Penetration (ft_per_hr)')),
            "hook_load_klbs": safe_float(row.get('Hook Load (klbs)')),
            "differential_pressure_psi": safe_float(row.get('Differential Pressure (psi)')),
            "total_pump_output_gpm": safe_float(row.get('Total Pump Output (gal_per_min)')),
            "convertible_torque_kft_lb": safe_float(row.get('Convertible Torque (kft_lb)')),
            "tvd_ft": safe_float(row.get('Interpolated TVD (feet)')),
            "memos": clean_value(row.get('Memos'))
        }

        try:
            insert_row('Time', data)
            inserted += 1
        except Exception as e:
            logger.warning(f"Skipped row due to error: {e}")

    logger.info(f"Inserted {inserted} time records for {filename} (well_id {well_id})")
    print(f"→ Inserted {inserted} time records")
    return inserted

# ── Batch processor ──────────────────────────────────────────────────────
def process_folder(folder_path):
    logger.info(f"=== Starting Time Records import: {folder_path} ===")
    print(f"\n=== Importing Time Records: {folder_path} ===")
    
    files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
             for f in files if f.lower().endswith(('.xlsx', '.csv'))]
    
    total_files = len(files)
    print(f"Found {total_files} time files")
    
    if total_files == 0:
        print("No files found.")
        return
    
    total_inserted = 0
    with tqdm(total=total_files, desc="Time Records", unit="file") as pbar:
        for file_path in files:
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
