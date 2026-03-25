# import_timeGH.py - Production importer for Render/FastAPI (matches latest local time.py)
import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    filename='time_import_log.txt',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

# Neon connection
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

def clean_value(val):
    if pd.isna(val) or val == '': return None
    return str(val).strip()

def safe_float(val):
    if val is None or pd.isna(val): return None
    try: return float(val)
    except: return None

def strip_prefixes(filename):
    name = str(filename).strip()
    prefixes = ['Time_', 'TIME_', 'time_', 'Time ', 'TIME ', 'time ',
                'Coterra_', 'COTERRA_', 'coterra_',
                'FME_', 'FME3_', 'FME ', 'FME3 ']
    for prefix in prefixes:
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
    
    clean_name = strip_prefixes(filename)
    print(f"DEBUG - Clean name from filename: '{clean_name}'")
    logger.info(f"DEBUG - Clean name from filename: '{clean_name}'")
    
    cur.execute('SELECT id FROM "Wells" WHERE well_name ILIKE %s LIMIT 1', (f'%{clean_name}%',))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row else None

def upload_time_records(file_path, downsample_every=5):
    filename = os.path.basename(file_path)
    print(f"Processing: {filename}")
    well_id = find_well_id(filename)
    if not well_id:
        print(f"    WARNING: No well match — skipping")
        logger.warning(f"No well match for {filename}")
        return 0

    try:
        if file_path.lower().endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path, 
                             on_bad_lines='skip', 
                             engine='python',
                             quotechar='"',
                             doublequote=True,
                             dtype=str)

            numeric_cols = ['Days', 'Hole Depth (feet)', 'Bit Depth (feet)', 
                            'Rate Of Penetration (ft_per_hr)', 'Hook Load (klbs)',
                            'Differential Pressure (psi)', 'Total Pump Output (gal_per_min)',
                            'Convertible Torque (kft_lb)']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        if downsample_every > 1:
            df = df.iloc[::downsample_every]
            print(f"    Downsampled: keeping every {downsample_every}th row")

        print(f"    Loaded {len(df)} rows")

    except Exception as e:
        print(f"    ERROR reading {filename}: {e}")
        logger.error(f"Failed to read {filename}: {e}")
        return 0

    current_days = 0.0
    batch = []
    inserted = 0
    skipped_bad = 0
    prev_depth = None
    row_num = 0

    for _, row in df.iterrows():
        row_num += 1
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

        depth = safe_float(row.get('Hole Depth (feet)'))

        is_early_row = row_num <= 500
        max_allowed_jump = 5000 if is_early_row else 1000

        skip_reason = None
        if depth is None or depth < 0:
            skip_reason = f"negative or null depth ({depth})"
        elif prev_depth is not None and abs(depth - prev_depth) > max_allowed_jump:
            skip_reason = f"jump too large: {prev_depth} → {depth} (diff {abs(depth - prev_depth):.1f} ft)"

        if skip_reason:
            skipped_bad += 1
            print(f"    Skipped row {row_num}: {skip_reason}")
            logger.info(f"Skipped row {row_num} in {filename}: {skip_reason}")
            continue

        prev_depth = depth

        days_val = current_days
        current_days += 0.03472

        batch.append((
            well_id, date_val, time_val, days_val,
            depth,
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

    print(f"→ Inserted {inserted} records ({skipped_bad} bad depth rows skipped)")
    logger.info(f"Finished {filename}: {inserted} inserted, {skipped_bad} skipped")
    return inserted

def process_folder(folder_path, downsample_every=5):
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

def run_time_import(downsample_every=5):
    folder = os.path.join("uploads", "time")
    process_folder(folder, downsample_every)
    return "Time import completed successfully"

if __name__ == "__main__":
    run_time_import(downsample_every=5)
