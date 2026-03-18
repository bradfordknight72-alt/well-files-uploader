# Recaps.py - Batch importer for mud recap Excels into Neon database

import pandas as pd
import os
from tqdm import tqdm
import logging
import psycopg2

# Setup logging to file (detailed output goes here)
logging.basicConfig(
    filename='mud_import_log.txt',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

# Optional: also log to console at WARNING level or higher
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

def process_folder(folder_path):
    logger.info(f"=== Starting batch processing of folder: {folder_path} ===")
    print(f"\n=== Batch Processing Folder: {folder_path} ===")
    
    excel_files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
                   for f in files if f.lower().endswith('.xlsx')]
    
    total_files = len(excel_files)
    logger.info(f"Found {total_files} .xlsx files")
    print(f"Found {total_files} .xlsx files")
    
    if total_files == 0:
        logger.info("No files found — exiting")
        print("No files found.")
        return
    
    processed = 0
    skipped = 0
    failed = 0
    
    with tqdm(total=total_files, desc="Importing recaps", unit="file", leave=True) as pbar:
        for file_path in excel_files:
            filename = os.path.basename(file_path).strip()
            logger.info(f"Processing file: {filename}")
            
            # Duplicate check (using Neon)
            try:
                conn = get_neon_connection()
                cur = conn.cursor()
                cur.execute('SELECT id FROM "Wells" WHERE filename = %s LIMIT 1', (filename,))
                if cur.fetchone():
                    logger.info(f"Already exists — skipping {filename}")
                    skipped += 1
                    pbar.update(1)
                    cur.close()
                    conn.close()
                    continue
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Duplicate check failed for {filename}: {str(e)}")
                print(f"Duplicate check error: {e}")
            
            # Upload
            try:
                upload_file(file_path)
                processed += 1
                logger.info(f"Success: {filename}")
            except Exception as e:
                logger.error(f"FAILED {filename}: {str(e)}")
                print(f"FAILED {filename}: {e}")
                failed += 1
            
            pbar.update(1)
    
    # Final summary
    summary = f"""
=== Batch Complete ===
Processed successfully: {processed}
Skipped (already in DB): {skipped}
Failed: {failed}
"""
    print(summary)
    logger.info(summary.strip())

def upload_file(file_path):
    filename = os.path.basename(file_path)
    print(f"  Processing: {filename}")

    try:
        df = pd.read_excel(file_path, sheet_name='Sheet1', header=None, engine='openpyxl')
        print(f"    Excel loaded — {len(df)} rows")
    except Exception as e:
        print(f"    Failed to read Excel: {e}")
        return False

    # Direct well info reads (kept exactly as in your original script)
    well_info = {
        "operator": clean_value(df.iloc[15, 13]),
        "well_name": clean_value(df.iloc[16, 13]),
        "field_block": clean_value(df.iloc[17, 13]),
        "section_township_range": clean_value(df.iloc[18, 13]),
        "county_parish": clean_value(df.iloc[19, 13]),
        "state_province": clean_value(df.iloc[20, 13]),
        "spud_date": clean_value(df.iloc[22, 13]),
        "rig": clean_value(df.iloc[25, 13]),
        "report_no": clean_value(df.iloc[28, 13]),
        "report_date": clean_value(df.iloc[29, 13]),
    }

    well_name = str(well_info["well_name"] or "").strip()
    if not well_name or len(well_name) < 5:
        well_name = filename.replace('Recap_', '').replace('_1.xlsx', '').strip()

    report_no_str = str(well_info["report_no"] or "")
    max_report = 0
    if '-' in report_no_str:
        try:
            max_report = int(report_no_str.split('-')[-1])
        except ValueError:
            pass

    well_data = {
        "filename": filename,
        "well_name": well_name,
        "operator": well_info["operator"],
        "field_block": well_info["field_block"],
        "section_township_range": well_info["section_township_range"],
        "county_parish": well_info["county_parish"],
        "state_province": well_info["state_province"],
        "spud_date": well_info["spud_date"],
        "rig": well_info["rig"],
        "report_no": report_no_str,
        "report_date": well_info["report_date"],
    }

    try:
        insert_row('Wells', well_data)
        # Get the generated well_id
        conn = get_neon_connection()
        cur = conn.cursor()
        cur.execute('SELECT id FROM "Wells" WHERE filename = %s ORDER BY id DESC LIMIT 1', (filename,))
        well_id = cur.fetchone()[0]
        cur.close()
        conn.close()
        logger.info(f"Wells inserted successfully – ID = {well_id} for {filename}")
    except Exception as e:
        logger.error(f"Wells insert failed for {filename}: {e}")
        print(f"Wells insert failed: {e}")
        return False

    # ── Surveys table ──────────────────────────────────────────
    survey_header_row = find_section_start(df, 'Rpt No.', column=0)
    if survey_header_row is None:
        print("→ Could not find 'Rpt No.' header → skipping Surveys")
    else:
        print(f"→ Found 'Rpt No.' header at row {survey_header_row} (0-based)")
        survey_start = survey_header_row + 1
        survey_nrows = 0
        for i in range(survey_start, len(df)):
            val = df.iloc[i, 0]
            if pd.isna(val):
                break
            cleaned = str(clean_value(val)).strip()
            if cleaned == '' or not cleaned.replace('.', '').isdigit():
                if survey_nrows > 0:
                    break
                continue
            survey_nrows += 1

        survey_nrows = max(survey_nrows, max_report) if max_report > 0 else survey_nrows
        print(f"→ Planning to read ~{survey_nrows} survey rows")

        try:
            survey_df = pd.read_excel(
                file_path,
                sheet_name='Sheet1',
                skiprows=survey_header_row,
                nrows=survey_nrows + 5,
                header=0,
                usecols="A:Z"
            )

            survey_df.columns = [
                col.strip()
                   .replace('_x000D_\n', '')
                   .replace('\n', ' ')
                   .replace(' (ft)', '_ft')
                   .replace(' (deg)', '_deg')
                   .replace(' (lbf)', '_lbf')
                   .replace(' (rpm)', '')
                   .replace(' (ft/hr)', '_ft_hr')
                   .replace('(', '')
                   .replace(')', '')
                   .strip()
                for col in survey_df.columns
            ]

            rename_map = {
                'MDft': 'md_ft',
                'TVDft': 'tvd_ft',
                'Inc.deg': 'inc_deg',
                'Azi.deg': 'azi_deg',
                'WOBlbf': 'wob_lbf',
                'Rot. wt.lbf': 'rot_wt_lbf',
                'S/O wt.lbf': 'so_wt_lbf',
                'P/U wt.lbf': 'pu_wt_lbf',
                'RPMrpm': 'rpm',
                'ROPft/hr': 'rop_ft_hr',
                'Depth drilledft': 'depth_drilled_ft',
                'Drilling interval': 'drilling_interval',
                'Formation': 'formation',
                'Engineer': 'engineer',
                'Activity': 'activity'
            }
            survey_df = survey_df.rename(columns=rename_map)

            print("\nSurveys preview (first 8 rows):")
            print(survey_df.head(8).to_string(index=False))
            print("\nColumns:", list(survey_df.columns))
            print("-" * 60)

            inserted = 0
            for idx, row in survey_df.iterrows():
                rpt = clean_value(row.get('Rpt No.'))
                if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                    print(f"  Skipping row {idx} — invalid Rpt No.: {rpt}")
                    continue

                numeric_cols = ['md_ft', 'tvd_ft', 'inc_deg', 'azi_deg', 'wob_lbf', 'rot_wt_lbf']
                skip_row = False
                for col in numeric_cols:
                    val = row.get(col)
                    if val is not None and pd.notna(val):
                        val_str = str(val).strip()
                        if not val_str.replace('.', '').replace('-', '').replace('0', '').isdigit():
                            if val_str.lower() not in ['0', '0.0', '0.00']:
                                skip_row = True
                                break
                if skip_row:
                    print(f"  Skipping row {idx} — text in numeric columns")
                    continue

                data = {
                    "well_id": well_id,
                    "rpt_no": int(float(rpt)),
                    "date": clean_value(row.get('Date')),
                    "md_ft": clean_value(row.get('md_ft')),
                    "tvd_ft": clean_value(row.get('tvd_ft')),
                    "inc_deg": clean_value(row.get('inc_deg')),
                    "azi_deg": clean_value(row.get('azi_deg')),
                    "wob_lbf": clean_value(row.get('wob_lbf')),
                    "rot_wt_lbf": clean_value(row.get('rot_wt_lbf')),
                    "so_wt_lbf": clean_value(row.get('so_wt_lbf')),
                    "pu_wt_lbf": clean_value(row.get('pu_wt_lbf')),
                    "rpm": clean_value(row.get('rpm')),
                    "rop_ft_hr": clean_value(row.get('rop_ft_hr')),
                    "depth_drilled_ft": clean_value(row.get('depth_drilled_ft')),
                    "drilling_interval": clean_value(row.get('drilling_interval')),
                    "formation": clean_value(row.get('formation')),
                    "engineer": clean_value(row.get('engineer')),
                    "activity": clean_value(row.get('activity')),
                }

                try:
                    insert_row('Surveys', data)
                    inserted += 1
                except Exception as e:
                    print(f"  Surveys insert failed on row {idx}: {e}")

            print(f"→ Successfully inserted {inserted} survey rows")

        except Exception as e:
            print(f"Failed to read or insert Surveys: {e}")

    # ── Mud Water ──────────────────────────────────────────────────────────
    mud_water_header_row = find_section_start(df, 'Mud Water', column=0)
    if mud_water_header_row is None:
        print("→ Could not find 'Mud Water' header → skipping Mud Water")
    else:
        print(f"→ Found 'Mud Water' header at row {mud_water_header_row} (0-based)")
        mud_water_start = mud_water_header_row + 1
        mud_water_nrows = 0
        for i in range(mud_water_start, len(df)):
            val = df.iloc[i, 0]
            if pd.isna(val):
                break
            cleaned = str(clean_value(val)).strip()
            if cleaned == '' or not cleaned.replace('.', '').isdigit():
                if mud_water_nrows > 0:
                    break
                continue
            mud_water_nrows += 1

        try:
            mud_water_df = pd.read_excel(
                file_path,
                sheet_name='Sheet1',
                skiprows=mud_water_header_row,
                nrows=mud_water_nrows + 5,
                header=0,
                usecols="A:Z"
            )

            mud_water_df.columns = [
                col.strip()
                   .replace('_x000D_\n', '')
                   .replace('\n', ' ')
                   .replace(' (gal)', '_gal')
                   .replace(' (lb/gal)', '_lb_gal')
                   .replace(' (lb/bbl)', '_lb_bbl')
                   .replace(' (ppg)', '_ppg')
                   .replace(' (lbm/bbl)', '_lbm_bbl')
                   .replace('(', '')
                   .replace(')', '')
                   .strip()
                for col in mud_water_df.columns
            ]

            print("\nMud Water preview (first 8 rows):")
            print(mud_water_df.head(8).to_string(index=False))
            print("\nColumns:", list(mud_water_df.columns))
            print("-" * 60)

            inserted = 0
            for idx, row in mud_water_df.iterrows():
                data = {
                    "well_id": well_id,
                    "date": clean_value(row.get('Date')),
                    "water_gal": safe_float(row.get('Water_gal')),
                    "oil_gal": safe_float(row.get('Oil_gal')),
                    "mud_weight_lb_gal": safe_float(row.get('Mud Weight_lb_gal')),
                    "funnel_visc_sec_qt": safe_float(row.get('Funnel Visc_sec_qt')),
                    "pv_cp": safe_float(row.get('PV_cp')),
                    "yp_lb_100ft2": safe_float(row.get('YP_lb_100ft2')),
                    "gels_10sec_10min": clean_value(row.get('Gels_10sec_10min')),
                    "ph": safe_float(row.get('pH')),
                    "chlorides_mg_l": safe_float(row.get('Chlorides_mg_l')),
                    "calcium_mg_l": safe_float(row.get('Calcium_mg_l')),
                    "retort_solids_percent": safe_float(row.get('Retort Solids_percent')),
                    "low_grav_solids_percent": safe_float(row.get('Low Grav Solids_percent')),
                    "high_grav_solids_percent": safe_float(row.get('High Grav Solids_percent')),
                    "sand_percent": safe_float(row.get('Sand_percent')),
                    "mbt_lb_bbl": safe_float(row.get('MBT_lb_bbl')),
                    "remarks": clean_value(row.get('Remarks')),
                }

                try:
                    insert_row('MudWater', data)
                    inserted += 1
                except Exception as e:
                    print(f"  Mud Water insert failed on row {idx}: {e}")

            print(f"→ Successfully inserted {inserted} Mud Water rows")

        except Exception as e:
            print(f"Failed to read or insert Mud Water: {e}")

# ── Mud Properties Oil table ──────────────────────────────────────────
    oil_header = find_section_start(df, 'Properties - oil|Properties -oil|Properties oil|Oil based mud', column=0)
    if oil_header is None:
        print("→ Mud Properties Oil header not found → skipping")
    else:
        print(f"→ Found Mud Oil at row {oil_header}")
        print(f"  Row content check: {df.iloc[oil_header:oil_header+3, 0:5].to_string()}")

        try:
                # Dynamically count only Oil rows
                oil_start = oil_header + 1
                oil_nrows = 0
                for i in range(oil_start, len(df)):
                    val = df.iloc[i, 0]  # column A = Rpt No. or title
                    if pd.isna(val):
                        break
                    cleaned = str(clean_value(val)).strip().lower()
                    if cleaned == '' or not cleaned.replace('.', '').isdigit():
                        # Check if this is the Rheology header (next section for Oil)
                        row_text_a = str(df.iloc[i, 0]).lower()
                        row_text_b = str(df.iloc[i, 1]).lower() if len(df.columns) > 1 else ''
                        rheo_markers = ['rheology', 'rheo', 'gel str', 'rpm']
                        if any(marker in row_text_a or marker in row_text_b for marker in rheo_markers):
                            print(f"  Stopping Oil count at row {i} - detected Rheology section start ('{row_text_a}')")
                            break
                        if oil_nrows > 0:
                            break  # stop after good rows
                        continue
                    # Count only if rpt_no looks valid
                    try:
                        rpt_num = int(cleaned.split('.')[0]) if '.' in cleaned else int(cleaned)
                        if rpt_num > 200:  # safety cap - no well has 200+ Oil reports
                            print(f"  Stopping Oil count - rpt_no {rpt_num} too high")
                            break
                    except ValueError:
                        continue
                    oil_nrows += 1

                print(f"→ Detected {oil_nrows} Oil mud rows")

                oil_df = pd.read_excel(
                    file_path,
                    sheet_name='Sheet1',
                    skiprows=oil_header + 1,
                    nrows=oil_nrows + 2,  # small buffer - matches Water
                    header=0,
                    usecols="A:AA"
                )

                # ── Clean columns (copy your Water cleaning chain here if different) ────────
                oil_df.columns = [
                    col.strip()
                    .replace('_x000D_', '')
                    .replace('\n', ' ')
                    .replace('\r', '')
                    .replace('  ', ' ')
                    .replace(' (', '_(')
                    .replace(' )', ')')
                    .replace('mg/L', '_mg_l')
                    .replace('ppm', '_ppm')
                    .replace('%', '_pct')
                    .replace('CAom', 'caom')
                    .replace('Volt', '_volt')
                    .replace('Aw', '_aw')
                    .replace('cc/cc', '_cc')
                    .replace('pom', '')
                    .replace('1/32in', '_1_32in')
                    .strip()
                    for col in oil_df.columns
                ]

                rename_map = {
                    'Rpt No.': 'rpt_no',
                    'Date': 'date',
                    'MD (ft)': 'md_ft',
                    'Sample from': 'sample_from',
                    'MW (ppg)': 'mw_ppg',
                    'Funnel visc._(sec/qt)': 'funnel_visc_sec_qt',
                    'PV (cP)': 'pv_cp',
                    'YP (lbf/100ft2)': 'yp_lbf_100ft2',
                    '6 RPM': '_6_rpm',
                    'HTHP filtrate (ml/30min)': 'hthp_filtrate_ml_30min',
                    'HTHP cake thickness (1/32in)': 'hthp_cake_thickness_1_32in',
                    'LGS_(_pct)': 'lgs_pct',
                    'Solids_(_pct)': 'solids_pct',
                    'Oil (%)': 'oil_pct',
                    'Water (%)': 'water_pct',
                    'Oil/water ratio': 'oil_water_ratio',
                    'Alkalinity (cc/cc)': 'alkalinity_cc',
                    'Excess lime_(lb/bbl)': 'excess_lime_lb_bbl',
                    'Chlorides (mg/L)': 'chlorides_mg_l',
                    'Solids salt (%)': 'solids_salt_pct',
                    'Salt phase (%)': 'salt_phase_pct',
                    'WPS_(_ppm)': 'wps',
                    'Whole mud Ca (CAom) (mg/L)': 'whole_caom_mg_l',
                    'Electrical stability(_volt)': 'electrical_stability_volt',
                    'Water activity(_aw)': 'water_activity_aw',
                    'Fine LCM_(lb/bbl)': 'fine_lcm_lb_bbl',
                    'Coarse LCM_(lb/bbl)': 'coarse_lcm_lb_bbl'
                }
                oil_df = oil_df.rename(columns=rename_map)

            # Force-fix any lingering uncleaned column names (common after cleaning)
                rename_fixes = {
                    'Oil_(_pct)': 'oil_pct',
                    'Water_(_pct)': 'water_pct',
                    'Solids adjusted for salt_(_pct)': 'solids_salt_pct',
                    'Salt content water phase_(_pct)': 'salt_phase_pct',
                    'Chlorides whole mud_(_mg_l)': 'chlorides_mg_l',
                    'Alkalinity mud_()_(_cc)': 'alkalinity_cc',
                    'Whole mud Ca_(caom)_(_mg_l)': 'whole_caom_mg_l',
                    'HTHP filtrate_(ml/30min)': 'hthp_filtrate_ml_30min',
                    'HTHP cake thickness_(_1_32in)': 'hthp_cake_thickness_1_32in',
                    'MD_(ft)': 'md_ft',
                    'MW_(ppg)': 'mw_ppg',
                    'PV_(cP)': 'pv_cp',
                    'YP_(lbf/100ft2)': 'yp_lbf_100ft2',
                }

                oil_df = oil_df.rename(columns=rename_fixes)

                print("\nColumns after force-fix rename:")
                print(list(oil_df.columns))

                print("\nMud Oil preview (first 8 rows):")
                print(oil_df.head(8).to_string(index=False))
                print("-" * 60)

                inserted_o = 0
                for _, row in oil_df.iterrows():
                    rpt = clean_value(row.get('rpt_no'))
                    if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                        print(f"  Skipping Mud Oil row - invalid rpt_no: {rpt}")
                        continue

                    oil_data = {
                        "well_id": well_id,
                        "rpt_no": int(float(rpt)) if rpt else None,
                        "date": clean_value(row.get('date')),
                        "md_ft": safe_float(row.get('md_ft')),
                        "sample_from": clean_value(row.get('sample_from')),
                        "mw_ppg": safe_float(row.get('mw_ppg')),
                        "funnel_visc_sec_qt": safe_float(row.get('funnel_visc_sec_qt')),
                        "pv_cp": safe_float(row.get('pv_cp')),
                        "yp_lbf_100ft2": safe_float(row.get('yp_lbf_100ft2')),
                        "_6_rpm": safe_float(row.get('_6_rpm')),
                        "hthp_filtrate_ml_30min": safe_float(row.get('hthp_filtrate_ml_30min')),
                        "hthp_cake_thickness_1_32in": safe_float(row.get('hthp_cake_thickness_1_32in')),
                        "lgs_pct": safe_float(row.get('lgs_pct')),
                        "solids_pct": safe_float(row.get('solids_pct')),
                        "oil_pct": safe_float(row.get('oil_pct')),
                        "water_pct": safe_float(row.get('water_pct')),
                        "oil_water_ratio": clean_value(row.get('oil_water_ratio')),
                        "alkalinity_cc": safe_float(row.get('alkalinity_cc')),
                        "excess_lime_lb_bbl": safe_float(row.get('excess_lime_lb_bbl')),
                        "chlorides_mg_l": safe_float(row.get('chlorides_mg_l')),
                        "solids_salt_pct": safe_float(row.get('solids_salt_pct')),
                        "salt_phase_pct": safe_float(row.get('salt_phase_pct')),
                        "wps": safe_float(row.get('wps')),
                        "whole_caom_mg_l": safe_float(row.get('whole_caom_mg_l')),
                        "electrical_stability_volt": safe_float(row.get('electrical_stability_volt')),
                        "water_activity_aw": safe_float(row.get('water_activity_aw')),
                        "fine_lcm_lb_bbl": safe_float(row.get('fine_lcm_lb_bbl')),
                        "coarse_lcm_lb_bbl": safe_float(row.get('coarse_lcm_lb_bbl'))
                    }

                    try:
                        supabase.table('MudPropertiesOil').insert(oil_data).execute()
                        inserted_o += 1
                    except Exception as e:
                        print(f"  Mud Oil insert failed for rpt_no {rpt}: {e}")
                        print("    Data:", oil_data)

                print(f"→ Inserted {inserted_o} mud oil rows")

        except Exception as e:
                print(f"Failed to read or insert Mud Oil: {e}")
# ── Mud Rheology table ──────────────────────────────────────────
    rheo_header = find_section_start(df, 'Rheology|Rheo|Gel Strength|Rheology Section', column=0)
    if rheo_header is None:
        print("→ Rheology header not found → skipping")
    else:
        print(f"→ Found Rheology at row {rheo_header}")
        print(f"  Row content check: {df.iloc[rheo_header:rheo_header+3, 0:5].to_string()}")

        try:
                # Dynamically count only Rheology rows
                rheo_start = rheo_header + 1
                rheo_nrows = 0
                for i in range(rheo_start, len(df)):
                    val = df.iloc[i, 0]  # column A = Rpt No. or title
                    if pd.isna(val):
                        break
                    cleaned = str(clean_value(val)).strip().lower()
                    if cleaned == '' or not cleaned.replace('.', '').isdigit():
                        # Check if this is the next major section (e.g. MBT, No data)
                        row_text_a = str(df.iloc[i, 0]).lower()
                        row_text_b = str(df.iloc[i, 1]).lower() if len(df.columns) > 1 else ''
                        next_markers = ['mbt capacity', 'no data', 'yield point', 'plastic viscosity']
                        if any(marker in row_text_a or marker in row_text_b for marker in next_markers):
                            print(f"  Stopping Rheology count at row {i} - detected next section ('{row_text_a}')")
                            break
                        if rheo_nrows > 0:
                            break  # stop after good rows
                        continue
                    # Count only if rpt_no looks valid
                    try:
                        rpt_num = int(cleaned.split('.')[0]) if '.' in cleaned else int(cleaned)
                        if rpt_num > 200:  # safety cap
                            print(f"  Stopping Rheology count - rpt_no {rpt_num} too high")
                            break
                    except ValueError:
                        continue
                    rheo_nrows += 1

                print(f"→ Detected {rheo_nrows} Rheology rows")

                rheo_df = pd.read_excel(
                    file_path,
                    sheet_name='Sheet1',
                    skiprows=rheo_header + 1,
                    nrows=rheo_nrows + 2,  # small buffer, same as Water
                    header=0,
                    usecols="A:P"
                )

                # ── Clean columns (adapted for Rheology) ──────────────────────────
                rheo_df.columns = [
                    col.strip()
                    .replace('_x000D_', '')
                    .replace('\n', ' ')
                    .replace('\r', '')
                    .replace('  ', ' ')
                    .replace(' (', '_(')
                    .replace(' )', ')')
                    .replace('°F', '_f')
                    .replace('RPM', '_rpm')
                    .replace('Gel Str.', 'gel')
                    .replace('10 sec', '_10sec')
                    .replace('10 min', '_10min')
                    .replace('30 min', '_30min')
                    .replace('lbf/100ft2', 'lbf_100ft2')
                    .replace('(f)', '_f')
                    .replace('(lbf/100ft2)', 'lbf_100ft2')
                    .strip()
                    for col in rheo_df.columns
                ]

                # ── Rename map for Rheology (based on your sample) ────────────────
                rename_map = {
                    'Rpt No.': 'rpt_no',
                    'Date': 'date',
                    'MD_(ft)': 'md_ft',
                    'MW_(ppg)': 'mw_ppg',  # sometimes present
                    'Funnel visc._(sec/qt)': 'funnel_visc_sec_qt',
                    'PV_(cP)': 'pv_cp',
                    'YP_(lbf_100ft2)': 'yp_lbf_100ft2',
                    'Gel str._(10sec)_(lbf_100ft2)': 'gel_10sec',
                    'Gel str._(10min)_(lbf_100ft2)': 'gel_10min',
                    'Gel str._(30min)_(lbf_100ft2)': 'gel_30min',
                    '600': '_600_rpm',
                    '300': '_300_rpm',
                    '200': '_200_rpm',
                    '100': '_100_rpm',
                    '6': '_6_rpm',
                    '3': '_3_rpm',
                }
                rheo_df = rheo_df.rename(columns=rename_map)

                # Manual fixes for common variations
                rheo_df = rheo_df.rename(columns={
                    'Gel (10sec)': 'gel_10sec',
                    'Gel (10min)': 'gel_10min',
                    'Gel (30min)': 'gel_30min',
                    'Gel Str. (10sec)': 'gel_10sec',
                    'Gel Str. (10min)': 'gel_10min',
                    'Gel Str. (30min)': 'gel_30min',
                })

                print("\nMud Rheology preview (first 8 rows):")
                print(rheo_df.head(8).to_string(index=False))
                print("-" * 60)

                inserted_r = 0
                for _, row in rheo_df.iterrows():
                    rpt = clean_value(row.get('rpt_no'))
                    if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                        print(f"  Skipping Mud Rheology row - invalid rpt_no: {rpt}")
                        continue

                    rheo_data = {
                        "well_id": well_id,
                        "rpt_no": int(float(rpt)) if rpt else None,
                        "date": clean_value(row.get('date')),
                        "md_ft": safe_float(row.get('md_ft')),
                        "mw_ppg": safe_float(row.get('mw_ppg')),  # optional
                        "funnel_visc_sec_qt": safe_float(row.get('funnel_visc_sec_qt')),
                        "pv_cp": safe_float(row.get('pv_cp')),
                        "yp_lbf_100ft2": safe_float(row.get('yp_lbf_100ft2')),
                        "gel_10sec": safe_float(row.get('gel_10sec')),
                        "gel_10min": safe_float(row.get('gel_10min')),
                        "gel_30min": safe_float(row.get('gel_30min')),
                        "_600_rpm": safe_float(row.get('_600_rpm')),
                        "_300_rpm": safe_float(row.get('_300_rpm')),
                        "_200_rpm": safe_float(row.get('_200_rpm')),
                        "_100_rpm": safe_float(row.get('_100_rpm')),
                        "_6_rpm": safe_float(row.get('_6_rpm')),
                        "_3_rpm": safe_float(row.get('_3_rpm')),
                    }

                    try:
                        supabase.table('MudRheology').insert(rheo_data).execute()
                        inserted_r += 1
                    except Exception as e:
                        print(f"  Mud Rheology insert failed for rpt_no {rpt}: {e}")
                        print("    Data:", rheo_data)

                print(f"→ Inserted {inserted_r} mud rheology rows")

        except Exception as e:
                print(f"Failed to read or insert Mud Rheology: {e}")
        # ── Mud Solids Analysis table ─────────────────────────────────────────
# ── Mud Solids table ──────────────────────────────────────────
    solids_header = find_section_start(df, 'Solids analysis|Solids Analysis|Solids', column=0)
    if solids_header is None:
        print("→ Solids header not found → skipping")
    else:
        print(f"→ Found Solids at row {solids_header}")
        print(f"  Row content check: {df.iloc[solids_header:solids_header+3, 0:5].to_string()}")


        try:
                # Dynamically count only Solids rows
                solids_start = solids_header + 1
                solids_nrows = 0
                for i in range(solids_start, len(df)):
                    val = df.iloc[i, 0]  # column A = Rpt No. or title
                    if pd.isna(val):
                        break
                    cleaned = str(clean_value(val)).strip().lower()
                    if cleaned == '' or not cleaned.replace('.', '').isdigit():
                        # Check if this is the next major section (e.g. MBT, No data, or end)
                        row_text_a = str(df.iloc[i, 0]).lower()
                        row_text_b = str(df.iloc[i, 1]).lower() if len(df.columns) > 1 else ''
                        next_markers = ['mbt capacity', 'no data', 'yield point', 'plastic viscosity', 'rheology']
                        if any(marker in row_text_a or marker in row_text_b for marker in next_markers):
                            print(f"  Stopping Solids count at row {i} - detected next section ('{row_text_a}')")
                            break
                        if solids_nrows > 0:
                            break  # stop after good rows
                        continue
                    # Count only if rpt_no looks valid
                    try:
                        rpt_num = int(cleaned.split('.')[0]) if '.' in cleaned else int(cleaned)
                        if rpt_num > 200:  # safety cap
                            print(f"  Stopping Solids count - rpt_no {rpt_num} too high")
                            break
                    except ValueError:
                        continue
                    solids_nrows += 1

                print(f"→ Detected {solids_nrows} Solids analysis rows")

                solids_df = pd.read_excel(
                    file_path,
                    sheet_name='Sheet1',
                    skiprows=solids_header + 1,
                    nrows=solids_nrows + 2,  # small buffer, same as Water/Oil/Rheo
                    header=0,
                    usecols="A:O"
                )

                # ── Clean columns (adapted for Solids units) ──────────────────────
                solids_df.columns = [
                    col.strip()
                    .replace('_x000D_', '')
                    .replace('\n', ' ')
                    .replace('\r', '')
                    .replace('  ', ' ')
                    .replace(' (', '_(')
                    .replace(' )', ')')
                    .replace('lb/bbl', '_lb_bbl')
                    .replace('%', '_pct')
                    .replace('(%)', '_pct')
                    .replace('(lb/bbl)', '_lb_bbl')
                    .replace('DS/Bent', 'ds_bent')
                    .replace('Avg. SG', 'avg_sg')
                    .strip()
                    for col in solids_df.columns
                ]

                # ── Rename map for Solids Analysis ────────────────────────────────
                rename_map = {
                    'Rpt No.': 'rpt_no',
                    'Date': 'date',
                    'MD_(ft)': 'md_ft',
                    'LGS_(_pct)': 'lgs_pct',
                    'LGS_(_lb_bbl)': 'lgs_lb_bbl',
                    'HGS_(_pct)': 'hgs_pct',
                    'HGS_(_lb_bbl)': 'hgs_lb_bbl',
                    'Bentonite_(_pct)': 'bentonite_pct',
                    'Bentonite_(_lb_bbl)': 'bentonite_lb_bbl',
                    'Drill solids_(_pct)': 'drill_solids_pct',
                    'Drill solids_(_lb_bbl)': 'drill_solids_lb_bbl',
                    'DS/Bent ratio': 'ds_bent_ratio',
                    'OBM chemicals_(_pct)': 'obm_pct',
                    'OBM chemicals_(_lb_bbl)': 'obm_lb_bbl',
                    ' avg_sg of solids': 'avg_sg_solids',
                }
                solids_df = solids_df.rename(columns=rename_map)

                # Manual fixes for common variations
                solids_df = solids_df.rename(columns={
                    'LGS (%)': 'lgs_pct',
                    'LGS (lb/bbl)': 'lgs_lb_bbl',
                    'HGS (%)': 'hgs_pct',
                    'HGS (lb/bbl)': 'hgs_lb_bbl',
                    'DS/Bent': 'ds_bent_ratio',
                })

                print("\nMud Solids Analysis preview (first 8 rows):")
                print(solids_df.head(8).to_string(index=False))
                print("-" * 60)

                inserted_s = 0
                for _, row in solids_df.iterrows():
                    rpt = clean_value(row.get('rpt_no'))
                    if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                        print(f"  Skipping Mud Solids row - invalid rpt_no: {rpt}")
                        continue

                    solids_data = {
                        "well_id": well_id,
                        "rpt_no": int(float(rpt)) if rpt else None,
                        "date": clean_value(row.get('date')),
                        "md_ft": safe_float(row.get('md_ft')),
                        "lgs_pct": safe_float(row.get('lgs_pct')),
                        "lgs_lb_bbl": safe_float(row.get('lgs_lb_bbl')),
                        "hgs_pct": safe_float(row.get('hgs_pct')),
                        "hgs_lb_bbl": safe_float(row.get('hgs_lb_bbl')),
                        "bentonite_pct": safe_float(row.get('bentonite_pct')),
                        "bentonite_lb_bbl": safe_float(row.get('bentonite_lb_bbl')),
                        "drill_solids_pct": safe_float(row.get('drill_solids_pct')),
                        "drill_solids_lb_bbl": safe_float(row.get('drill_solids_lb_bbl')),
                        "ds_bent_ratio": safe_float(row.get('ds_bent_ratio')),
                        "obm_pct": safe_float(row.get('obm_pct')),
                        "obm_lb_bbl": safe_float(row.get('obm_lb_bbl')),
                        "avg_sg_solids": safe_float(row.get('avg_sg_solids')),
                    }

                    try:
                        supabase.table('MudSolidsAnalysis').insert(solids_data).execute()
                        inserted_s += 1
                    except Exception as e:
                        print(f"  Mud Solids insert failed for rpt_no {rpt}: {e}")
                        print("    Data:", solids_data)

                print(f"→ Inserted {inserted_s} mud solids analysis rows")

        except Exception as e:
                print(f"Failed to read or insert Mud Solids Analysis: {e}")    
# ── Mud Volumes table ──────────────────────────────────────────
    volumes_header = find_section_start(df, 'Volumes|Volume|Daily addition|Start vol.|End vol.', column=0)
    if volumes_header is None:
        print("→ Volume header not found → skipping")
    else:
        print(f"→ Found Volumes at row {volumes_header}")
        print(f"  Row content check: {df.iloc[volumes_header:volumes_header+3, 0:5].to_string()}")


        try:
                # Dynamically count only Volumes rows
                volumes_start = volumes_header + 1
                volumes_nrows = 0
                for i in range(volumes_start, len(df)):
                    val = df.iloc[i, 0]  # column A = Rpt No. or title
                    if pd.isna(val):
                        break
                    cleaned = str(clean_value(val)).strip().lower()
                    if cleaned == '' or not cleaned.replace('.', '').isdigit():
                        # Check if this is the next major section (e.g. MBT, No data, Solids, etc.)
                        row_text_a = str(df.iloc[i, 0]).lower()
                        row_text_b = str(df.iloc[i, 1]).lower() if len(df.columns) > 1 else ''
                        next_markers = ['mbt capacity', 'no data', 'solids', 'rheology', 'yield point']
                        if any(marker in row_text_a or marker in row_text_b for marker in next_markers):
                            print(f"  Stopping Volumes count at row {i} - detected next section ('{row_text_a}')")
                            break
                        if volumes_nrows > 0:
                            break  # stop after good rows
                        continue
                    # Count only if rpt_no looks valid
                    try:
                        rpt_num = int(cleaned.split('.')[0]) if '.' in cleaned else int(cleaned)
                        if rpt_num > 200:  # safety cap
                            print(f"  Stopping Volumes count - rpt_no {rpt_num} too high")
                            break
                    except ValueError:
                        continue
                    volumes_nrows += 1

                print(f"→ Detected {volumes_nrows} Volumes rows")

                volumes_df = pd.read_excel(
                    file_path,
                    sheet_name='Sheet1',
                    skiprows=volumes_header + 1,
                    nrows=volumes_nrows + 2,  # small buffer, same as all other sections
                    header=0,
                    usecols="A:AA"
                )

                # ── Clean columns (adapted for Volumes units) ─────────────────────
                volumes_df.columns = [
                    col.strip()
                    .replace('_x000D_', '')
                    .replace('\n', ' ')
                    .replace('\r', '')
                    .replace('  ', ' ')
                    .replace(' (', '_(')
                    .replace(' )', ')')
                    .replace('bbl', '_bbl')
                    .replace('(bbl)', '_bbl')
                    .replace('%', '_pct')
                    .replace('(%)', '_pct')
                    .replace('Start vol.', 'start_vol')
                    .replace('End vol.', 'end_vol')
                    .replace('Daily addition', 'daily_addition')
                    .replace('Daily loss', 'daily_loss')
                    .replace('From storage', 'from_storage')
                    .replace('To storage', 'to_storage')
                    .strip()
                    for col in volumes_df.columns
                ]

                # ── Rename map for Volumes (based on your sample) ─────────────────
                rename_map = {
                    'Rpt No.': 'rpt_no',
                    'Date': 'date',
                    'MD_(ft)': 'md_ft',
                    'start_vol_(_bbl)': 'start_vol_bbl',
                    'Other mud_(_bbl)': 'other_mud_bbl',
                    'Base fluid_(_bbl)': 'base_mud_bbl',
                    'Water_(_bbl)': 'water_bbl',
                    'Products_(_bbl)': 'products_bbl',
                    'Weight materials_(_bbl)': 'weight_materials_bbl',
                    'Formation_(_bbl)': 'formation_bbl',
                    'Cuttings_(_bbl)': 'cuttings_bbl',
                    'Others_(_bbl)': 'others_bbl',
                    'daily_addition_(_bbl)': 'daily_addition_bbl',
                    'Cuttings/retention_(_bbl)': 'cuttings_retention_bbl',
                    'Seepage_(_bbl)': 'seepage_bbl',
                    'Dump_(_bbl)': 'dump_bbl',
                    'Shakers_(_bbl)': 'shakers_bbl',
                    'Centrifuge_(_bbl)': 'centrifuge_bbl',
                    'Evaporation_(_bbl)': 'evaporation_bbl',
                    'Pit cleaning_(_bbl)': 'pit_cleaning_bbl',
                    'Formation loss_(_bbl)': 'formation_loss_bbl',
                    'Others_(_bbl).1': 'others_loss_bbl',
                    'daily_loss_(_bbl)': 'daily_loss_bbl',
                    'from_storage_(_bbl)': 'from_storage_bbl',
                    'to_storage_(_bbl)': 'to_storage_bbl',
                    'end_vol_(_bbl)': 'end_vol_bbl',
                    'Total on location_(_bbl)': 'on_location_bbl',
                }
                volumes_df = volumes_df.rename(columns=rename_map)

                # Manual fixes for common variations
                volumes_df = volumes_df.rename(columns={
                    'Start vol. (bbl)': 'start_vol_bbl',
                    'End vol. (bbl)': 'end_vol_bbl',
                    'Daily addition': 'daily_addition_bbl',
                    'Daily loss': 'daily_loss_bbl',
                    'Cuttings/retention': 'cuttings_retention_bbl',
                })

                print("\nMud Volumes preview (first 8 rows):")
                print(volumes_df.head(8).to_string(index=False))
                print("-" * 60)

                inserted_v = 0
                for _, row in volumes_df.iterrows():
                    rpt = clean_value(row.get('rpt_no'))
                    if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                        print(f"  Skipping Mud Volumes row - invalid rpt_no: {rpt}")
                        continue

                    volumes_data = {
                        "well_id": well_id,
                        "rpt_no": int(float(rpt)) if rpt else None,
                        "date": clean_value(row.get('date')),
                        "md_ft": safe_float(row.get('md_ft')),
                        "start_vol_bbl": safe_float(row.get('start_vol_bbl')),
                        "other_mud_bbl": safe_float(row.get('other_mud_bbl')),
                        "base_mud_bbl": safe_float(row.get('base_mud_bbl')),
                        "water_bbl": safe_float(row.get('water_bbl')),
                        "products_bbl": safe_float(row.get('products_bbl')),
                        "weight_materials_bbl": safe_float(row.get('weight_materials_bbl')),
                        "formation_bbl": safe_float(row.get('formation_bbl')),
                        "cuttings_bbl": safe_float(row.get('cuttings_bbl')),
                        "others_bbl": safe_float(row.get('others_bbl')),
                        "daily_addition_bbl": safe_float(row.get('daily_addition_bbl')),
                        "cuttings_retention_bbl": safe_float(row.get('cuttings_retention_bbl')),
                        "seepage_bbl": safe_float(row.get('seepage_bbl')),
                        "dump_bbl": safe_float(row.get('dump_bbl')),
                        "shakers_bbl": safe_float(row.get('shakers_bbl')),
                        "centrifuge_bbl": safe_float(row.get('centrifuge_bbl')),
                        "evaporation_bbl": safe_float(row.get('evaporation_bbl')),
                        "pit_cleaning_bbl": safe_float(row.get('pit_cleaning_bbl')),
                        "formation_loss_bbl": safe_float(row.get('formation_loss_bbl')),
                        "others_loss_bbl": safe_float(row.get('others_loss_bbl')),
                        "daily_loss_bbl": safe_float(row.get('daily_loss_bbl')),
                        "from_storage_bbl": safe_float(row.get('from_storage_bbl')),
                        "to_storage_bbl": safe_float(row.get('to_storage_bbl')),
                        "end_vol_bbl": safe_float(row.get('end_vol_bbl')),
                        "on_location_bbl": safe_float(row.get('on_location_bbl')),
                    }

                    try:
                        supabase.table('MudVolumes').insert(volumes_data).execute()
                        inserted_v += 1
                    except Exception as e:
                        print(f"  Mud Volumes insert failed for rpt_no {rpt}: {e}")
                        print("    Data:", volumes_data)

                print(f"→ Inserted {inserted_v} mud volumes rows")

        except Exception as e:
                print(f"Failed to read or insert Mud Volumes: {e}")   
# ── Mud Cost table ────────────────────────────────────────────────────
    cost_header = find_section_start(df, 'Cost|Cost table|Daily cost|Cum. cost', column=0)
    if cost_header is None:
        print("→ Cost header not found → skipping")
    else:
        print(f"→ Found Cost at row {cost_header}")
        print(f"  Row content check: {df.iloc[cost_header:cost_header+3, 0:5].to_string()}")

        try:
                # Dynamically count only Cost rows
                cost_start = cost_header + 1
                cost_nrows = 0
                for i in range(cost_start, len(df)):
                    val = df.iloc[i, 0]  # column A = Rpt No. or title
                    if pd.isna(val):
                        break
                    cleaned = str(clean_value(val)).strip().lower()
                    if cleaned == '' or not cleaned.replace('.', '').isdigit():
                        # Check if this is the next major section (e.g. MBT, No data, etc.)
                        row_text_a = str(df.iloc[i, 0]).lower()
                        row_text_b = str(df.iloc[i, 1]).lower() if len(df.columns) > 1 else ''
                        next_markers = ['mbt capacity', 'no data', 'yield point', 'plastic viscosity', 'rheology', 'solids']
                        if any(marker in row_text_a or marker in row_text_b for marker in next_markers):
                            print(f"  Stopping Cost count at row {i} - detected next section ('{row_text_a}')")
                            break
                        if cost_nrows > 0:
                            break  # stop after good rows
                        continue
                    # Count only if rpt_no looks valid
                    try:
                        rpt_num = int(cleaned.split('.')[0]) if '.' in cleaned else int(cleaned)
                        if rpt_num > 200:  # safety cap
                            print(f"  Stopping Cost count - rpt_no {rpt_num} too high")
                            break
                    except ValueError:
                        continue
                    cost_nrows += 1

                print(f"→ Detected {cost_nrows} Cost rows")

                cost_df = pd.read_excel(
                    file_path,
                    sheet_name='Sheet1',
                    skiprows=cost_header + 1,
                    nrows=cost_nrows + 2,  # small buffer, same as all other sections
                    header=0,
                    usecols="A:Q"
                )

                # ── Clean columns (adapted for Cost units) ────────────────────────
                cost_df.columns = [
                    col.strip()
                    .replace('_x000D_', '')
                    .replace('\n', ' ')
                    .replace('\r', '')
                    .replace('  ', ' ')
                    .replace(' (', '_(')
                    .replace(' )', ')')
                    .replace('($)', '_s')
                    .replace('(s)', '_s')
                    .replace('Cum.', 'cum')
                    .replace('Daily', 'daily')
                    .replace('Total', 'total')
                    .replace('MD(ft)', 'md_ft')
                    .strip()
                    for col in cost_df.columns
                ]

                # ── Rename map aligned with POST-CLEANING column names ───────────
                rename_map = {
                    'Rpt No.': 'rpt_no',
                    'Date': 'date',
                    'MD_(_ft)': 'md_ft',
                    'Product__s': 'product_s',
                    'Package__s': 'package_s',
                    'Service__s': 'service_s',
                    'Engineering__s': 'engineering_s',
                    'Tax__s': 'tax_s',
                    'daily total__s': 'daily_s',
                    'cum cost__s': 'cum_s',
                    'Total cost': 'total_cost',
                }
                cost_df = cost_df.rename(columns=rename_map)

                # Force-fix any lingering uncleaned names (common after cleaning)
                rename_fixes = {
                    'Product_(s)': 'product_s',
                    'Package_(s)': 'package_s',
                    'Service_(s)': 'service_s',
                    'Engineering_(s)': 'engineering_s',
                    'Tax_(s)': 'tax_s',
                    'Daily_(s)': 'daily_s',
                    'Cum._(s)': 'cum_s',
                    'MD_(ft)': 'md_ft',
                }
                cost_df = cost_df.rename(columns=rename_fixes)

                print("\nMud Cost columns after force-fix rename:")
                print(list(cost_df.columns))

                print("\nMud Cost preview (first 8 rows):")
                print(cost_df.head(8).to_string(index=False))
                print("-" * 60)

                inserted_c = 0
                for _, row in cost_df.iterrows():
                    rpt = clean_value(row.get('rpt_no'))
                    if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                        print(f"  Skipping Mud Cost row - invalid rpt_no: {rpt}")
                        continue

                    cost_data = {
                        "well_id": well_id,
                        "rpt_no": int(float(rpt)) if rpt else None,
                        "date": clean_value(row.get('date')),
                        "md_ft": safe_float(row.get('md_ft')),
                        "product_s": safe_float(row.get('product_s')),
                        "package_s": safe_float(row.get('package_s')),
                        "service_s": safe_float(row.get('service_s')),
                        "engineering_s": safe_float(row.get('engineering_s')),
                        "tax_s": safe_float(row.get('tax_s')),
                        "daily_s": safe_float(row.get('daily_s')),
                        "cum_s": safe_float(row.get('cum_s')),
                        "total_cost": safe_float(row.get('total_cost')),
                    }

                    try:
                        supabase.table('MudCost').insert(cost_data).execute()
                        inserted_c += 1
                    except Exception as e:
                        print(f"  Mud Cost insert failed for rpt_no {rpt}: {e}")
                        print("    Data:", cost_data)

                print(f"→ Inserted {inserted_c} mud cost rows")

        except Exception as e:
                print(f"Failed to read or insert Mud Cost: {e}")
# ── Remarks table ──────────────────────────────────────────
    remarks_header = find_section_start(df, 'Remarks|Remarks Section|Notes|Comments', column=0)
    if remarks_header is None:
        print("→ Remarks header not found → skipping")
    else:
        print(f"→ Found Remarks at row {remarks_header}")
        print(f"  Row content check: {df.iloc[remarks_header:remarks_header+3, 0:5].to_string()}")


        try:
                # Dynamically count Remarks rows (lenient for text section)
                remarks_start = remarks_header + 1
                remarks_nrows = 0
                consecutive_invalid = 0
                max_consecutive_invalid = 5  # allow more gaps in remarks

                for i in range(remarks_start, len(df)):
                    val = df.iloc[i, 0]
                    if pd.isna(val):
                        consecutive_invalid += 1
                        if consecutive_invalid >= max_consecutive_invalid:
                            print(f"  Stopping Remarks count at row {i} - too many consecutive blanks/invalids")
                            break
                        continue
                    cleaned = str(clean_value(val)).strip().lower()
                    if cleaned == '' or not cleaned.replace('.', '').isdigit():
                        row_text_a = str(df.iloc[i, 0]).lower()
                        row_text_b = str(df.iloc[i, 1]).lower() if len(df.columns) > 1 else ''
                        next_markers = ['mbt capacity', 'no data', 'yield point', 'plastic viscosity', 'rheology', 'solids', 'cost', 'volumes']
                        if any(marker in row_text_a or marker in row_text_b for marker in next_markers):
                            print(f"  Stopping Remarks count at row {i} - detected next section ('{row_text_a}')")
                            break
                        consecutive_invalid += 1
                        if consecutive_invalid >= max_consecutive_invalid:
                            print(f"  Stopping Remarks count at row {i} - too many consecutive invalids")
                            break
                        continue
                    consecutive_invalid = 0
                    try:
                        rpt_num = int(cleaned.split('.')[0]) if '.' in cleaned else int(cleaned)
                        if rpt_num > 200:
                            print(f"  Stopping Remarks count - rpt_no {rpt_num} too high")
                            break
                    except ValueError:
                        continue
                    remarks_nrows += 1

                print(f"→ Detected {remarks_nrows} Remarks rows")

                remarks_df = pd.read_excel(
                    file_path,
                    sheet_name='Sheet1',
                    skiprows=remarks_header + 1,
                    nrows=remarks_nrows + 2,
                    header=0,
                    usecols="A:D"  # only columns A-D (Rpt No., Date, MD, Remarks)
                )

                print(f"Rows read for Remarks: {len(remarks_df)}")

                # ── Light cleaning for text-heavy remarks ─────────────────────────
                remarks_df.columns = [
                    col.strip()
                    .replace('_x000D_', '')
                    .replace('\n', ' ')
                    .replace('\r', '')
                    .replace('  ', ' ')
                    .replace('MD(ft)', 'md_ft')
                    .replace('MD (ft)', 'md_ft')
                    .strip()
                    for col in remarks_df.columns
                ]

                # ── Simple rename (only A-D columns) ──────────────────────────────
                rename_map = {
                    'Rpt No.': 'rpt_no',
                    'Rpt No': 'rpt_no',
                    'Date': 'date',
                    'MD (ft)': 'md_ft',
                    'MD_(ft)': 'md_ft',
                    'Remarks': 'remarks',
                    # No engineer/tickets — ignore them if present
                }
                remarks_df = remarks_df.rename(columns=rename_map)

                print("\nMud Remarks columns after rename:")
                print(list(remarks_df.columns))

                print("\nMud Remarks preview (first 8 rows):")
                print(remarks_df.head(8).to_string(index=False))
                print("-" * 60)

                inserted_rem = 0
                for _, row in remarks_df.iterrows():
                    rpt = clean_value(row.get('rpt_no'))
                    if rpt is None or not str(rpt).strip().replace('.', '').isdigit():
                        print(f"  Skipping Mud Remarks row - invalid rpt_no: {rpt}")
                        continue

                    # Duplicate check: skip if well_id + rpt_no already exists
                    existing = supabase.table('MudRemarks') \
                        .select('id') \
                        .eq('well_id', well_id) \
                        .eq('rpt_no', int(float(rpt))) \
                        .execute()
                    if existing.data:
                        print(f"  Skipping duplicate Remarks for rpt_no {rpt}")
                        continue

                    remarks_data = {
                        "well_id": well_id,
                        "rpt_no": int(float(rpt)),
                        "date": clean_value(row.get('date')),
                        "md_ft": safe_float(row.get('md_ft')),
                        "remarks": clean_text(row.get('remarks')),
                    }

                    try:
                        supabase.table('MudRemarks').insert(remarks_data).execute()
                        inserted_rem += 1
                    except Exception as e:
                        print(f"  Mud Remarks insert failed for rpt_no {rpt}: {e}")
                        print("    Data:", remarks_data)

                print(f"→ Inserted {inserted_rem} mud remarks rows (skipped duplicates)")

        except Exception as e:
                print(f"Failed to read or insert Mud Remarks: {e}")


    print(f"  → File processed")
    return True

def process_folder(folder_path):
    logger.info(f"=== Starting batch processing of folder: {folder_path} ===")
    print(f"\n=== Batch Processing Folder: {folder_path} ===")
    
    excel_files = [os.path.join(root, f) for root, dirs, files in os.walk(folder_path)
                   for f in files if f.lower().endswith('.xlsx')]
    
    total_files = len(excel_files)
    logger.info(f"Found {total_files} .xlsx files")
    print(f"Found {total_files} .xlsx files")
    
    if total_files == 0:
        logger.info("No files found — exiting")
        print("No files found.")
        return
    
    processed = 0
    skipped = 0
    failed = 0
    
    with tqdm(total=total_files, desc="Importing recaps", unit="file", leave=True) as pbar:
        for file_path in excel_files:
            filename = os.path.basename(file_path).strip()
            logger.info(f"Processing file: {filename}")
            
            # Duplicate check (using Neon)
            try:
                conn = get_neon_connection()
                cur = conn.cursor()
                cur.execute('SELECT id FROM "Wells" WHERE filename = %s LIMIT 1', (filename,))
                if cur.fetchone():
                    logger.info(f"Already exists — skipping {filename}")
                    skipped += 1
                    pbar.update(1)
                    cur.close()
                    conn.close()
                    continue
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Duplicate check failed for {filename}: {str(e)}")
                print(f"Duplicate check error: {e}")
            
            # Upload
            try:
                upload_file(file_path)
                processed += 1
                logger.info(f"Success: {filename}")
            except Exception as e:
                logger.error(f"FAILED {filename}: {str(e)}")
                print(f"FAILED {filename}: {e}")
                failed += 1
            
            pbar.update(1)
    
    # Final summary
    summary = f"""
=== Batch Complete ===
Processed successfully: {processed}
Skipped (already in DB): {skipped}
Failed: {failed}
"""
    print(summary)
    logger.info(summary.strip())

if __name__ == "__main__":
    folder = os.path.join("uploads", "recaps")
    process_folder(folder)