"""
NYC TLC Data Processor for Congestion Pricing Analysis - 2024 Q1 Data
Phase 1: Big Data Engineering Layer for 2024 Comparison Data
Compatible with PySpark 3.5 + Java 11
"""

import os

# Set Java 11 for PySpark (COMPATIBLE)
java_home = r"C:\Program Files\Java\jdk-11"
os.environ['JAVA_HOME'] = java_home
os.environ['PATH'] = f"{java_home}\\bin;{os.environ['PATH']}"

# Windows-specific Spark fixes
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_PUBLIC_DNS'] = 'localhost'
os.environ['PYSPARK_PYTHON'] = r"C:\Users\black\AppData\Local\Programs\Python\Python311\python.exe"

print("="*80)
print(f"Java configured: {os.environ['JAVA_HOME']}")
print("="*80)

import sys
from pathlib import Path
import time
import logging

logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
logging.getLogger("pyspark.sql.utils").setLevel(logging.ERROR)

# Set project root (where your scripts folder is)
project_root = Path.cwd()  # Current working directory
print(f"Project root: {project_root}")

RAW_DATA_PATH = project_root / "data" / "raw"
PROCESSED_DATA_PATH = project_root / "data" / "processed_2024"  # Different folder for 2024
AUDIT_LOG_PATH = project_root / "outputs" / "audit_logs_2024"   # Different folder for 2024

# Required columns as per assignment - EXACTLY THESE 8 COLUMNS (SAME AS 2025)
REQUIRED_COLUMNS = [
    'pickup_time',          # tpep_pickup_datetime (yellow) / lpep_pickup_datetime (green)
    'dropoff_time',         # tpep_dropoff_datetime (yellow) / lpep_dropoff_datetime (green)
    'pickup_loc',           # PULocationID
    'dropoff_loc',          # DOLocationID
    'trip_distance',        # trip_distance
    'fare',                 # fare_amount
    'total_amount',         # total_amount
    'congestion_surcharge'  # congestion_surcharge (direct column, no fallback)
]

# 2024 Q1 months to process
Q1_MONTHS = ['01', '02', '03']  # January, February, March 2024

def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*70)
    print(f" {text}")
    print("="*70)

def print_status(message, status="INFO"):
    """Print status messages with consistent alignment"""
    status_map = {
        "SUCCESS": "[✓]",
        "WARNING": "[⚠]",
        "ERROR": "[✗]",
        "INFO": "[→]"
    }
    prefix = status_map.get(status, "[→]")
    print(f"{prefix} {message}")

def create_folders():
    """Create necessary folder structure for 2024 data"""
    folders = [PROCESSED_DATA_PATH, AUDIT_LOG_PATH]
    
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)
        print_status(f"Created: {folder.relative_to(project_root)}", "SUCCESS")
    
    # Create taxi-specific folders for 2024
    (PROCESSED_DATA_PATH / "yellow").mkdir(exist_ok=True)
    (PROCESSED_DATA_PATH / "green").mkdir(exist_ok=True)
    (AUDIT_LOG_PATH / "yellow").mkdir(exist_ok=True)
    (AUDIT_LOG_PATH / "green").mkdir(exist_ok=True)
    
    return folders

def init_spark_session():
    """Initialize PySpark session"""
    print_status("Initializing PySpark session...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("NYC_Congestion_Analysis_2024") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        print_status("PySpark session created", "SUCCESS")
        return spark
        
    except Exception as e:
        print_status(f"Failed to initialize PySpark: {e}", "ERROR")
        sys.exit(1)

def check_2024_data_availability():
    """Check if 2024 Q1 data exists in raw folders"""
    print_status("Checking for 2024 Q1 data...")
    
    months_found = set()
    missing_months = []
    
    for taxi_type in ['yellow', 'green']:
        folder = RAW_DATA_PATH / f"{taxi_type}_2024"  # Your 2024 folder structure
        
        if not folder.exists():
            print_status(f"Folder not found: {folder.relative_to(project_root)}", "ERROR")
            continue
        
        for month in Q1_MONTHS:
            file_path = folder / f"2024_{month}.parquet"
            if file_path.exists():
                months_found.add(month)
                print_status(f"Found: {taxi_type}_2024/2024_{month}.parquet", "SUCCESS")
            else:
                missing_months.append(f"{taxi_type}_2024/2024_{month}.parquet")
                print_status(f"Missing: {taxi_type}_2024/2024_{month}.parquet", "WARNING")
    
    if len(months_found) == 0:
        print_status("No 2024 Q1 data found! Please download it first.", "ERROR")
        return []
    
    return sorted(list(months_found))

def read_and_unify_data_2024(spark, file_path, taxi_type):
    """Read and unify schema to EXACTLY 8 required columns - for 2024 data"""
    from pyspark.sql.functions import col
    
    try:
        # Read the parquet file
        df = spark.read.parquet(str(file_path))
        
        # Determine datetime column names based on taxi type
        if taxi_type == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        else:  # green taxi
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        
        # Check if congestion_surcharge column exists in 2024 data
        # Note: 2024 data might have congestion_surcharge or mta_tax as alternative
        if 'congestion_surcharge' in df.columns:
            surcharge_col = col("congestion_surcharge")
        elif 'mta_tax' in df.columns:
            # Use mta_tax as proxy for congestion surcharge in older data
            surcharge_col = col("mta_tax")
        else:
            # If neither exists, create zero column
            surcharge_col = col("fare_amount") * 0  # Creates 0 values
            
            print_status(f"Warning: No congestion surcharge column found in {file_path.name}, using zeros", "WARNING")
        
        # Unified schema selection (same as 2025)
        unified_df = df.select(
            col(pickup_col).alias("pickup_time"),
            col(dropoff_col).alias("dropoff_time"),
            col("PULocationID").alias("pickup_loc"),
            col("DOLocationID").alias("dropoff_loc"),
            col("trip_distance"),
            col("fare_amount").alias("fare"),
            col("total_amount"),
            surcharge_col.alias("congestion_surcharge")
        )
        
        # Verify we have exactly 8 columns
        actual_columns = [field.name for field in unified_df.schema.fields]
        if len(actual_columns) != 8:
            print_status(f"Warning: Expected 8 columns, got {len(actual_columns)}", "WARNING")
        
        return unified_df
        
    except Exception as e:
        print_status(f"Error reading {file_path.name}: {e}", "ERROR")
        return None

def apply_ghost_filters(df):
    """Apply Phase 1 ghost trip filters - SAME AS 2025 PROCESSOR"""
    from pyspark.sql.functions import col, when, unix_timestamp
    
    # Calculate trip duration in seconds
    df_with_duration = df.withColumn(
        "trip_seconds",
        unix_timestamp(col("dropoff_time")) - unix_timestamp(col("pickup_time"))
    )
    
    # Calculate trip duration in minutes
    df_with_minutes = df_with_duration.withColumn(
        "trip_minutes",
        col("trip_seconds") / 60.0
    )
    
    # Calculate speed in MPH (miles per hour)
    # Speed = distance / (minutes / 60)
    df_with_speed = df_with_minutes.withColumn(
        "speed_mph",
        when(col("trip_minutes") > 0, 
             col("trip_distance") / (col("trip_minutes") / 60.0))
        .otherwise(0.0)
    )
    
    # Define the three ghost trip conditions
    impossible_physics = col("speed_mph") > 65
    teleporter = (col("trip_minutes") < 1) & (col("fare") > 20)
    stationary_ride = (col("trip_distance") == 0) & (col("fare") > 0)
    
    # Combine conditions
    is_suspicious = impossible_physics | teleporter | stationary_ride
    
    # Split into clean and suspicious data
    clean_df = df_with_speed.filter(~is_suspicious).drop("trip_seconds", "trip_minutes", "speed_mph")
    audit_df = df_with_speed.filter(is_suspicious)
    
    # Add audit reason column
    from pyspark.sql.functions import when as spark_when
    audit_df = audit_df.withColumn(
        "audit_reason",
        spark_when(impossible_physics, "Impossible Physics (>65 MPH)")
        .when(teleporter, "Teleporter (<1 min & >$20)")
        .when(stationary_ride, "Stationary Ride (0 distance & fare > 0)")
        .otherwise("Unknown")
    )
    
    return clean_df, audit_df

def process_single_month_2024(spark, taxi_type, month):
    """Process a single month's 2024 data"""
    folder_path = RAW_DATA_PATH / f"{taxi_type}_2024"
    file_path = folder_path / f"2024_{month}.parquet"
    
    if not file_path.exists():
        print_status(f"File not found: {file_path.name}", "WARNING")
        return None, None
    
    print_status(f"Processing {taxi_type} {month}/2024...")
    
    # Step 1: Read and unify schema (with 2024-specific handling)
    unified_df = read_and_unify_data_2024(spark, file_path, taxi_type)
    if unified_df is None:
        return None, None
    
    # Show schema verification
    print_status(f"  Schema verified: {len(unified_df.columns)} columns")
    
    # Step 2: Apply ghost trip filters (same as 2025)
    clean_df, audit_df = apply_ghost_filters(unified_df)
    
    # Count results
    clean_count = clean_df.count()
    audit_count = audit_df.count()
    
    print_status(f"  Clean trips: {clean_count:,}", "SUCCESS")
    print_status(f"  Suspicious trips: {audit_count:,}", "INFO")
    
    # Show breakdown of audit reasons if any
    if audit_count > 0:
        try:
            from pyspark.sql.functions import count
            audit_breakdown = audit_df.groupBy("audit_reason").agg(count("*").alias("count")).collect()
            for row in audit_breakdown:
                print_status(f"  {row['audit_reason']}: {row['count']:,}")
        except:
            pass
    
    return clean_df, audit_df

def save_dataframes_2024(clean_df, audit_df, taxi_type, month):
    """Save processed 2024 data to appropriate folders"""
    # Save clean data
    clean_path = PROCESSED_DATA_PATH / taxi_type / f"clean_2024_{month}.parquet"
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    
    print_status(f"  Saving clean data to: {clean_path.relative_to(project_root)}")
    clean_df.write.mode("overwrite").parquet(str(clean_path))
    
    # Save audit log if there are suspicious trips
    if audit_df.count() > 0:
        audit_path = AUDIT_LOG_PATH / taxi_type / f"audit_2024_{month}.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        print_status(f"  Saving audit log to: {audit_path.relative_to(project_root)}")
        audit_df.write.mode("overwrite").parquet(str(audit_path))
        return clean_path, audit_path
    
    return clean_path, None

def verify_2024_completion():
    """Verify 2024 Q1 processing was completed successfully"""
    print_header("2024 Q1 PROCESSING VERIFICATION")
    
    # Check if processed data exists
    yellow_processed = list((PROCESSED_DATA_PATH / "yellow").glob("*.parquet"))
    green_processed = list((PROCESSED_DATA_PATH / "green").glob("*.parquet"))
    
    yellow_audit = list((AUDIT_LOG_PATH / "yellow").glob("*.parquet"))
    green_audit = list((AUDIT_LOG_PATH / "green").glob("*.parquet"))
    
    print(f"Yellow Taxi 2024 - Clean data: {len(yellow_processed)} months")
    print(f"Yellow Taxi 2024 - Audit logs: {len(yellow_audit)} months")
    print(f"Green Taxi 2024 - Clean data: {len(green_processed)} months")
    print(f"Green Taxi 2024 - Audit logs: {len(green_audit)} months")
    
    # Verify schema of one processed file
    if yellow_processed:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("Verification_2024").getOrCreate()
            
            sample_file = yellow_processed[0]
            df = spark.read.parquet(str(sample_file))
            
            print(f"\nSchema verification for {sample_file.name}:")
            print(f"  Columns: {len(df.columns)}")
            print(f"  Column names: {', '.join(df.columns)}")
            
            # Check if we have exactly the 8 required columns
            required_set = set(REQUIRED_COLUMNS)
            actual_set = set(df.columns)
            
            if required_set == actual_set:
                print_status("✓ Schema matches exactly 8 required columns", "SUCCESS")
            else:
                print_status("✗ Schema mismatch", "ERROR")
                print(f"  Missing: {required_set - actual_set}")
                print(f"  Extra: {actual_set - required_set}")
            
            spark.stop()
        except:
            pass
    
    print(f"\n Clean 2024 data location: {PROCESSED_DATA_PATH}")
    print(f" Audit 2024 logs location: {AUDIT_LOG_PATH}")

def display_2024_summary():
    """Display comprehensive 2024 Q1 processing summary"""
    print_header("2024 Q1 PROCESSING SUMMARY")
    
    total_clean_files = 0
    total_audit_files = 0
    
    for taxi_type in ['yellow', 'green']:
        clean_folder = PROCESSED_DATA_PATH / taxi_type
        audit_folder = AUDIT_LOG_PATH / taxi_type
        
        if clean_folder.exists():
            clean_files = list(clean_folder.glob("*.parquet"))
            total_clean_files += len(clean_files)
            
            print(f"\n{taxi_type.upper()} TAXI 2024:")
            print(f"  Clean data: {len(clean_files)} months")
            
            # Show each month's status
            months_processed = sorted([f.stem.split('_')[-1] for f in clean_files])
            if months_processed:
                print(f"  Months: {', '.join(months_processed)}")
        
        if audit_folder.exists():
            audit_files = list(audit_folder.glob("*.parquet"))
            total_audit_files += len(audit_files)
            print(f"  Audit logs: {len(audit_files)} months")
    
    print(f"\n 2024 Q1 TOTAL:")
    print(f"  Clean datasets: {total_clean_files}")
    print(f"  Audit logs: {total_audit_files}")
    
    # Compare with requirements
    expected_months = len(Q1_MONTHS) * 2  # 3 months × 2 taxi types = 6 files
    if total_clean_files == expected_months:
        print_status(f"✓ All {expected_months} Q1 2024 files processed", "SUCCESS")
    else:
        print_status(f"⚠ Expected {expected_months} files, got {total_clean_files}", "WARNING")

def main():
    """Main processing function for 2024 Q1 data"""
    print_header("NYC CONGESTION PRICING AUDIT - 2024 Q1 DATA")
    print("BIG DATA ENGINEERING LAYER FOR COMPARISON DATA")
    print("Processing 2024 Q1 NYC Taxi Data with PySpark 3.5.1")
    
    start_time = time.time()
    
    try:
        # Step 1: Create all necessary folders
        print_status("Creating 2024 folder structure...")
        create_folders()
        
        # Step 2: Initialize PySpark session
        spark = init_spark_session()
        
        # Step 3: Check available 2024 Q1 data
        print_status("Scanning for 2024 Q1 data...")
        available_months = check_2024_data_availability()
        
        if not available_months:
            print_status("No 2024 Q1 data found. Please download it first.", "ERROR")
            spark.stop()
            sys.exit(1)
        
        print_status(f"Found 2024 Q1 data for months: {', '.join(available_months)}", "SUCCESS")
        
        # Verify we have all Q1 months
        missing_q1 = [m for m in Q1_MONTHS if m not in available_months]
        if missing_q1:
            print_status(f"Missing Q1 months: {', '.join(missing_q1)}", "WARNING")
            print_status("Phase 2 comparison will be incomplete", "WARNING")
        
        # Step 4: Process 2024 Q1 data
        print_header("PROCESSING 2024 Q1 DATA")
        
        for taxi_type in ['yellow', 'green']:
            print_header(f"PROCESSING {taxi_type.upper()} TAXI 2024")
            
            for month in Q1_MONTHS:
                clean_df, audit_df = process_single_month_2024(spark, taxi_type, month)
                if clean_df is not None:
                    save_dataframes_2024(clean_df, audit_df, taxi_type, month)
                print("")  # Empty line between months
        
        # Step 5: Display summary
        display_2024_summary()
        
        # Step 6: Verification
        verify_2024_completion()
        
        # Step 7: Cleanup
        spark.stop()
        print_status("PySpark session stopped", "SUCCESS")
        
        # Step 8: Timing
        elapsed = time.time() - start_time
        print(f"\n  Total 2024 Q1 processing time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        
        print("\n" + "="*70)
        print(" 2024 Q1 DATA PROCESSING COMPLETED SUCCESSFULLY")
        print("="*70)
        
        print("                                                     ")
        print("                                                     ")
        print("                                                     ")
        print("                                                     ")
        
    except Exception as e:
        print_status(f"2024 Q1 processing failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()