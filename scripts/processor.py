"""
NYC Taxi Data Processor for Congestion Pricing Audit
Processes 2025 Yellow/Green taxi data using PySpark, applies ghost trip filters.
Unifies schemas to 8 required columns and handles missing December imputation.
- PySpark 3.5.0+
- Java 11.0+

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
PROCESSED_DATA_PATH = project_root / "data" / "processed"
AUDIT_LOG_PATH = project_root / "outputs" / "audit_logs"

# Required columns as per assignment - EXACTLY THESE 8 COLUMNS
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
    """Create necessary folder structure"""
    folders = [PROCESSED_DATA_PATH, AUDIT_LOG_PATH]
    
    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)
        print_status(f"Created: {folder.relative_to(project_root)}", "SUCCESS")
    
    # Create taxi-specific folders
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
            .appName("NYC_Congestion_Analysis") \
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

def get_available_months():
    """Get list of months with data"""
    months_found = set()
    
    for taxi_type in ['yellow', 'green']:
        folder = RAW_DATA_PATH / taxi_type
        if folder.exists():
            for file in folder.glob("2025_*.parquet"):
                month = file.stem.split('_')[1]
                months_found.add(month)
    
    return sorted(list(months_found))

def read_and_unify_data(spark, file_path, taxi_type):
    """Read and unify schema to EXACTLY 8 required columns - SIMPLIFIED"""
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
        
        # SIMPLE AND DIRECT COLUMN SELECTION
        # No conditional logic needed - all 2025 files have congestion_surcharge
        unified_df = df.select(
            col(pickup_col).alias("pickup_time"),
            col(dropoff_col).alias("dropoff_time"),
            col("PULocationID").alias("pickup_loc"),
            col("DOLocationID").alias("dropoff_loc"),
            col("trip_distance"),
            col("fare_amount").alias("fare"),
            col("total_amount"),
            col("congestion_surcharge")  # Direct column - verified to exist
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
    """Apply Phase 1 ghost trip filters"""
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

def process_single_month(spark, taxi_type, month):
    """Process a single month's data"""
    file_path = RAW_DATA_PATH / taxi_type / f"2025_{month}.parquet"
    
    if not file_path.exists():
        print_status(f"File not found: {file_path.name}", "WARNING")
        return None, None
    
    print_status(f"Processing {taxi_type} {month}/2025...")
    
    # Step 1: Read and unify schema
    unified_df = read_and_unify_data(spark, file_path, taxi_type)
    if unified_df is None:
        return None, None
    
    # Show schema verification
    print_status(f"  Schema verified: {len(unified_df.columns)} columns")
    
    # Step 2: Apply ghost trip filters
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

def save_dataframes(clean_df, audit_df, taxi_type, month):
    """Save processed data to appropriate folders"""
    # Save clean data
    clean_path = PROCESSED_DATA_PATH / taxi_type / f"clean_2025_{month}.parquet"
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    
    print_status(f"  Saving clean data to: {clean_path.relative_to(project_root)}")
    clean_df.write.mode("overwrite").parquet(str(clean_path))
    
    # Save audit log if there are suspicious trips
    if audit_df.count() > 0:
        audit_path = AUDIT_LOG_PATH / taxi_type / f"audit_2025_{month}.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        print_status(f"  Saving audit log to: {audit_path.relative_to(project_root)}")
        audit_df.write.mode("overwrite").parquet(str(audit_path))
        return clean_path, audit_path
    
    return clean_path, None

def create_december_imputation(spark):
    """Create December 2025 using weighted average of 2023 and 2024"""
    print_header("CREATING DECEMBER 2025 IMPUTATION")
    print_status("Using: 30% Dec 2023 + 70% Dec 2024", "INFO")
    
    for taxi_type in ['yellow', 'green']:
        # Check if historical files exist
        dec_2023 = RAW_DATA_PATH / "historical" / f"{taxi_type}_tripdata_2023-12.parquet"
        dec_2024 = RAW_DATA_PATH / "historical" / f"{taxi_type}_tripdata_2024-12.parquet"
        
        if not dec_2023.exists():
            print_status(f"Missing Dec 2023 data for {taxi_type}", "WARNING")
            continue
        if not dec_2024.exists():
            print_status(f"Missing Dec 2024 data for {taxi_type}", "WARNING")
            continue
        
        try:
            print_status(f"Imputing December 2025 for {taxi_type} taxi...")
            
            # Read historical data
            df_2023 = read_and_unify_data(spark, dec_2023, taxi_type)
            df_2024 = read_and_unify_data(spark, dec_2024, taxi_type)
            
            if df_2023 is None or df_2024 is None:
                print_status(f"Failed to read historical data for {taxi_type}", "ERROR")
                continue
            
            # Sample for weighted average (30% from 2023, 70% from 2024)
            print_status(f"  Sampling 30% from Dec 2023...")
            sample_2023 = df_2023.sample(fraction=0.3, seed=42)
            
            print_status(f"  Sampling 70% from Dec 2024...")
            sample_2024 = df_2024.sample(fraction=0.7, seed=42)
            
            # Combine samples
            imputed_df = sample_2023.union(sample_2024)
            
            # Apply ghost filters
            clean_imputed, audit_imputed = apply_ghost_filters(imputed_df)
            
            # Save imputed December
            clean_path, audit_path = save_dataframes(clean_imputed, audit_imputed, taxi_type, "12")
            
            print_status(f"Imputed {taxi_type}: {clean_imputed.count():,} clean trips", "SUCCESS")
            
        except Exception as e:
            print_status(f"Error imputing {taxi_type}: {e}", "ERROR")

def verify_phase1_completion():
    """Verify Phase 1 was completed successfully"""
    print_header("PHASE 1 VERIFICATION")
    
    # Check if processed data exists
    yellow_processed = list((PROCESSED_DATA_PATH / "yellow").glob("*.parquet"))
    green_processed = list((PROCESSED_DATA_PATH / "green").glob("*.parquet"))
    
    yellow_audit = list((AUDIT_LOG_PATH / "yellow").glob("*.parquet"))
    green_audit = list((AUDIT_LOG_PATH / "green").glob("*.parquet"))
    
    print(f"Yellow Taxi - Clean data: {len(yellow_processed)} months")
    print(f"Yellow Taxi - Audit logs: {len(yellow_audit)} months")
    print(f"Green Taxi - Clean data: {len(green_processed)} months")
    print(f"Green Taxi - Audit logs: {len(green_audit)} months")
    
    # Verify schema of one processed file
    if yellow_processed:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("Verification").getOrCreate()
            
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
    
    print(f"\n📁 Clean data location: {PROCESSED_DATA_PATH}")
    print(f"📁 Audit logs location: {AUDIT_LOG_PATH}")

def display_phase1_summary():
    """Display comprehensive Phase 1 summary"""
    print_header("PHASE 1 COMPLETION SUMMARY")
    
    total_clean_files = 0
    total_audit_files = 0
    
    for taxi_type in ['yellow', 'green']:
        clean_folder = PROCESSED_DATA_PATH / taxi_type
        audit_folder = AUDIT_LOG_PATH / taxi_type
        
        if clean_folder.exists():
            clean_files = list(clean_folder.glob("*.parquet"))
            total_clean_files += len(clean_files)
            
            print(f"\n{taxi_type.upper()} TAXI:")
            print(f"  Clean data: {len(clean_files)} months")
            
            # Show each month's status
            months_processed = sorted([f.stem.split('_')[-1] for f in clean_files])
            if months_processed:
                print(f"  Months: {', '.join(months_processed)}")
        
        if audit_folder.exists():
            audit_files = list(audit_folder.glob("*.parquet"))
            total_audit_files += len(audit_files)
            print(f"  Audit logs: {len(audit_files)} months")
    
    print(f"\n TOTAL:")
    print(f"  Clean datasets: {total_clean_files}")
    print(f"  Audit logs: {total_audit_files}")
    print(f"\n PHASE 1 - BIG DATA ENGINEERING LAYER COMPLETE")

def main():
    """Main processing function for Phase 1"""
    print_header("NYC CONGESTION PRICING AUDIT - PHASE 1")
    print("BIG DATA ENGINEERING LAYER")
    print("Processing 2025 NYC Taxi Data with PySpark 3.5.1")
    
    start_time = time.time()
    
    try:
        # Step 1: Create all necessary folders
        print_status("Creating folder structure...")
        create_folders()
        
        # Step 2: Initialize PySpark session
        spark = init_spark_session()
        
        # Step 3: Check available data
        print_status("Scanning for available data...")
        months = get_available_months()
        
        if not months:
            print_status("No 2025 data found. Run scraper.py first.", "ERROR")
            spark.stop()
            sys.exit(1)
        
        print_status(f"Found 2025 data for months: {', '.join(months)}", "SUCCESS")
        
        # Step 4: Check if December is missing
        if '12' not in months:
            print_status("December 2025 is missing - will impute using historical data", "WARNING")
            december_missing = True
        else:
            december_missing = False
        
        # Step 5: Process real data (January to November)
        print_header("PROCESSING REAL 2025 DATA")
        
        for taxi_type in ['yellow', 'green']:
            print_header(f"PROCESSING {taxi_type.upper()} TAXI")
            
            for month in months:
                if month != '12':  # Process all months except December
                    clean_df, audit_df = process_single_month(spark, taxi_type, month)
                    if clean_df is not None:
                        save_dataframes(clean_df, audit_df, taxi_type, month)
                    print("")  # Empty line between months
        
        # Step 6: Handle December (impute or process real)
        print_header("HANDLING DECEMBER 2025")
        
        if december_missing:
            # Create imputed December
            create_december_imputation(spark)
        else:
            # Process real December
            for taxi_type in ['yellow', 'green']:
                print_status(f"Processing real December 2025 for {taxi_type} taxi...")
                clean_df, audit_df = process_single_month(spark, taxi_type, '12')
                if clean_df is not None:
                    save_dataframes(clean_df, audit_df, taxi_type, '12')
        
        # Step 7: Display summary
        display_phase1_summary()
        
        # Step 8: Verification
        verify_phase1_completion()
        
        # Step 9: Cleanup
        spark.stop()
        print_status("PySpark session stopped", "SUCCESS")
        
        # Step 10: Timing
        elapsed = time.time() - start_time
        print(f"\n⏱  Total Phase 1 processing time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        
        print("\n" + "="*70)
        print(" PHASE 1 COMPLETED SUCCESSFULLY")
        print("="*70)
        print("\nNext steps:")
        print("1. Data is ready for Phase 2 (Congestion Zone Impact Analysis)")
        print("2. Run Phase 2 script to analyze traffic patterns")
        print("3. Check outputs/audit_logs for suspicious trip reports")
        print("                                                         ")
        print("                                                         ")
        print("                                                         ")
        print("                                                         ")
        
    except Exception as e:
        print_status(f"Phase 1 processing failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()