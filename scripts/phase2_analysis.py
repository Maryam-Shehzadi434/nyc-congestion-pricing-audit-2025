"""
NYC Congestion Pricing Audit - Phase 2 Analysis
Congestion Zone Impact Analysis for 2024 vs 2025 Q1 Comparison
Author: [Your Name/Black]
Date: February 2026
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
from datetime import datetime

# Set project root
project_root = Path.cwd()
print(f"Project root: {project_root}")

# File paths
PROCESSED_2025_PATH = project_root / "data" / "processed"
PROCESSED_2024_PATH = project_root / "data" / "processed_2024"
OUTPUT_PATH = project_root / "outputs" / "phase2_results"

# Congestion Zone Location IDs (Manhattan South of 60th Street)
# Based on NYC TLC Taxi Zone map: Zones 1-263 are Manhattan
# South of 60th St roughly corresponds to zones 1-200 (approximation)
CONGESTION_ZONE_LOCATIONS = list(range(1, 201))  # Approximate - zones 1-200

# Important dates
CONGESTION_START_DATE = "2025-01-05"  # Congestion pricing implementation date

# Q1 months
Q1_MONTHS = ['01', '02', '03']

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

def create_output_folder():
    """Create output folder for Phase 2 results"""
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    print_status(f"Created output folder: {OUTPUT_PATH.relative_to(project_root)}", "SUCCESS")
    return OUTPUT_PATH

def save_report_to_file(report_content, filename="phase2_report.txt"):
    """Save analysis report to file"""
    report_path = OUTPUT_PATH / filename
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    print_status(f"Report saved to: {report_path.relative_to(project_root)}", "SUCCESS")
    return report_path

def init_spark_session():
    """Initialize PySpark session for analysis"""
    print_status("Initializing PySpark session for Phase 2 analysis...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("NYC_Congestion_Phase2_Analysis") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .getOrCreate()
        
        print_status("PySpark session created", "SUCCESS")
        return spark
        
    except Exception as e:
        print_status(f"Failed to initialize PySpark: {e}", "ERROR")
        sys.exit(1)

def load_processed_data_safely(spark, file_path):
    """Safely load a single parquet file with multiple fallback methods"""
    try:
        # Method 1: Try with mergeSchema option
        df = spark.read.option("mergeSchema", "true").parquet(str(file_path))
        return df, True
    except Exception as e1:
        print_status(f"Method 1 failed for {file_path.name}: {str(e1)[:100]}", "WARNING")
        try:
            # Method 2: Try without any options
            df = spark.read.parquet(str(file_path))
            return df, True
        except Exception as e2:
            print_status(f"Method 2 failed for {file_path.name}: {str(e2)[:100]}", "WARNING")
            try:
                # Method 3: Try with format().load()
                df = spark.read.format("parquet").load(str(file_path))
                return df, True
            except Exception as e3:
                print_status(f"All methods failed for {file_path.name}", "ERROR")
                return None, False

def load_q1_data(spark, year, taxi_type):
    """
    Load Q1 data for specific year and taxi type
    Handles schema issues with individual file loading
    """
    if year == 2024:
        base_path = PROCESSED_2024_PATH / taxi_type
    else:
        base_path = PROCESSED_2025_PATH / taxi_type
    
    if not base_path.exists():
        print_status(f"No processed data found for {taxi_type} {year}", "ERROR")
        return None
    
    dataframes = []
    
    for month in Q1_MONTHS:
        file_pattern = f"clean_{year}_{month}.parquet"
        file_path = base_path / file_pattern
        
        if not file_path.exists():
            print_status(f"File not found: {file_path.name}", "WARNING")
            continue
        
        df, success = load_processed_data_safely(spark, file_path)
        if success and df is not None:
            dataframes.append(df)
            print_status(f"Loaded {taxi_type} {year}-{month}: {df.count():,} trips")
        else:
            print_status(f"Failed to load {file_path.name}", "ERROR")
    
    if not dataframes:
        return None
    
    # Union all dataframes
    from functools import reduce
    from pyspark.sql import DataFrame
    combined_df = reduce(DataFrame.union, dataframes)
    return combined_df

def load_full_year_data(spark, year, taxi_type):
    """Load full year data using glob pattern (usually works fine)"""
    if year == 2024:
        base_path = PROCESSED_2024_PATH / taxi_type
    else:
        base_path = PROCESSED_2025_PATH / taxi_type
    
    if not base_path.exists():
        return None
    
    try:
        # Use glob pattern to load all files
        df = spark.read.parquet(str(base_path / "*.parquet"))
        print_status(f"Loaded {taxi_type} {year} data: {df.count():,} trips")
        return df
    except Exception as e:
        print_status(f"Error loading {taxi_type} {year} data: {str(e)[:100]}", "WARNING")
        return None

def analyze_trip_patterns(df, taxi_type, year):
    """
    Analyze trip patterns relative to congestion zone
    
    Returns:
        Dictionary with trip pattern statistics
    """
    from pyspark.sql.functions import col, when, count
    
    print_status(f"Analyzing {taxi_type} {year} trip patterns...")
    
    # Define zone conditions
    pickup_in_zone = col("pickup_loc").isin(CONGESTION_ZONE_LOCATIONS)
    dropoff_in_zone = col("dropoff_loc").isin(CONGESTION_ZONE_LOCATIONS)
    
    # Classify trips
    df_classified = df.withColumn(
        "trip_pattern",
        when(~pickup_in_zone & dropoff_in_zone, "Outside → Inside")
        .when(pickup_in_zone & ~dropoff_in_zone, "Inside → Outside")
        .when(pickup_in_zone & dropoff_in_zone, "Inside → Inside")
        .otherwise("Outside → Outside")
    )
    
    # Count trips by pattern
    pattern_counts = df_classified.groupBy("trip_pattern").agg(
        count("*").alias("trip_count")
    ).collect()
    
    # Convert to dictionary
    pattern_dict = {row["trip_pattern"]: row["trip_count"] for row in pattern_counts}
    
    # Calculate total trips entering zone
    entering_zone = pattern_dict.get("Outside → Inside", 0)
    total_trips = df.count()
    
    print_status(f"  Total trips: {total_trips:,}")
    print_status(f"  Trips entering zone: {entering_zone:,}")
    
    return {
        "taxi_type": taxi_type,
        "year": year,
        "total_trips": total_trips,
        "entering_zone": entering_zone,
        "patterns": pattern_dict
    }

def perform_leakage_audit(spark, yellow_2025_df, green_2025_df):
    
    print_header("LEAKAGE AUDIT (Post Jan 5, 2025)")
    
    results = {}
    
    for taxi_type, df in [("Yellow", yellow_2025_df), ("Green", green_2025_df)]:
        if df is None:
            print_status(f"No 2025 data for {taxi_type} taxi", "WARNING")
            continue
        
        print_status(f"Analyzing {taxi_type} taxi leakage...")
        
        from pyspark.sql.functions import col, to_date, count, avg, sum as spark_sum
        
        # Define zone conditions
        pickup_in_zone = col("pickup_loc").isin(CONGESTION_ZONE_LOCATIONS)
        dropoff_in_zone = col("dropoff_loc").isin(CONGESTION_ZONE_LOCATIONS)
        
        # Filter trips:
        # 1. Pickup outside zone
        # 2. Dropoff inside zone  
        # 3. After Jan 5, 2025
        leakage_trips = df.filter(
            ~pickup_in_zone & 
            dropoff_in_zone &
            (to_date(col("pickup_time")) >= CONGESTION_START_DATE)
        )
        
        total_leakage_trips = leakage_trips.count()
        
        if total_leakage_trips == 0:
            print_status(f"  No trips matching leakage criteria for {taxi_type} taxi", "WARNING")
            continue
        
        # Calculate compliance rate
        trips_with_surcharge = leakage_trips.filter(col("congestion_surcharge") > 0).count()
        compliance_rate = (trips_with_surcharge / total_leakage_trips) * 100
        
        # Calculate average surcharge
        avg_surcharge_result = leakage_trips.agg(avg(col("congestion_surcharge"))).collect()
        avg_surcharge = avg_surcharge_result[0][0] if avg_surcharge_result else 0
        
        # Identify top 3 pickup locations with missing surcharges
        # Missing surcharge = trips with 0 surcharge
        missing_surcharge_trips = leakage_trips.filter(col("congestion_surcharge") == 0)
        
        if missing_surcharge_trips.count() > 0:
            top_missing_locations = missing_surcharge_trips.groupBy("pickup_loc").agg(
                count("*").alias("missing_count")
            ).orderBy(col("missing_count").desc()).limit(3).collect()
        else:
            top_missing_locations = []
        
        # Store results
        results[taxi_type] = {
            "total_trips_entering": total_leakage_trips,
            "trips_with_surcharge": trips_with_surcharge,
            "compliance_rate": compliance_rate,
            "avg_surcharge": avg_surcharge,
            "top_missing_locations": top_missing_locations
        }
        
        # Print results
        print_status(f"  Total trips entering zone (post-Jan 5): {total_leakage_trips:,}")
        print_status(f"  Trips with surcharge: {trips_with_surcharge:,}")
        print_status(f"  Compliance rate: {compliance_rate:.2f}%")
        print_status(f"  Average surcharge: ${avg_surcharge:.2f}")
        
        if top_missing_locations:
            print_status(f"  Top 3 pickup locations with missing surcharges:")
            for i, row in enumerate(top_missing_locations, 1):
                print_status(f"    {i}. Location {row['pickup_loc']}: {row['missing_count']:,} trips")
        else:
            print_status(f"  No trips with missing surcharges found", "INFO")
    
    return results

def compare_q1_volumes(spark):
    """
    Compare Q1 2024 vs Q1 2025 trip volumes entering congestion zone
    
    Requirements:
    1. Load processed Q1 2024 data (Jan-Mar)
    2. Load processed Q1 2025 data (Jan-Mar)
    3. For each taxi type, count trips entering zone in both years
    4. Compute percentage change
    """
    print_header("YELLOW vs GREEN DECLINE ANALYSIS (Q1 2024 vs Q1 2025)")
    
    comparison_results = {}
    
    for taxi_type in ['yellow', 'green']:
        print_status(f"Analyzing {taxi_type} taxi Q1 volumes...")
        
        # Load Q1 2024 data
        df_2024 = load_q1_data(spark, 2024, taxi_type)
        if df_2024 is None:
            print_status(f"  No 2024 Q1 data for {taxi_type} taxi", "WARNING")
            continue
        
        # Load Q1 2025 data  
        df_2025 = load_q1_data(spark, 2025, taxi_type)
        if df_2025 is None:
            print_status(f"  No 2025 Q1 data for {taxi_type} taxi", "WARNING")
            continue
        
        from pyspark.sql.functions import col
        
        # Define zone conditions
        pickup_in_zone = col("pickup_loc").isin(CONGESTION_ZONE_LOCATIONS)
        dropoff_in_zone = col("dropoff_loc").isin(CONGESTION_ZONE_LOCATIONS)
        
        # Count trips entering zone in 2024 Q1
        entering_2024 = df_2024.filter(~pickup_in_zone & dropoff_in_zone).count()
        
        # Count trips entering zone in 2025 Q1
        entering_2025 = df_2025.filter(~pickup_in_zone & dropoff_in_zone).count()
        
        # Calculate percentage change
        if entering_2024 > 0:
            pct_change = ((entering_2025 - entering_2024) / entering_2024) * 100
        else:
            pct_change = 0
        
        # Store results
        comparison_results[taxi_type] = {
            "q1_2024_entering": entering_2024,
            "q1_2025_entering": entering_2025,
            "change": entering_2025 - entering_2024,
            "pct_change": pct_change
        }
        
        # Print results
        change_word = "increase" if entering_2025 > entering_2024 else "decrease"
        print_status(f"  Q1 2024 trips entering zone: {entering_2024:,}")
        print_status(f"  Q1 2025 trips entering zone: {entering_2025:,}")
        print_status(f"  Change: {change_word} of {abs(entering_2025 - entering_2024):,} trips")
        print_status(f"  Percentage change: {pct_change:+.2f}%")
    
    return comparison_results

def generate_phase2_report(geospatial_results, leakage_results, decline_results):
    """
    Generate comprehensive Phase 2 report
    """
    print_header("GENERATING PHASE 2 REPORT")
    
    report_lines = []
    report_lines.append("="*80)
    report_lines.append("NYC CONGESTION PRICING AUDIT - PHASE 2 REPORT")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("="*80)
    
    # Section 1: Geospatial Mapping
    report_lines.append("\n" + "="*80)
    report_lines.append("1. GEOSPATIAL MAPPING - TRIP PATTERNS")
    report_lines.append("="*80)
    report_lines.append(f"Congestion Zone: Manhattan South of 60th St")
    report_lines.append(f"Zone Location IDs: {CONGESTION_ZONE_LOCATIONS[0]} to {CONGESTION_ZONE_LOCATIONS[-1]}")
    report_lines.append("")
    
    for result in geospatial_results:
        report_lines.append(f"{result['taxi_type'].upper()} TAXI {result['year']}:")
        report_lines.append(f"  Total trips: {result['total_trips']:,}")
        report_lines.append(f"  Trips entering zone: {result['entering_zone']:,}")
        report_lines.append(f"  Trip patterns:")
        for pattern, count in result['patterns'].items():
            report_lines.append(f"  {pattern}: {count:,}")
        report_lines.append("")
    
    # Section 2: Leakage Audit
    report_lines.append("\n" + "="*80)
    report_lines.append("2. LEAKAGE AUDIT (Post Jan 5, 2025)")
    report_lines.append("="*80)
    report_lines.append(f"Congestion Pricing Start Date: {CONGESTION_START_DATE}")
    report_lines.append("")
    
    for taxi_type, data in leakage_results.items():
        report_lines.append(f"{taxi_type.upper()} TAXI:")
        report_lines.append(f"  Total trips entering zone (post-Jan 5): {data['total_trips_entering']:,}")
        report_lines.append(f"  Trips with surcharge: {data['trips_with_surcharge']:,}")
        report_lines.append(f"  Compliance rate: {data['compliance_rate']:.2f}%")
        report_lines.append(f"  Average surcharge: ${data['avg_surcharge']:.2f}")
        
        if data['top_missing_locations']:
            report_lines.append(f"  Top 3 pickup locations with missing surcharges:")
            for i, row in enumerate(data['top_missing_locations'], 1):
                report_lines.append(f"    {i}. Location {row['pickup_loc']}: {row['missing_count']:,} trips")
        else:
            report_lines.append(f"  No trips with missing surcharges found")
        report_lines.append("")
    
    # Section 3: Yellow vs Green Decline
    report_lines.append("\n" + "="*80)
    report_lines.append("3. YELLOW vs GREEN DECLINE (Q1 2024 vs Q1 2025)")
    report_lines.append("="*80)
    report_lines.append("Comparison of trips entering congestion zone in Q1")
    report_lines.append("")
    
    for taxi_type, data in decline_results.items():
        report_lines.append(f"{taxi_type.upper()} TAXI:")
        report_lines.append(f"  Q1 2024 trips entering zone: {data['q1_2024_entering']:,}")
        report_lines.append(f"  Q1 2025 trips entering zone: {data['q1_2025_entering']:,}")
        report_lines.append(f"  Change: {data['change']:+,} trips")
        report_lines.append(f"  Percentage change: {data['pct_change']:+.2f}%")
        
        # Interpretation
        if data['pct_change'] < -5:
            report_lines.append(f"  → SIGNIFICANT DECLINE in {taxi_type} taxi usage")
        elif data['pct_change'] > 5:
            report_lines.append(f"  → SIGNIFICANT INCREASE in {taxi_type} taxi usage")
        else:
            report_lines.append(f"  → MINIMAL CHANGE in {taxi_type} taxi usage")
        report_lines.append("")
    
    # Section 4: Key Findings
    report_lines.append("\n" + "="*80)
    report_lines.append("4. KEY FINDINGS & RECOMMENDATIONS")
    report_lines.append("="*80)
    
    # Analyze overall trends
    yellow_change = decline_results.get('yellow', {}).get('pct_change', 0)
    green_change = decline_results.get('green', {}).get('pct_change', 0)
    
    report_lines.append("\nOVERALL IMPACT ASSESSMENT:")
    
    if yellow_change < 0 and green_change < 0:
        report_lines.append("• Both Yellow and Green taxis show DECLINE in zone entries")
        report_lines.append("• Congestion pricing appears to be reducing taxi traffic")
    elif yellow_change < 0 and green_change > 0:
        report_lines.append("• Yellow taxis DECLINE while Green taxis INCREASE")
        report_lines.append("• Possible shift from Yellow to Green taxis")
    elif yellow_change > 0 and green_change < 0:
        report_lines.append("• Yellow taxis INCREASE while Green taxis DECLINE")
        report_lines.append("• Mixed impact on different taxi types")
    else:
        report_lines.append("• Both taxi types show INCREASE in zone entries")
        report_lines.append("• Congestion pricing may not be deterring taxi usage")
    
    # Compliance assessment
    yellow_compliance = leakage_results.get('Yellow', {}).get('compliance_rate', 0)
    green_compliance = leakage_results.get('Green', {}).get('compliance_rate', 0)
    
    report_lines.append("\nSURCHARGE COMPLIANCE ASSESSMENT:")
    report_lines.append(f"• Yellow taxi compliance: {yellow_compliance:.1f}%")
    report_lines.append(f"• Green taxi compliance: {green_compliance:.1f}%")
    
    if yellow_compliance < 90 or green_compliance < 90:
        report_lines.append("• ACTION NEEDED: Compliance below 90% indicates leakage issues")
    else:
        report_lines.append("• GOOD: Compliance rates are above 90%")
    
    report_lines.append("\n" + "="*80)
    report_lines.append("END OF PHASE 2 REPORT")
    report_lines.append("="*80)
    
    report_content = "\n".join(report_lines)
    
    # Print to console
    print("\n" + report_content)
    
    return report_content

def main():
    """Main function for Phase 2 analysis"""
    print_header("NYC CONGESTION PRICING AUDIT - PHASE 2")
    print("CONGESTION ZONE IMPACT ANALYSIS")
    print("Analyzing 2024 vs 2025 Q1 Trip Volumes and Leakage Audit")
    
    start_time = time.time()
    
    try:
        # Step 1: Create output folder
        create_output_folder()
        
        # Step 2: Initialize Spark
        spark = init_spark_session()
        
        # Step 3: Load data for geospatial analysis
        print_header("LOADING DATA FOR ANALYSIS")
        
        # Load full 2025 data for leakage audit (using glob pattern)
        yellow_2025_full = load_full_year_data(spark, 2025, 'yellow')
        green_2025_full = load_full_year_data(spark, 2025, 'green')
        
        # Load full 2024 data for geospatial analysis
        yellow_2024_full = load_full_year_data(spark, 2024, 'yellow')
        green_2024_full = load_full_year_data(spark, 2024, 'green')
        
        # Step 4: Geospatial Mapping
        print_header("GEOSPATIAL MAPPING ANALYSIS")
        
        geospatial_results = []
        
        # Analyze 2024 patterns
        if yellow_2024_full:
            yellow_2024_patterns = analyze_trip_patterns(yellow_2024_full, "Yellow", 2024)
            geospatial_results.append(yellow_2024_patterns)
        
        if green_2024_full:
            green_2024_patterns = analyze_trip_patterns(green_2024_full, "Green", 2024)
            geospatial_results.append(green_2024_patterns)
        
        # Analyze 2025 patterns
        if yellow_2025_full:
            yellow_2025_patterns = analyze_trip_patterns(yellow_2025_full, "Yellow", 2025)
            geospatial_results.append(yellow_2025_patterns)
        
        if green_2025_full:
            green_2025_patterns = analyze_trip_patterns(green_2025_full, "Green", 2025)
            geospatial_results.append(green_2025_patterns)
        
        # Step 5: Leakage Audit (Post Jan 5, 2025)
        leakage_results = perform_leakage_audit(spark, yellow_2025_full, green_2025_full)
        
        # Step 6: Yellow vs Green Decline (Q1 Comparison)
        decline_results = compare_q1_volumes(spark)
        
        # Step 7: Generate Report
        report_content = generate_phase2_report(geospatial_results, leakage_results, decline_results)
        
        # Step 8: Save Report
        report_file = save_report_to_file(report_content)
        
        # Step 9: Cleanup
        spark.stop()
        print_status("PySpark session stopped", "SUCCESS")
        
        # Step 10: Timing
        elapsed = time.time() - start_time
        print(f"\n⏱Total Phase 2 analysis time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        
        print("\n" + "="*70)
        print(" PHASE 2 ANALYSIS COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"\n Report saved to: {report_file}")
        print("\nNext steps:")
        print("1. Review phase2_report.txt for detailed analysis")
        print("  ")
        print("  ")
        
    except Exception as e:
        print_status(f"Phase 2 analysis failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()