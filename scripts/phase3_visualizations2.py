"""
NYC Congestion Pricing Audit - Phase 3 Visualizations
Visualization 2: "Congestion Velocity" Heatmaps
Analyze if congestion pricing improved traffic speeds inside the congestion zone

Outputs:

congestion_velocity_yellow_heatmap.png - Side-by-side 2024 vs 2025
congestion_velocity_yellow_difference.png - Speed change heatmap
congestion_velocity_green_heatmap.png - Side-by-side 2024 vs 2025
congestion_velocity_green_difference.png - Speed change heatmap
congestion_velocity_summary.txt - Analysis results
"""

import os

# Set Java 11 for PySpark
java_home = r"C:\Program Files\Java\jdk-11"
os.environ['JAVA_HOME'] = java_home
os.environ['PATH'] = f"{java_home}\\bin;{os.environ['PATH']}"

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

PROCESSED_2025_PATH = project_root / "data" / "processed"
PROCESSED_2024_PATH = project_root / "data" / "processed_2024"
OUTPUT_PATH = project_root / "outputs" / "visualizations"

# Congestion zone Location IDs (Manhattan south of 60th St)
# Based on your Phase 2 analysis
CONGESTION_ZONE_IDS = list(range(1, 201))  # Approximation

# Q1 months for comparison
Q1_MONTHS = ['01', '02', '03']

def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*50)
    print(f" {text}")
    print("="*50)

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

def init_spark_session():
    """Initialize PySpark session"""
    print_status("Initializing PySpark session...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("NYC_Congestion_Velocity_Heatmaps") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        print_status("PySpark session created", "SUCCESS")
        return spark
        
    except Exception as e:
        print_status(f"Failed to initialize PySpark: {e}", "ERROR")
        sys.exit(1)

def load_q1_data_for_speed_analysis(spark, year, taxi_type):
    """Load Q1 data and filter for congestion zone trips"""
    print_status(f"Loading {taxi_type} {year} Q1 data for speed analysis...")
    
    if year == 2024:
        base_path = PROCESSED_2024_PATH / taxi_type
    else:
        base_path = PROCESSED_2025_PATH / taxi_type
    
    if not base_path.exists():
        return None
    
    try:
        from functools import reduce
        from pyspark.sql import DataFrame
        from pyspark.sql.functions import col
        
        dataframes = []
        for month in Q1_MONTHS:
            file_pattern = f"clean_{year}_{month}.parquet"
            file_path = base_path / file_pattern
            
            if file_path.exists():
                df = spark.read.parquet(str(file_path))
                # Filter for trips inside congestion zone only
                df = df.filter(
                    col("pickup_loc").isin(CONGESTION_ZONE_IDS) & 
                    col("dropoff_loc").isin(CONGESTION_ZONE_IDS)
                )
                dataframes.append(df)
        
        if not dataframes:
            return None
        
        combined_df = reduce(DataFrame.union, dataframes)
        print_status(f"Loaded {taxi_type} {year} Q1 congestion zone trips: {combined_df.count():,}", "SUCCESS")
        return combined_df
        
    except Exception as e:
        print_status(f"Error loading {taxi_type} {year} data: {str(e)[:100]}", "ERROR")
        return None

def calculate_trip_speeds(spark, df):
    """
    Calculate speed for each trip inside congestion zone
    Speed = distance / time
    """
    print_status("Calculating trip speeds...")
    
    from pyspark.sql.functions import col, unix_timestamp, hour, dayofweek, when
    
    # Calculate trip duration in hours
    df_with_duration = df.withColumn(
        "trip_duration_hours",
        (unix_timestamp(col("dropoff_time")) - unix_timestamp(col("pickup_time"))) / 3600.0
    )
    
    # Filter out trips with invalid duration (0 or negative)
    df_with_duration = df_with_duration.filter(col("trip_duration_hours") > 0)
    
    # Calculate speed in MPH
    df_with_speed = df_with_duration.withColumn(
        "speed_mph",
        col("trip_distance") / col("trip_duration_hours")
    )
    
    # Filter out unrealistic speeds (>65 mph in congestion zone)
    df_with_speed = df_with_speed.filter(col("speed_mph") <= 65)
    
    # Extract hour and day of week
    df_with_time = df_with_speed.withColumn(
        "hour_of_day",
        hour(col("pickup_time"))
    ).withColumn(
        "day_of_week",
        dayofweek(col("pickup_time"))  # 1=Sunday, 2=Monday, ..., 7=Saturday
    )
    
    # Convert day of week to 0-6 where 0=Monday (for better plotting)
    df_with_time = df_with_time.withColumn(
        "day_of_week_adj",
        when(col("day_of_week") == 1, 6)  # Sunday -> 6
        .when(col("day_of_week") == 2, 0)  # Monday -> 0
        .when(col("day_of_week") == 3, 1)  # Tuesday -> 1
        .when(col("day_of_week") == 4, 2)  # Wednesday -> 2
        .when(col("day_of_week") == 5, 3)  # Thursday -> 3
        .when(col("day_of_week") == 6, 4)  # Friday -> 4
        .otherwise(5)  # Saturday -> 5
    )
    
    return df_with_time

def create_velocity_heatmap_data(spark, df_2024, df_2025, taxi_type):
    """
    Create data for velocity heatmaps
    Returns aggregated speed data by hour and day for both years
    """
    print_status(f"Creating velocity heatmap data for {taxi_type} taxis...")
    
    from pyspark.sql.functions import avg, count
    
    # Calculate speeds for 2024
    df_2024_speed = calculate_trip_speeds(spark, df_2024)
    
    # Calculate speeds for 2025
    df_2025_speed = calculate_trip_speeds(spark, df_2025)
    
    # Aggregate by hour and day for 2024
    heatmap_data_2024 = df_2024_speed.groupBy("hour_of_day", "day_of_week_adj").agg(
        avg("speed_mph").alias("avg_speed_mph"),
        count("*").alias("trip_count")
    ).orderBy("day_of_week_adj", "hour_of_day")
    
    # Aggregate by hour and day for 2025
    heatmap_data_2025 = df_2025_speed.groupBy("hour_of_day", "day_of_week_adj").agg(
        avg("speed_mph").alias("avg_speed_mph"),
        count("*").alias("trip_count")
    ).orderBy("day_of_week_adj", "hour_of_day")
    
    # Convert to pandas for visualization
    heatmap_2024_pd = heatmap_data_2024.toPandas()
    heatmap_2025_pd = heatmap_data_2025.toPandas()
    
    print_status(f"2024 heatmap data: {len(heatmap_2024_pd)} time slots", "SUCCESS")
    print_status(f"2025 heatmap data: {len(heatmap_2025_pd)} time slots", "SUCCESS")
    
    return {
        "taxi_type": taxi_type,
        "heatmap_2024": heatmap_2024_pd,
        "heatmap_2025": heatmap_2025_pd
    }

def create_velocity_heatmaps(velocity_data):
    """
    Create side-by-side heatmaps for Q1 2024 vs Q1 2025
    """
    print_header("CREATING CONGESTION VELOCITY HEATMAPS")
    
    try:
        import matplotlib.pyplot as plt
        import numpy as np
        import seaborn as sns
        
        taxi_type = velocity_data["taxi_type"]
        heatmap_2024 = velocity_data["heatmap_2024"]
        heatmap_2025 = velocity_data["heatmap_2025"]
        
        # Create pivot tables for heatmaps
        pivot_2024 = heatmap_2024.pivot_table(
            index='day_of_week_adj',
            columns='hour_of_day',
            values='avg_speed_mph',
            fill_value=0
        )
        
        pivot_2025 = heatmap_2025.pivot_table(
            index='day_of_week_adj',
            columns='hour_of_day',
            values='avg_speed_mph',
            fill_value=0
        )
        
        # Create figure
        fig, axes = plt.subplots(1, 2, figsize=(20, 8))
        
        # Day names for y-axis
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Find common color scale
        vmin = min(pivot_2024.min().min(), pivot_2025.min().min())
        vmax = max(pivot_2024.max().max(), pivot_2025.max().max())
        
        # Plot 2024 heatmap
        ax1 = axes[0]
        sns.heatmap(pivot_2024, ax=ax1, cmap='RdYlGn', vmin=vmin, vmax=vmax,
                   cbar_kws={'label': 'Average Speed (MPH)'})
        ax1.set_title(f'{taxi_type.title()} Taxis - Q1 2024\nAverage Speed in Congestion Zone', 
                     fontsize=14, fontweight='bold', pad=15)
        ax1.set_xlabel('Hour of Day', fontsize=12)
        ax1.set_ylabel('Day of Week', fontsize=12)
        ax1.set_yticklabels(day_names, rotation=0)
        
        # Plot 2025 heatmap
        ax2 = axes[1]
        sns.heatmap(pivot_2025, ax=ax2, cmap='RdYlGn', vmin=vmin, vmax=vmax,
                   cbar_kws={'label': 'Average Speed (MPH)'})
        ax2.set_title(f'{taxi_type.title()} Taxis - Q1 2025\nAverage Speed in Congestion Zone', 
                     fontsize=14, fontweight='bold', pad=15)
        ax2.set_xlabel('Hour of Day', fontsize=12)
        ax2.set_ylabel('')
        ax2.set_yticklabels([''] * 7)
        
        # Calculate overall average speeds
        avg_2024 = pivot_2024.values.mean()
        avg_2025 = pivot_2025.values.mean()
        pct_change = ((avg_2025 - avg_2024) / avg_2024) * 100
        
        # Overall title
        fig.suptitle(f'NYC Congestion Pricing: "Congestion Velocity" Analysis\n{taxi_type.title()} Taxi Speed Heatmaps Inside Manhattan South of 60th St',
                    fontsize=16, fontweight='bold', y=1.05)
        
        # Add statistics
        stats_text = (
            f'Q1 2024 Average Speed: {avg_2024:.2f} MPH\n'
            f'Q1 2025 Average Speed: {avg_2025:.2f} MPH\n'
            f'Change: {pct_change:+.2f}%'
        )
        
        fig.text(0.5, 0.01, stats_text,
                ha='center', fontsize=12, style='italic',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8))
        
        plt.tight_layout(rect=[0, 0.08, 1, 0.95])
        
        # Save
        output_file = OUTPUT_PATH / f"congestion_velocity_{taxi_type.lower()}_heatmap.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print_status(f"Heatmap saved to: {output_file}", "SUCCESS")
        
        # Create difference heatmap
        create_difference_heatmap(pivot_2024, pivot_2025, taxi_type)
        
        # Removed plt.show() to prevent duplicate display
        # plt.show()
        
        return output_file, avg_2024, avg_2025, pct_change
        
    except Exception as e:
        print_status(f"Error creating heatmaps: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return None, 0, 0, 0

def create_difference_heatmap(pivot_2024, pivot_2025, taxi_type):
    """Create a heatmap showing the difference (2025 - 2024)"""
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Calculate difference
        diff = pivot_2025 - pivot_2024
        
        # Create figure
        plt.figure(figsize=(15, 8))
        
        # Use diverging colormap for differences
        max_abs = max(abs(diff.min().min()), abs(diff.max().max()))
        
        ax = sns.heatmap(diff, cmap='RdBu_r', center=0,
                        vmin=-max_abs, vmax=max_abs,
                        cbar_kws={'label': 'Speed Change (MPH)'})
        
        plt.title(f'{taxi_type.title()} Taxis: Speed Change (Q1 2025 - Q1 2024)\nCongestion Zone Velocity Difference',
                 fontsize=14, fontweight='bold', pad=15)
        plt.xlabel('Hour of Day', fontsize=12)
        plt.ylabel('Day of Week', fontsize=12)
        
        # Day names
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        ax.set_yticklabels(day_names, rotation=0)
        
        # Add interpretation
        overall_change = diff.values.mean()
        if overall_change > 0:
            interpretation = f"✅ Overall speed INCREASED by {overall_change:.2f} MPH after congestion pricing"
        else:
            interpretation = f"⚠ Overall speed DECREASED by {abs(overall_change):.2f} MPH after congestion pricing"
        
        plt.figtext(0.5, 0.01, interpretation,
                   ha='center', fontsize=11, style='italic',
                   bbox=dict(boxstyle='round,pad=0.5', facecolor='lightyellow', alpha=0.8))
        
        output_file = OUTPUT_PATH / f"congestion_velocity_{taxi_type.lower()}_difference.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print_status(f"Difference heatmap saved to: {output_file}", "SUCCESS")
        
    except Exception as e:
        print_status(f"Error creating difference heatmap: {e}", "WARNING")

def generate_velocity_summary(yellow_results, green_results):
    """Generate summary of velocity analysis"""
    print_header("CONGESTION VELOCITY ANALYSIS SUMMARY")
    
    summary = []
    summary.append("="*50)
    summary.append("CONGESTION VELOCITY HEATMAP ANALYSIS")
    summary.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary.append("="*50)
    summary.append("\nAnalysis of average taxi speeds inside Manhattan Congestion Zone (south of 60th St)")
    summary.append("Comparison: Q1 2024 (before congestion pricing) vs Q1 2025 (after implementation)")
    summary.append("")
    
    all_results = []
    
    if yellow_results:
        taxi_type = "YELLOW"
        _, avg_2024, avg_2025, pct_change = yellow_results
        
        summary.append(f"{taxi_type} TAXIS:")
        summary.append(f"  Q1 2024 Average Speed: {avg_2024:.2f} MPH")
        summary.append(f"  Q1 2025 Average Speed: {avg_2025:.2f} MPH")
        summary.append(f"  Change: {avg_2025 - avg_2024:+.2f} MPH ({pct_change:+.2f}%)")
        
        if pct_change > 5:
            summary.append(f"  → SIGNIFICANT SPEED INCREASE - Congestion pricing appears to be working")
        elif pct_change > 0:
            summary.append(f"  → MODEST SPEED INCREASE - Some improvement in traffic flow")
        elif pct_change > -5:
            summary.append(f"  → MINIMAL CHANGE - Congestion pricing has little effect on speeds")
        else:
            summary.append(f"  → SPEED DECREASE - Traffic may have worsened despite pricing")
        
        all_results.append(("Yellow", avg_2024, avg_2025, pct_change))
        summary.append("")
    
    if green_results:
        taxi_type = "GREEN"
        _, avg_2024, avg_2025, pct_change = green_results
        
        summary.append(f"{taxi_type} TAXIS:")
        summary.append(f"  Q1 2024 Average Speed: {avg_2024:.2f} MPH")
        summary.append(f"  Q1 2025 Average Speed: {avg_2025:.2f} MPH")
        summary.append(f"  Change: {avg_2025 - avg_2024:+.2f} MPH ({pct_change:+.2f}%)")
        
        all_results.append(("Green", avg_2024, avg_2025, pct_change))
        summary.append("")
    
    # Overall conclusion
    summary.append("="*50)
    summary.append("OVERALL ASSESSMENT OF CONGESTION PRICING IMPACT:")
    
    if all_results:
        yellow_speed_up = any(r[3] > 5 for r in all_results if r[0] == "Yellow")
        green_speed_up = any(r[3] > 5 for r in all_results if r[0] == "Green")
        
        if yellow_speed_up and green_speed_up:
            summary.append("-->STRONG EVIDENCE that congestion pricing IMPROVED traffic flow")
            summary.append("   Both Yellow and Green taxis show significant speed increases")
        elif yellow_speed_up or green_speed_up:
            summary.append("⚠ MIXED EVIDENCE on traffic flow improvement")
            summary.append("   Some taxi types show improvement, others don't")
        else:
            summary.append("-->LITTLE EVIDENCE that congestion pricing improved traffic flow")
            summary.append("   Taxi speeds show minimal or negative change")
        
        # Hypothesis test
        summary.append("\nHYPOTHESIS TEST: 'Did the toll actually speed up traffic?'")
        
        overall_avg_change = sum(r[3] for r in all_results) / len(all_results)
        if overall_avg_change > 5:
            summary.append(f"--> SUPPORTED: Overall speed increased by {overall_avg_change:.1f}%")
        elif overall_avg_change > 0:
            summary.append(f"⚠ PARTIALLY SUPPORTED: Small speed increase ({overall_avg_change:.1f}%)")
        else:
            summary.append(f"--> NOT SUPPORTED: Speed decreased or unchanged")
    
    summary.append("="*50)
    
    summary_text = "\n".join(summary)
    print(summary_text)
    
    # Save to file
    summary_file = OUTPUT_PATH / "congestion_velocity_summary.txt"
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(summary_text)
    
    print_status(f"Summary saved to: {summary_file}", "SUCCESS")
    
    return summary_text

def main():
    """Main function for Congestion Velocity Heatmap Analysis"""
    print_header("NYC CONGESTION PRICING AUDIT - PHASE 3")
    print("VISUALIZATION 2: CONGESTION VELOCITY HEATMAPS")
    print("Analyzing if congestion pricing improved traffic speeds")
    
    start_time = time.time()
    
    try:
        # Step 1: Ensure output folder exists
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        
        # Step 2: Initialize Spark
        spark = init_spark_session()
        
        # Step 3: Load data
        print_header("LOADING CONGESTION ZONE DATA")
        
        yellow_2024 = load_q1_data_for_speed_analysis(spark, 2024, 'yellow')
        yellow_2025 = load_q1_data_for_speed_analysis(spark, 2025, 'yellow')
        green_2024 = load_q1_data_for_speed_analysis(spark, 2024, 'green')
        green_2025 = load_q1_data_for_speed_analysis(spark, 2025, 'green')
        
        if yellow_2024 is None or yellow_2025 is None:
            print_status("Insufficient Yellow taxi data", "ERROR")
            spark.stop()
            return
        
        # Step 4: Create velocity heatmaps
        print_header("ANALYZING TRAFFIC SPEEDS")
        
        # Yellow taxis
        yellow_velocity_data = create_velocity_heatmap_data(spark, yellow_2024, yellow_2025, "Yellow")
        yellow_results = create_velocity_heatmaps(yellow_velocity_data)
        
        # Green taxis
        green_velocity_data = create_velocity_heatmap_data(spark, green_2024, green_2025, "Green")
        green_results = create_velocity_heatmaps(green_velocity_data)
        
        # Step 5: Generate summary
        summary = generate_velocity_summary(yellow_results, green_results)
        
        # Step 6: Cleanup
        spark.stop()
        print_status("PySpark session stopped", "SUCCESS")
        
        # Step 7: Timing
        elapsed = time.time() - start_time
        print(f"\n⏱Total time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        
        print("\n" + "="*50)
        print("CONGESTION VELOCITY ANALYSIS COMPLETED")
        print("="*50)
        
        print("\nOutputs created:")
        if yellow_results:
            print(f"1. Yellow Taxi Heatmaps: {OUTPUT_PATH / 'congestion_velocity_yellow_heatmap.png'}\n")
            print(f"2. Yellow Difference Map: {OUTPUT_PATH / 'congestion_velocity_yellow_difference.png'}\n")
        if green_results:
            print(f"3. Green Taxi Heatmaps: {OUTPUT_PATH / 'congestion_velocity_green_heatmap.png'}\n")
            print(f"4. Green Difference Map: {OUTPUT_PATH / 'congestion_velocity_green_difference.png'}\n")
        print(f"5. Velocity Summary: {OUTPUT_PATH / 'congestion_velocity_summary.txt'}")
        
        print("\nNext: Proceed to Visualization 3 (Tip Crowding Out Analysis)")
        
    except Exception as e:
        print_status(f"Velocity analysis failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()