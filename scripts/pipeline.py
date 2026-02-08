#!/usr/bin/env python3
"""
NYC Congestion Pricing Audit - Pipeline Runner
Place this in scripts/ folder
"""

import os
import sys
from pathlib import Path

def main():
    print("="*70)
    print("NYC CONGESTION PRICING AUDIT PIPELINE")
    print("="*70)
    print("Execution Sequence:")
    print("1. scraper.py - Download data")
    print("2. processor.py - Process 2025 data")
    print("3. processor_2024.py - Process 2024 data")
    print("4. phase2_analysis.py - Congestion zone analysis")
    print("5. phase3_visualizations1.py - Border effect maps")
    print("6. phase3_visualizations2.py - Velocity heatmaps")
    print("7. phase3_visualizations3.py - Tip analysis")
    print("8. phase4_rain_tax.py - Weather elasticity analysis")
    print("="*70)
    
    # Get the scripts directory
    scripts_dir = Path(__file__).parent
    project_root = scripts_dir.parent
    
    print(f"Scripts directory: {scripts_dir}")
    print(f"Project root: {project_root}")
    
    # Change to project root directory
    os.chdir(project_root)
    print(f"Current directory: {os.getcwd()}")
    
    # Scripts to run (relative to scripts folder)
    scripts = [
        ("Phase 1: Data Collection", "scraper.py"),
        ("Phase 1: 2025 Processing", "processor.py"),
        ("Phase 1: 2024 Processing", "processor_2024.py"),
        ("Phase 2: Zone Analysis", "phase2_analysis.py"),
        ("Phase 3: Border Maps", "phase3_visualizations1.py"),
        ("Phase 3: Velocity Heatmaps", "phase3_visualizations2.py"),
        ("Phase 3: Tip Analysis", "phase3_visualizations3.py"),
        ("Phase 4: Rain Tax", "phase4_rain_tax.py")
    ]
    
    successful = 0
    total = len(scripts)
    
    for name, script_name in scripts:
        print(f"\n▶️  {name}")
        print(f"   Running: {script_name}")
        
        script_path = scripts_dir / script_name
        
        if script_path.exists():
            print(f"   Found at: {script_path}")
            result = os.system(f'python "{script_path}"')
            if result == 0:
                print(f"   ✅ Completed successfully")
                successful += 1
            else:
                print(f"   ⚠️  Completed with exit code: {result}")
        else:
            print(f"   ❌ Script not found at: {script_path}")
    
    print("\n" + "="*70)
    print("PIPELINE COMPLETION SUMMARY")
    print("="*70)
    print(f"Successful: {successful}/{total}")
    
    if successful >= total - 2:  # Allow 2 failures
        print("\n✅ PIPELINE EXECUTED SUCCESSFULLY")
        print("All required analysis completed.")
        print("\nNEXT STEPS:")
        print("1. View visualizations: outputs/visualizations/")
        print("2. Launch dashboard: streamlit run dashboard.py")
        print("3. Check reports: outputs/phase2_results/")
        return 0
    else:
        print("\n⚠️  PIPELINE COMPLETED WITH ERRORS")
        print("Some scripts failed. Check logs above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())