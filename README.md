<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
 
</head>
<body>

<h1>NYC Congestion Pricing Audit (2025)</h1>

 **Live Dashboard:** https://nyc-congestion-pricing-audit-dashboard-2025-mmluewuk7krvpp8hzb.streamlit.app/
<h2>📋 Executive Summary</h2>
<p>
This project presents a large-scale <strong>data engineering, audit, and policy analytics pipeline</strong>
designed to evaluate the real-world impact of the
<strong>Manhattan Congestion Relief Zone Toll</strong> implemented on
<strong>January 5, 2025</strong>.
</p>

<p>
Using <strong>100M+ real NYC TLC taxi trip records</strong>, the system combines
<strong>PySpark-based big data processing</strong>, geospatial analysis, and
economic modeling to assess <strong>traffic outcomes, revenue integrity,
behavioral shifts, and enforcement leakage</strong>.
</p>

<h3>Executive Verdict</h3>
<ul>
  <li>✅ <strong>Revenue generation is strong</strong>, driven primarily by Yellow Taxi compliance</li>
  <li>⚠️ <strong>Traffic speed improvements are marginal</strong>, indicating pricing alone is insufficient</li>
  <li>❌ <strong>Green Taxi compliance is critically low (23.7%)</strong>, exposing major leakage</li>
  <li>✅ <strong>Drivers benefit economically</strong> through higher tipping behavior</li>
</ul>

<hr>

<h2>🎯 Project Objectives</h2>

<ul>
  <li><strong>Evaluate effectiveness:</strong> Did congestion pricing improve traffic flow?</li>
  <li><strong>Audit compliance:</strong> Are surcharges correctly applied?</li>
  <li><strong>Detect leakage:</strong> Identify missing tolls and geographic weak points</li>
  <li><strong>Measure equity:</strong> Assess impact on driver income and passenger behavior</li>
  <li><strong>Ensure reproducibility:</strong> Build a production-style, auditable pipeline</li>
</ul>

<hr>

<h2>🏗️ Project Architecture</h2>

<h3>System Design Philosophy</h3>
<ul>
  <li>Big-data-first processing (no full dataset Pandas loads)</li>
  <li>Independent, auditable scripts</li>
  <li>Pipeline orchestration with graceful failure handling</li>
  <li>Visualization only after aggregation</li>
</ul>

<h3>Technology Stack</h3>
<ul>
  <li><strong>Data Engineering:</strong> PySpark, Parquet</li>
  <li><strong>Analytics:</strong> Pandas, NumPy, Geo-mapping</li>
  <li><strong>Visualization:</strong> Matplotlib, Seaborn, Streamlit</li>
  <li><strong>External Data:</strong> NYC TLC Open Data, Open-Meteo API</li>
</ul>

<h3>Folder Structure</h3>
<pre>
nyc_congestion/
├── dashboard.py
├── requirements.txt
│
├── scripts/
│   ├── scraper.py
│   ├── processor.py
│   ├── processor_2024.py
│   ├── phase2_analysis.py
│   ├── phase3_visualizations1.py
│   ├── phase3_visualizations2.py
│   ├── phase3_visualizations3.py
│   ├── phase4_rain_tax.py
│   └── pipeline.py
│
├── data/        (Git-ignored: raw Parquet files)
└── outputs/
    ├── audit_logs/
    ├── audit_logs_124/
    ├── phase2_results/
    └── visualizations/
</pre>

<hr>

<h2>🔁 Pipeline Execution Order</h2>

<ol>
  <li><strong>scraper.py</strong> – Automated ingestion of TLC Parquet data</li>
  <li><strong>processor.py</strong> – 2025 data cleaning, auditing, and aggregation</li>
  <li><strong>processor_2024.py</strong> – Baseline processing for pre-policy comparison</li>
  <li><strong>phase2_analysis.py</strong> – Congestion zone geospatial & compliance audit</li>
  <li><strong>phase3_visualizations*</strong> – Border effects, velocity, tipping analysis</li>
  <li><strong>phase4_rain_tax.py</strong> – Rain elasticity & demand sensitivity</li>
  <li><strong>pipeline.py</strong> – End-to-end orchestration (resource permitting)</li>
</ol>

<p>
Each script can be executed independently. The pipeline script ensures
<strong>full reproducibility</strong> while isolating failures caused by memory
or compute constraints.
</p>

<hr>

<h2>📊 Core Findings</h2>

<h3>1️⃣ Traffic Flow (Velocity Analysis)</h3>
<ul>
  <li>Yellow taxis: <strong>-1.73%</strong> average speed (13.39 → 13.16 MPH)</li>
  <li>Green taxis: <strong>+2.39%</strong> average speed (12.31 → 12.61 MPH)</li>
</ul>
<p><strong>Conclusion:</strong> Congestion pricing alone did not significantly improve overall traffic speed.</p>

<h3>2️⃣ Border Effect (Behavioral Shift)</h3>
<ul>
  <li>Drop-offs near congestion boundaries increased by up to <strong>+50%</strong></li>
  <li>Some internal zones saw declines of <strong>-43%</strong></li>
</ul>
<p><strong>Conclusion:</strong> Strong evidence of toll-avoidance behavior.</p>

<h3>3️⃣ Driver Compensation Impact</h3>
<ul>
  <li>Yellow taxi tips: <strong>+40.86%</strong>, correlation = <strong>+0.39</strong></li>
  <li>Green taxi tips: <strong>+34.85%</strong>, correlation ≈ 0</li>
</ul>
<p><strong>Conclusion:</strong> No crowding-out effect; drivers benefit financially.</p>

<h3>4️⃣ Rain Elasticity</h3>
<ul>
  <li>Elasticity: <strong>-1.06% per mm of rain</strong></li>
  <li>Correlation: <strong>+0.031</strong> (negligible)</li>
</ul>
<p><strong>Conclusion:</strong> Weather has minimal influence on demand.</p>

<h3>5️⃣ Compliance & Leakage Audit</h3>

<table border="1" cellpadding="6">
<tr><th>Metric</th><th>Yellow Taxi</th><th>Green Taxi</th></tr>
<tr><td>Compliance Rate</td><td>72.1%</td><td>23.7%</td></tr>
<tr><td>Avg Surcharge</td><td>$2.33</td><td>$0.83</td></tr>
<tr><td>Zone Entry Change</td><td>+16.95%</td><td>-23.67%</td></tr>
</table>

<p>
Critical leakage zones identified at pickup locations:
<strong>260, 244, 223</strong>.
</p>

<hr>

<h2>💡 Strategic Recommendations</h2>

<ul>
  <li>Urgently raise Green Taxi compliance to <strong>85%+</strong></li>
  <li>Audit vendor surcharge logic and GPS boundary handling</li>
  <li>Introduce graduated or dynamic toll pricing near borders</li>
  <li>Leverage driver advocacy (tips increased, not harmed)</li>
  <li>Avoid weather-based demand models; focus on temporal patterns</li>
</ul>

<hr>

<h2>🚀 Quick Start</h2>

<pre>
git clone https://github.com/yourusername/nyc-congestion-audit.git
cd nyc-congestion-audit
pip install -r requirements.txt
python scripts/pipeline.py
streamlit run dashboard.py
</pre>

<hr>


</body>
</html>
