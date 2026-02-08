<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>NYC Congestion Pricing Audit (2025)</title>
</head>
<body>

<h1>NYC Congestion Pricing Audit (2025)</h1>

<h2>📋 Executive Summary</h2>
<p>
A comprehensive data engineering and analytics pipeline to evaluate the impact of the
<strong>Manhattan Congestion Relief Zone Toll</strong> implemented on <strong>January 5, 2025</strong>.
This project analyzes <strong>100M+ NYC taxi records</strong> using <strong>PySpark</strong> to answer
critical questions about traffic flow, economic fairness, and policy effectiveness.
</p>

<h3>Key Findings</h3>
<ul>
  <li><strong>Estimated Revenue:</strong> $183.2M from congestion surcharges</li>
  <li><strong>Compliance Rate:</strong> 92.4% across all trips</li>
  <li><strong>Rain Elasticity:</strong> -0.15 (Inelastic demand)</li>
  <li><strong>Ghost Trips:</strong> 0.34% suspicious transactions identified</li>
</ul>

<hr>

<h2>🎯 Project Overview</h2>

<h3>Business Context</h3>
<p>
As a Lead Data Scientist for a private transportation consultancy, this project delivers
a definitive report on how the Manhattan Congestion Relief Zone Toll impacted the taxi
industry and traffic flow throughout 2025.
</p>

<h3>Three Critical Questions Analyzed</h3>
<ul>
  <li><strong>Did it work?</strong> (Traffic flow and revenue analysis)</li>
  <li><strong>Is it fair?</strong> (Impact on driver tips and short-trip economics)</li>
  <li><strong>Is it watertight?</strong> (Leakage, fraud, and "ghost trip" detection)</li>
</ul>

<hr>

<h2>🏗️ Project Architecture</h2>

<h3>Technology Stack</h3>
<pre>
┌─────────────────────────────────────────────┐
│          Streamlit Dashboard                │
│        (Interactive Visualization)          │
└─────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────┐
│           Analytics Pipeline                │
│   (PySpark + Pandas + Geospatial Analysis)  │
└─────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────┐
│         Big Data Engineering Layer          │
│    (Automated Ingestion + Schema Unification)│
└─────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────┐
│         Data Sources (NYC TLC)              │
│   (Yellow/Green Taxi Data + Weather API)    │
└─────────────────────────────────────────────┘
</pre>

<h3>Folder Structure</h3>
<pre>
nyc_congestion/
├── dashboard.py
├── audit_report.pdf.docx
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
├── data/        (Git-ignored)
└── outputs/
    ├── audit_logs/
    ├── phase2_results/
    └── visualizations/
</pre>

<hr>

<h2>🚀 Quick Start</h2>

<h3>Prerequisites</h3>
<ul>
  <li>Python 3.8+</li>
  <li>Java JDK 11 (required for PySpark)</li>
  <li>8GB+ RAM recommended</li>
  <li>Git</li>
</ul>

<h3>Installation</h3>
<pre>
git clone https://github.com/yourusername/nyc-congestion-analysis.git
cd nyc-congestion-analysis
pip install -r requirements.txt
</pre>

<h3>Run Pipeline</h3>
<pre>
python scripts/pipeline.py
</pre>

<h3>Launch Dashboard</h3>
<pre>
streamlit run dashboard.py
</pre>

<hr>

<h2>📊 Analysis Pipeline</h2>

<h3>Phase 1: Big Data Engineering Layer</h3>
<ul>
  <li>Automated web scraping of NYC TLC Parquet files</li>
  <li>Schema unification across Yellow & Green taxis</li>
  <li>Ghost trip detection:
    <ul>
      <li>Average speed &gt; 65 MPH</li>
      <li>Trip &lt; 1 minute with fare &gt; $20</li>
      <li>Zero-distance rides with positive fare</li>
    </ul>
  </li>
  <li>Weighted imputation for missing December 2025 data</li>
</ul>

<h3>Phase 2: Congestion Zone Impact</h3>
<ul>
  <li>Geospatial mapping of Manhattan south of 60th St</li>
  <li>Surcharge compliance rate: <strong>92.4%</strong></li>
  <li>Leakage and pickup location audits</li>
  <li>Q1 2024 vs Q1 2025 volume comparison</li>
</ul>

<h3>Phase 3: Visual Analytics</h3>
<ul>
  <li><strong>Border Effect:</strong> Up to +50% drop-offs near zone boundary</li>
  <li><strong>Velocity Heatmaps:</strong> Green taxis +2.39%, Yellow taxis -1.73%</li>
  <li><strong>Tip Crowding:</strong> Positive correlation observed</li>
</ul>

<h3>Phase 4: Rain Tax</h3>
<ul>
  <li>Open-Meteo precipitation API integration</li>
  <li>Elasticity: -0.15 → Inelastic demand</li>
</ul>

<hr>

<h2>📈 Key Metrics</h2>

<table border="1" cellpadding="6">
  <tr><th>Metric</th><th>Value</th><th>Insight</th></tr>
  <tr><td>Estimated Revenue</td><td>$183.2M</td><td>Strong fiscal impact</td></tr>
  <tr><td>Compliance Rate</td><td>92.4%</td><td>High adherence</td></tr>
  <tr><td>Ghost Trip Rate</td><td>0.34%</td><td>Low fraud incidence</td></tr>
  <tr><td>Rain Elasticity</td><td>-0.15</td><td>Inelastic demand</td></tr>
</table>

<hr>

<h2>📄 License</h2>
<p>This project is licensed under the <strong>MIT License</strong>.</p>

<hr>

<h2>📞 Contact</h2>
<p>
<strong>Project Maintainer:</strong> Lead Data Scientist<br>
<strong>Email:</strong> data.science@transport-consultancy.com<br>
<strong>LinkedIn:</strong> Professional Profile
</p>

</body>
</html>
