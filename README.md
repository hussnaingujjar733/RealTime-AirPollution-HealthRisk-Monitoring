🌍 EnviroHealth: Distributed Real-Time Monitoring Pipeline
🔗 Project Links
Final Demo Video (10 Minutes): https://youtu.be/IVGqxV3CXvA

University: MSc Computer Science & Data Science @ ESILV

Team: Hussnain Amanat Ali, Muhammad Irfan, and Ayman

🛠️ Project Architecture
This project demonstrates an end-to-end Big Data pipeline, transitioning from raw cloud infrastructure to real-time actionable insights. The system ingests high-velocity environmental data from Paris, processes it through a distributed cluster, and provides unified monitoring for both system health and environmental trends.

📦 The Tech Stack
Cluster Orchestration: Hadoop 3.3.6

Resource Management: Apache YARN (1 Master + 5 Workers)

Real-Time Compute: Apache Spark 3.5.7 (Structured Streaming)

Data Ingestion: Apache Kafka

Monitoring & Storage: TIG Stack (Telegraf, InfluxDB 1.6, Grafana)

Data Lake: HDFS (Partitioned by Year/Month)

🖥️ Cluster Configuration
Verified via the YARN Resource Manager (Port 8088) as shown in the project video:

Nodes: 6 Total Active Nodes (1 Master Node, 5 Worker Nodes)

Memory: 8 GB RAM per node (48 GB Total Cluster RAM)

Compute: 8 vCores per node (48 Total vCores)

Environment: Azure Virtual Machines running Ubuntu 24.04 LTS

📊 Integrated Dashboard & Analytics
Our Grafana dashboard provides real-time visualization across two critical layers:

1. Infrastructure Layer (System Health)
Usage Graph: Tracks live memory consumption across the distributed Azure cluster.

Collection: Telegraf agents push real-time physical hardware metrics to InfluxDB.

2. Functional Layer (Environmental Insights)
Pollution Deep-Dive: Real-time PM2.5 concentration tracking via Spark windowed averages.

Weather Correlation: Live cross-analysis between Temperature and pollution metrics.

Worker Audit: Verified batch processing indicating consistent data throughput and pipeline integrity.

🚀 How to Run
1. Initialize Cluster Services
Bash

start-dfs.sh && start-yarn.sh
2. Start Spark Processing
Submit the streaming job to the YARN cluster:

Bash

spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_processor.py
3. Launch Kafka Producer
Run the synchronized weather and air quality producer:

Bash

python3 openaq_producer.py
💡 Technical Challenges Solved
Ubuntu 24.04 Integration: Successfully configured Telegraf and Hadoop components on the latest Ubuntu release, bypassing GPG repository errors.

Real-time Synchronization: Coordinated disparate weather and air quality API streams into a unified Kafka topic for processing.

Distributed Processing: Optimized yarn-site.xml to ensure Spark executors utilized the full 48GB cluster capacity effectively.

👥 Team & Roles
Hussnain Amanat Ali: Project Lead, Spark Processing Logic & Final Presentation.

Muhammad Irfan: Infrastructure Lead, Hadoop Multi-node Configuration & YARN Management.

Ayman: Monitoring Lead, TIG Stack Integration & Grafana Visualizations.