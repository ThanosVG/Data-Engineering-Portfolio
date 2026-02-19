# Data Engineering Portfolio

![Badge](https://img.shields.io/badge/Python-Advanced-blue) ![Badge](https://img.shields.io/badge/Spark-Advanced-blue) ![Badge](https://img.shields.io/badge/Kafka-Intermediate-yellow) ![Badge](https://img.shields.io/badge/Docker-Beginner-red)  

Welcome to my Data Engineering portfolio. I'm building skills in ETL pipelines, big data tools, and data architecture. This repo showcases hands-on projects from my 30-day learning journey.  

## Projects  

### 1. SQL ETL (Days 1-8)  
- **Description**: Built employee ETL with SQL (ingestion, transformations, aggs).  
- **Tech**: SQLite, Python.  
- **Key Files**: employee_etl.py, salary_analysis.sql.  
- **Learn more**: [Folder](sql-etl).  

### 2. PySpark ETL (Days 9-13)  
- **Description**: Local PySpark pipeline for salaries data (bonus calc, aggs, Parquet output).  
- **Tech**: PySpark, Pandas.  
- **Key Files**: pyspark_salaries_etl.py.  
- **Learn more**: [Folder](pyspark-etl).  

### 3. Scala Spark (Days 14-16)  
- **Description**: Scala version of ETL with joins/windows.  
- **Tech**: Scala, Spark.  
- **Key Files**: Day15Etl.scala.  
- **Learn more**: [Folder](scala-etl).  
 
### 4. Kafka Streaming (Days 17-19)  
- **Description**: Streaming ingestion with Kafka + Spark (real-time aggs).  
- **Tech**: Kafka, PySpark Streaming.  
- **Key Files**: kafka_producer.py, spark_kafka_consumer.py.  
- **Learn more**: [Folder](kafka-streaming).  

### 5. Docker Containerization (Days 20-23)  
- **Description**: Dockerized PySpark/Scala/Kafka pipelines.  
- **Tech**: Docker.
- **Key Files**: Dockerfile, Dockerfile-kafka.  
- **Learn more**: [Folder](docker-pipelines).  

### 6. Combined Pipeline (Days 24-25)  
- **Description**: End-to-end streaming ETL (Kafka → Scala Spark → Parquet).  
- **Tech**: Kafka, Scala Spark Streaming.  
- **Key Files**: Day25Streaming.scala, kafka_producer.py.  
- **Learn more**: [Folder](combined-pipeline).  

## Tech Stack  
- Languages: Python, Scala, SQL  
- Tools: PySpark, Kafka, Docker, Spark Streaming  

## Next Steps  
- Cloud integration (AWS EMR)  
- CI/CD with GitHub Actions  
- Full data architecture project  

Contact me: [Email](t.vgenopoulos@gmail.com)  

Detailed day by day:
"Day 1.00: GitHub setup complete. Ready for Data Engineering projects."  
"Day 2.00: SQL basics with contact DB project."  
"Day 2.01: Fixed table exists error, added robust CREATE, and verified contact DB."  
"Day 3.00: Added test.db from Day 2 SQL experiments."  
"Day 3.01: Python dashboard output."  
"Day 3.02: Added salary analysis script and employee DB."  
"Day 4.01: Completed joins, reports, and optimization."  
"Day 5.00: Added views, plans, and optimization."  
"Day 6.00: Python ETL Pipeline - Processed employee data with bonuses and error handling."  
"Day 7.00: Started API ETL on day7-api-etl branch"  
"Day 7.01: Fixed KeyError in API ETL by extracting nested weather description"  
"Day 7.02: Added retry logic to API ETL for better error handling"  
"Day 8.00: Added multi-source ETL pipeline combining CSV and API"  
"Day 8.01: Updated ETL script with random city generation"  
"Day 8.02: Fixed variable inconsistency in enriched ETL script"  
"Day 8.03: Fixed SyntaxError in enriched ETL script by removing formatting artifacts"  
"Day 8.04: Added bonus calculation to enriched ETL script"  
"Day 8.05: Fixed ValueError in enriched ETL by using row-wise apply for bonus adjustment"  
"Day 9.00: PySpark ETL project - salaries with bonus, group-bys and partitioning"  
"Day 9.01: PySpark ETL with random bonus, group bys, and partition"  
"Day 10.00: Producer: Reads `salaries.csv` and sends each row as JSON to Kafka topic `salaries_stream` (simulates real-time ingestion)  
"Day 11.00: Consumer: Pulls messages from the topic and prints them live (simulates processing/sink)"  
"Day 12.00: PySpark advanced - join, bonus logic, quality flags, partition"  
"Day 13.00: PySpark advanced - window functions, caching, quality checks"  
"Day 14.00: Scala Spark ETL complete"  
"Day 14.01: Scala Spark ETL - fixed JAR compilation with sbt"  
"Day 14.02: Fixed build.sbt syntax for Scala Spark ETL"  
"Day 14.03: Fixed build.sbt with sbt-assembly plugin for Scala Spark ETL"  
"Day 14.04: Added sbt-assembly plugin to fix build"  
"Day 14.05: Fixed Scala chaining syntax for ETL"  
"Day 15.00: Scala Spark with joins - enriched salaries ETL"  
"Day 15.01: Scala Spark with joins - fixed assembly plugin error"  
"Day 15.02: Scala Spark ETL - added script and ignored target folder"  
"Day 16.00: Scala Spark with window functions - running totals and ranks"  
"Day 16.01: Fixed Window import error for Scala Spark ETL"  
"Day 17.00: Kafka + Spark Streaming - real-time ingestion pipeline"  
"Day 17.01: Fixed Kafka connector for Spark streaming"  
"Day 17.02: Kafka + Spark Streaming - fixed binary Spark setup"  
"Day 17.03: Kafka + Spark Streaming - fixed timestamp column"  
"Day 18.00: Kafka + Spark Streaming - added file output and quality checks"  
"Day 19.00: Kafka + Spark Streaming - added more aggregations by experience_level"  
"Day 20.00: Docker fundamentals - first container with Python script" 
"Day 20.01: Homework: Updated Docker script"   
"Day 21.00: Dockerized PySpark ETL - portable pipeline"  
"Day 21.01: Fixed JAVA_HOME in Docker for PySpark ETL"  
"Day 21.02: Fixed Java install in Docker for PySpark ETL"  
"Day 21.03: Fixed Docker tag for Java base in PySpark ETL"  
"Day 21.04: Fixed Docker base image for PySpark ETL"  
"Day 21.05: Fixed Docker base tag with Temurin for PySpark ETL"  
"Day 21.06: Fixed S3 path for local Docker PySpark ETL"  
"Day 21.07: Fixed OUTPUT_FOLDER in PySpark ETL script"  
"Day 22.00: Dockerized Scala Spark ETL - portable pipeline"  
"Day 22.01: Dockerized Scala Spark ETL - fixed sbt install"  
"Day 22.02: Dockerized Scala Spark ETL - fixed Spark dep in build.sbt"  
"Day 22.03: Dockerized Scala Spark ETL - added Spark install"  
"Day 21.04: Fixed legacy ENV format in Dockerfile for PySpark ETL"  
"Day 22.00: Dockerized Scala Spark ETL - portable pipeline"  
"Day 22.01: Dockerized Scala Spark ETL - fixed sbt install"  
"Day 22.02: Dockerized Scala Spark ETL - fixed Spark dep in build.sbt"  
"Day 22.03: Dockerized Scala Spark ETL - added Spark install"  
"Day 22.04: Fixed legacy ENV format in Dockerfile"  
"Day 22.05: Dockerized Scala Spark ETL - added Spark install"  
"Day 22.06: Fixed CSV copy in Docker for Scala Spark ETL"  
"Day 22.05: Fixed CSV copy for Scala Spark ETL"  
"Day 23.00: Dockerized Kafka pipeline - broker, producer, consumer"  
"Day 23.01: Dockerized Kafka pipeline - fixed daemon connection"  
"Day 23.02: Fixed JSON CMD warning in Dockerfile"  
"Day 23.03: Fixed JSON CMD warning and consumer build"  
"Day 23.04: Fixed PEP 668 error in Docker consumer"  
"Day 23.05: Fixed container name conflict for Kafka broker"  
"Day 24.00: Combined Kafka + PySpark streaming ETL pipeline"   
"Day 24.01: Fixed kafka-python dependency for producer"  
"Day 24.02: Fixed broker connection for combined pipeline"  
"Day 24.03: Dockerized producer for combined pipeline"  
"Day 24.04: Fixed pandas dependency in producer"  
"Day 24.05: Fixed Docker host connection for producer"  
"Day 24.06: Fixed Docker daemon connection"  
"Day 24.07: Fixed producer connection to Docker broker"  
"Day 24.08: Fixed host connection address for pipeline"  
"Day 24.09: Fixed producer timeout with retries"  
"Day 24.10: Fixed broker startup CMD for combined pipeline"  
"Day 24.11: Fixed JSON CMD warning in Kafka broker"  
"Day 24.12: Combined pipeline complete - fixed startup and topic"  
"Day 24.13: Dockerized producer to fix connection"  
"Day 24.14: Fixed Spark session conflict for consumer"  
"Day 25.00: Scala + Kafka streaming pipeline"    
"Day 26.00: Portfolio Polish"  