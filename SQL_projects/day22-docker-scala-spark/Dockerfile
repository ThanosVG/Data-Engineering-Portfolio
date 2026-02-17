FROM eclipse-temurin:17-jre

WORKDIR /app

# Install Python
RUN apt-get update && apt-get install -y python3.10 python3-pip && rm -rf /var/lib/apt/lists/*

# Install PySpark (override PEP 668)
RUN pip install --break-system-packages pyspark==3.5.0

# Copy ETL script and data
COPY pyspark_salaries_etl.py .
COPY salaries.csv .

# Run ETL
CMD ["python3", "pyspark_salaries_etl.py"]