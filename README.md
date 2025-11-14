# Healthcare Data Pipeline: FHIR to Tabular Transformation

**Orchestrating Healthcare ETL with Apache Airflow and PySpark**

[![Apache Airflow](https://img.shields.io/badge/Airflow-3.1.2-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C?logo=apache-spark)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Additional Resources](#-additional-resources)


---

## 🎯 Overview

This project implements a **automated data pipeline** that transforms HL7 FHIR (Fast Healthcare Interoperability Resources) formatted healthcare data into structured tabular formats suitable for analytics and visualization. The pipeline leverages **Apache Airflow** for orchestration, **PySpark** for distributed data processing.

---

## 🔍 Problem Statement

Healthcare data interoperability remains a critical challenge in modern healthcare systems. The HL7 FHIR standard, while providing a robust framework for healthcare data exchange, presents significant barriers for data analysts and business intelligence teams due to its **complex nested JSON structure**.

### The Challenge

- **Volume**: A single patient's FHIR bundle can contain 1-50MB of data with thousands of nested resources
- **Variety**: Deeply nested JSON (5+ levels) with arrays and objects requiring sophisticated parsing
- **Velocity**: Continuous data arrival from multiple sources (EHRs, labs, imaging systems)
- **Veracity**: Common data quality issues requiring robust validation and error handling

### The Solution

This project builds an **end-to-end automated ETL pipeline** that:
1. Detects incoming FHIR patient bundles
2. Extracts patient demographics, medical conditions, and clinical encounters
3. Transforms nested JSON into flat CSV tables
4. Outputs analytics-ready data for tools like Tableau, Power BI, or Python pandas


---

## 🏗️ Architecture

![Healthcare Data Pipeline Architecture](healthcare-workflow-diagram.jpg)

---

## ✨ Key Features

### Pipeline Orchestration
- **Apache Airflow DAG** with 6 sequential tasks
-  **Conditional execution** using ShortCircuitOperator
-  **Automatic retry** mechanism with configurable delays
-  **Complete audit trail** with task logs and execution history
-  **Race condition prevention** via staging/processing folder pattern

### Data Processing
-  **Distributed processing** with PySpark for scalability
-  **Schema inference** from nested JSON structures
-  **Robust error handling** for missing or malformed data
-  **Parallel CSV output** with Spark partitioning


---


## 🚀 Setup Instructions

### Prerequisites

- **Docker Desktop** installed and running

### Step 1: Clone the Repository

```bash
git clone https://github.com/vedanthirekar/healthcare-data-pipeline.git
cd healthcare-data-pipeline
```

### Step 2: Build Custom Docker Image

The project uses a custom Docker image with PySpark and Java pre-installed:

```bash
docker compose build
```

⏳ *This takes 5-10 minutes the first time*

### Step 3: Start Airflow Services

```bash
docker compose up -d
```

Wait for all services to be healthy:

```bash
docker compose ps
```

Expected output: 7 services running (apiserver, scheduler, worker, dag-processor, triggerer, postgres, redis)

### Step 4: Access Airflow UI

- **URL:** http://localhost:8080
- **Username:** `airflow`
- **Password:** `airflow`

### Step 5: Enable the DAG

1. Navigate to the Airflow UI
2. Find `healthcare_data_pipeline` in the DAG list
3. Toggle the switch to **ON**

### Step 6: Set Up Data Source

**Important:** This repository does not include synthetic data generation. You need to set up a data source to simulate incoming FHIR bundles.

#### Option 1: Use Synthea (Recommended)

[Synthea](https://github.com/synthetichealth/synthea) is a synthetic patient generator that creates realistic FHIR data.

**Installation:**
```bash
# Clone Synthea repository
git clone https://github.com/synthetichealth/synthea.git
cd synthea

# Build Synthea (requires Java)
./gradlew build check test

# Generate patient data
./run_synthea -p 10  # Generates 10 patients
```

**Copy generated data to pipeline:**
```bash
# Copy FHIR bundles to the pipeline's input folder
cp output/fhir/*.json /path/to/healthcare-data-pipeline/data/generated_data/fhir/
```

#### Option 2: Use Sample FHIR Data

Download sample FHIR bundles from:
- [FHIR Examples](https://www.hl7.org/fhir/downloads.html)
- [Public FHIR Test Servers](https://confluence.hl7.org/display/FHIR/Public+Test+Servers)

Place the JSON files in `data/generated_data/fhir/` folder.

**Note:** The pipeline expects FHIR HL7 format JSON bundles with Patient, Condition, and Encounter resources.

---

## 📊 Usage

### Monitoring the Pipeline

1. **Airflow UI** (http://localhost:8080)
   - View DAG runs and task status
   - Check execution logs
   - Monitor task duration


2. **Output Folders**
   - `data/processed_data/patients/` - Patient demographics CSV
   - `data/processed_data/conditions/` - Medical conditions CSV
   - `data/processed_data/encounters/` - Clinical encounters CSV



### Pipeline Behavior

- **Every 5 minutes**: DAG checks for new files
- **If files exist**: Pipeline runs all 6 tasks sequentially
- **If no files**: Downstream tasks are skipped (success state)
- **On completion**: Files are archived and deleted from processing folder

### Stopping the Pipeline

```bash
# Stop data generator: Press Ctrl+C in its terminal

# Stop Airflow services
docker compose down
```

---

## 📚 Additional Resources

- [Project Report (PDF)](Healthcare%20Data%20Pipeline%20Project%20Report.pdf) - Detailed academic report
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [HL7 FHIR Specification](https://hl7.org/fhir/)
- [Synthea Documentation](https://synthetichealth.github.io/synthea/)

---
