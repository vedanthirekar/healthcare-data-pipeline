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
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Pipeline Details](#pipeline-details)
- [Data Flow](#data-flow)
- [Results](#results)
- [Future Enhancements](#future-enhancements)
- [Author](#author)

---

## 🎯 Overview

This project implements a **production-ready automated data pipeline** that transforms HL7 FHIR (Fast Healthcare Interoperability Resources) formatted healthcare data into structured tabular formats suitable for analytics and visualization. The pipeline leverages **Apache Airflow** for orchestration, **PySpark** for distributed data processing, and **Docker** for containerization.

**Author:** Vedant Hirekar  
**Course:** Management, Access and Use of Big Data  
**GitHub:** [healthcare-data-pipeline](https://github.com/vedanthirekar/healthcare-data-pipeline)

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

**Result:** 284:1 compression ratio (142 MB → 500 KB) while preserving critical clinical information

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Generation Layer                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Synthea FHIR Generator + Custom Script                  │  │
│  │  Generates patient bundles at random intervals (10-60s)  │  │
│  └────────────────────┬─────────────────────────────────────┘  │
└────────────────────────┼────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Ingestion Layer (Staging)                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  data/generated_data/fhir/                               │  │
│  │  - Incoming FHIR bundles (simulates real-time data)     │  │
│  └────────────────────┬─────────────────────────────────────┘  │
└────────────────────────┼────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Orchestration Layer (Apache Airflow)                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  DAG: healthcare_data_pipeline (runs every 5 minutes)    │  │
│  │                                                           │  │
│  │  Tasks:                                                   │  │
│  │  1. check_for_files → Move to processing folder         │  │
│  │  2. cleanfolder → Filter patient bundles only           │  │
│  │  3. patients_job → Extract patient demographics         │  │
│  │  4. conditions_job → Extract medical conditions         │  │
│  │  5. encounters_job → Extract clinical encounters        │  │
│  │  6. cleanup → Archive and delete processed files        │  │
│  └────────────────────┬─────────────────────────────────────┘  │
└────────────────────────┼────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Processing Layer (Apache Spark)                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  PySpark DataFrames                                      │  │
│  │  - JSON schema inference                                 │  │
│  │  - Nested field extraction                               │  │
│  │  - Parallel processing across partitions                 │  │
│  └────────────────────┬─────────────────────────────────────┘  │
└────────────────────────┼────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Storage Layer                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  data/processed_data/                                    │  │
│  │  ├── patients/ (CSV partitions)                          │  │
│  │  ├── conditions/ (CSV partitions)                        │  │
│  │  └── encounters/ (CSV partitions)                        │  │
│  │                                                           │  │
│  │  data/archive/YYYY/MM/DD/HH-MM-SS/                       │  │
│  │  └── Archived FHIR bundles (audit trail)                 │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

### Pipeline Orchestration
- ✅ **Apache Airflow DAG** with 6 sequential tasks
- ✅ **Conditional execution** using ShortCircuitOperator
- ✅ **Automatic retry** mechanism with configurable delays
- ✅ **Complete audit trail** with task logs and execution history

### Data Processing
- ✅ **Distributed processing** with PySpark for scalability
- ✅ **Schema inference** from nested JSON structures
- ✅ **Robust error handling** for missing or malformed data
- ✅ **Parallel CSV output** with Spark partitioning

### Production Patterns
- ✅ **Race condition prevention** via staging/processing folder pattern
- ✅ **Data archiving** for compliance and reprocessing
- ✅ **Idempotent operations** for reliability
- ✅ **Containerized deployment** with Docker Compose

### Data Simulation
- ✅ **Synthea integration** for realistic FHIR data generation
- ✅ **Custom data generator** script for random interval simulation
- ✅ **Real-time data arrival** simulation (10-60 second intervals)

---

## 🛠️ Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Orchestration | Apache Airflow | 3.1.2 | Workflow management |
| Processing | Apache Spark (PySpark) | 3.5.0 | Distributed data processing |
| Container | Docker / Docker Compose | Latest | Environment isolation |
| Database | PostgreSQL | 16 | Airflow metadata |
| Message Queue | Redis | 7.2 | Celery task queue |
| Runtime | Python | 3.12 | Scripting language |
| JVM | OpenJDK | 17 | Spark dependency |
| Data Generator | Synthea | Latest | Synthetic FHIR data |

---

## 📁 Project Structure

```
healthcare-data-pipeline/
├── dags/
│   └── main_pipeline.py              # Airflow DAG definition
├── scripts/
│   ├── cleanfolder.py                 # Filter patient bundles
│   ├── patients_job.py                # Extract patient demographics
│   ├── conditions_job.py              # Extract medical conditions
│   ├── encounters_job.py              # Extract clinical encounters
│   ├── cleanup.py                     # Archive and cleanup
│   └── data_generator.py              # Simulate real-time data arrival
├── data/
│   ├── generated_data/
│   │   ├── fhir/                      # Incoming FHIR bundles
│   │   └── processing/                # Isolated processing folder
│   ├── processed_data/
│   │   ├── patients/                  # Patient CSV output
│   │   ├── conditions/                # Conditions CSV output
│   │   └── encounters/                # Encounters CSV output
│   └── archive/                       # Archived raw files
│       └── YYYY/MM/DD/HH-MM-SS/
├── logs/                              # Airflow logs
├── config/                            # Airflow configuration
├── plugins/                           # Custom Airflow plugins
├── docker-compose.yaml                # Docker services definition
├── Dockerfile                         # Custom Airflow image
├── .env                               # Environment variables
└── README.md                          # This file
```

---

## 🚀 Setup Instructions

### Prerequisites

- **Docker Desktop** installed and running
- **8GB RAM** minimum (16GB recommended)
- **20GB free disk space**
- **Windows 11 with WSL2** (or Linux/Mac)

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

### Step 6: Start Data Generator

In a **separate terminal**, run the data generator to simulate real-time data arrival:

```bash
python scripts/data_generator.py
```

This script:
- Generates synthetic FHIR patient bundles using Synthea format
- Creates files at random intervals (10-60 seconds)
- Stores them in `data/generated_data/fhir/`
- Simulates real-world continuous data arrival

**Output:**
```
🏥 Healthcare Data Generator Started
📁 Output Directory: D:\healthcare-data-pipeline\data\generated_data\fhir
⏰ Generating patient data at random intervals (10-60 seconds)

✅ Generated: patient_20251107_143045_a1b2c3d4.json | Patient: John Smith
⏳ Next generation in 45 seconds...
```

---

## 📊 Usage

### Monitoring the Pipeline

1. **Airflow UI** (http://localhost:8080)
   - View DAG runs and task status
   - Check execution logs
   - Monitor task duration

2. **Data Generator Terminal**
   - Watch new files being created
   - See patient names and timestamps

3. **Output Folders**
   - `data/processed_data/patients/` - Patient demographics CSV
   - `data/processed_data/conditions/` - Medical conditions CSV
   - `data/processed_data/encounters/` - Clinical encounters CSV

4. **Archive Folder**
   - `data/archive/YYYY/MM/DD/HH-MM-SS/` - Archived raw FHIR bundles

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

## 🔄 Pipeline Details

### Task Flow

```
check_for_files (ShortCircuit)
    ↓
cleanfolder (Filter non-patient bundles)
    ↓
patients_job (Extract patient demographics)
    ↓
conditions_job (Extract medical conditions)
    ↓
encounters_job (Extract clinical encounters)
    ↓
cleanup (Archive and delete processed files)
```

### Task Descriptions

1. **check_for_files**: Detects new FHIR files and atomically moves them to processing folder
2. **cleanfolder**: Filters out non-patient FHIR bundles (practitioner info, hospital info)
3. **patients_job**: Extracts patient ID, name, gender, birthdate using PySpark
4. **conditions_job**: Extracts medical conditions with patient references
5. **encounters_job**: Extracts clinical encounters with timestamps
6. **cleanup**: Archives processed files for audit trail, then deletes from processing folder

### Race Condition Prevention

The pipeline uses a **staging/processing folder pattern**:

```
Incoming: data/generated_data/fhir/
    ↓ (atomic move at pipeline start)
Processing: data/generated_data/processing/
    ↓ (pipeline reads from here)
Archive: data/archive/YYYY/MM/DD/HH-MM-SS/
```

**Benefits:**
- New data arriving during processing doesn't interfere
- Each pipeline run processes a complete, consistent batch
- No file corruption or partial reads

---

## 📈 Data Flow

### Input: FHIR Bundle (JSON)

```json
{
  "resourceType": "Bundle",
  "type": "collection",
  "entry": [
    {
      "resource": {
        "resourceType": "Patient",
        "id": "ac36018e-ffd8-f58b-64dc-b9f06951df55",
        "name": [{"given": ["John"], "family": "Smith"}],
        "gender": "male",
        "birthDate": "1942-04-27"
      }
    },
    {
      "resource": {
        "resourceType": "Condition",
        "subject": {"reference": "Patient/ac36018e..."},
        "code": {"text": "Hypertension"},
        "recordedDate": "2024-03-15T10:30:00Z"
      }
    }
  ]
}
```

### Output: CSV Tables

**patients.csv**
```csv
id,gender,birthDate,full_name
ac36018e-ffd8-f58b-64dc-b9f06951df55,male,1942-04-27,John Smith
```

**conditions.csv**
```csv
id,patient_ref,condition_name,recorded_date
uuid-1,Patient/ac36018e...,Hypertension,2024-03-15T10:30:00Z
```

**encounters.csv**
```csv
id,patient_ref,class_code,start_date,end_date
uuid-2,Patient/ac36018e...,ambulatory,2024-03-15T09:00:00Z,2024-03-15T10:30:00Z
```

---

## 📊 Results

### Performance Metrics

**Test Dataset:** 16 patient bundles (142 MB)

| Metric | Value |
|--------|-------|
| Total Pipeline Duration | ~90 seconds |
| Data Compression Ratio | 284:1 (142 MB → 500 KB) |
| Patients Extracted | 16 records |
| Conditions Extracted | 50+ records |
| Encounters Extracted | 100+ records |

**Task Execution Times:**
- check_for_files: 1.3s
- cleanfolder: 5.4s
- patients_job: 30.1s
- conditions_job: 24.8s
- encounters_job: 27.6s
- cleanup: 0.9s

### Scalability

- **Current throughput**: 1.6 MB/s
- **With 4 workers**: ~6.4 MB/s (linear scaling)
- **With 10 workers**: ~16 MB/s (linear scaling)

---

## 🔮 Future Enhancements

1. **Data Quality Validation**
   - Integrate Great Expectations framework
   - Validate required fields, data types, business rules
   - Fail pipeline on critical data quality issues

2. **Real-time Streaming**
   - Replace batch processing with Kafka + Spark Streaming
   - Reduce latency from 5 minutes to seconds
   - Enable real-time clinical dashboards

3. **Monitoring Dashboard**
   - Add Prometheus metrics and Grafana dashboards
   - Track pipeline duration, record counts, error rates
   - Slack/PagerDuty alerts for failures

4. **Cloud Deployment**
   - Deploy to AWS EMR, GCP Dataproc, or Azure HDInsight
   - Use S3/GCS for data lake storage
   - Implement auto-scaling for cost optimization

5. **Incremental Processing**
   - Add watermarking to track last processed timestamp
   - Process only new/modified records
   - Implement Delta Lake or Hudi for ACID transactions

---

## 👤 Author

**Vedant Hirekar**

- **Course:** Management, Access and Use of Big Data
- **GitHub:** [@vedanthirekar](https://github.com/vedanthirekar)
- **Project:** [healthcare-data-pipeline](https://github.com/vedanthirekar/healthcare-data-pipeline)

---

## 📄 License

This project is created for educational purposes as part of a Big Data course.

---

## 🙏 Acknowledgments

- **Apache Airflow** - Workflow orchestration framework
- **Apache Spark** - Distributed data processing engine
- **Synthea** - Synthetic patient data generator
- **HL7 FHIR** - Healthcare interoperability standard
- **Docker** - Containerization platform

---

## 📚 Additional Resources

- [Project Report (PDF)](Healthcare%20Data%20Pipeline%20Project%20Report.pdf) - Detailed academic report
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [HL7 FHIR Specification](https://hl7.org/fhir/)
- [Synthea Documentation](https://synthetichealth.github.io/synthea/)

---

**⭐ If you find this project useful, please consider giving it a star on GitHub!**
