# AMEX Lumi Case Study — Major Capstone

> **End-to-end data ingestion pipeline with Spring Boot API, Apache Airflow orchestration, Apache Beam processing, and PostgreSQL storage.**
---

## Table of Contents

1. [Overview](#-overview)
2. [Architecture](#-architecture)
3. [Tech Stack](#-tech-stack)
4. [Project Structure](#-project-structure)
5. [Quick Start](#-quick-start)
6. [Pipeline Flow](#-pipeline-flow)
7. [Sample Data](#-sample-data)
8. [Author](#-author)

---

## Overview

This project implements a **data ingestion pipeline** as part of the AMEX Lumi Case Study . The system ingests employee data from multiple file formats, validates and encrypts sensitive information, and stores it in a PostgreSQL data warehouse with full audit tracking.

### Key Features

- **Multi-format support** — CSV, FixedWidth, JSON, XML
- **AES-256-GCM encryption** — Authenticated encryption for sensitive fields
- **4-level validation** — Required, Type, Length, Pattern
- **Error handling** — Failed rows captured to error files
- **End-to-end tracking** — ingestion_count table with RUNNING/PASSED/FAILED status
- **Orchestrated** — Apache Airflow DAG with 5 tasks
- **Containerized** — Docker Compose for full stack
- **REST API** — Spring Boot trigger endpoint

---

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Postman   │─────▶│ Spring Boot  │─────▶│   Airflow    │─────▶│  Apache Beam │─────▶│  PostgreSQL  │
│   / curl    │ POST │   API 8081   │ HTTP │     DAG      │ Bash │  DirectRunner│ JDBC │   Warehouse  │
└─────────────┘      └──────────────┘      └──────────────┘      └──────────────┘      └──────────────┘
                            │                     │                       │                      │
                            │                     │                       │                      ▼
                            │                     │                       │              ┌──────────────┐
                            │                     │                       └─────────────▶│ Error Files  │
                            │                     ▼                                      │  (txt files) │
                            │            ┌──────────────┐                              └──────────────┘
                            │            │  ingestion_  │
                            └───────────▶│    count     │
                                          │   (status)   │
                                          └──────────────┘
```

### Flow Description

1. **User** triggers ingestion via POST request to Spring Boot API (port 8081)
2. **Spring Boot** receives request, validates it, calls Airflow REST API
3. **Airflow** triggers the `sor_ingestion_dag` DAG
4. **DAG** runs 5 tasks: validate input → run Beam JAR → count records → update status → notify
5. **Apache Beam** reads file, parses by format, validates, encrypts, writes to DB or error file
6. **PostgreSQL** stores encrypted records in `stg_employees` and tracks status in `ingestion_count`

---

##  Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **API Layer** | Spring Boot | 3.x |
| **Orchestration** | Apache Airflow | 2.x |
| **Processing** | Apache Beam | 2.50.0 |
| **Runner** | DirectRunner | (local mode) |
| **Storage** | PostgreSQL | 13+ |
| **Language** | Java | 17 |
| **Build Tool** | Maven | 3.x |
| **Container** | Docker Compose | v2 |
| **Splitter (Phase 1)** | PySpark | 3.x |

---

##  Project Structure

```
case-study-final/
│
├── spring-boot-ingestion/          
│   ├── src/main/java/com/caseStudy/
│   │   ├── Application.java            
│   │   ├── controller/
│   │   │   └── IngestionController.java
│   │   ├── service/
│   │   │   └── AirflowTriggerService.java
│   │   ├── dto/
│   │   │   ├── IngestionRequest.java
│   │   │   └── IngestionResponse.java
│   │   ├── config/
│   │   │   └── HttpClientConfig.java
│   │   └── exception/
│   │       └── GlobalExceptionHandler.java
│   ├── src/main/resources/
│   │   └── application.properties
│   ├── pom.xml
│   └── Dockerfile
│
├── beam-java/                       
│   ├── src/main/java/com/caseStudy/beam/
│   │   ├── IngestionPipeline.java       
│   │   ├── IngestionConfig.java         
│   │   ├── parser/
│   │   │   ├── FileParserFactory.java
│   │   │   ├── CsvLineParser.java
│   │   │   ├── FixedWidthLineParser.java
│   │   │   ├── JsonLineParser.java
│   │   │   └── XmlLineParser.java
│   │   ├── encryption/
│   │   │   └── EncryptionService.java   
│   │   ├── transform/
│   │   │   ├── EncryptSensitiveFields.java
│   │   │   ├── AddMetadataColumns.java
│   │   │   ├── ReplaceNullsWithWhitespace.java
│   │   │   ├── WriteErrorsToTextFile.java
│   │   │   ├── WriteToWarehouse.java
│   │   │   └── LengthValidator.java    
│   │   ├── validation/
│   │   │   └── LengthValidator.java
│   │   └── tools/
│   │       └── DecryptTool.java        
│   ├── pom.xml
│   └── Dockerfile
│
├── dags/                           
│   ├── sor_ingestion_dag.py            
│   └── sor_ingestion_failure_handler.py 
│
├── pyspark-job/                     
│   └── splitter_job.py
│
├── data/
│   ├── inputs/                      
│   │   ├── employees.json
│   │   ├── employees.xml
│   │   ├── employees_fixedwidth.txt
│   │   ├── employees.csv
│   │   ├── employees_schema.json
│   │   └── employees_control.json
│   └── output/
│       └── errors/                  
│
├── sql/                           
│   ├── init-scripts/
│   │   └── 01-init.sql
│   ├── stg_employees.sql
│   └── ingestion_count.sql
│
├── docker-compose.yml                  
├── .gitignore
└── README.md
```

---

## Quick Start

### Prerequisites

- **Docker Desktop** (v4.x+)
- **Java 17** (for Spring Boot)
- **Maven 3.x**
- **Python 3.10+** (for PySpark splitter, optional)

### 1. Clone Repository

```bash
git clone https://github.com/rajwanshbhati-NT/Major_Capstone
cd Major_Capstone
```

### 2. Build All Components

```bash
# Build Spring Boot
cd spring-boot-ingestion
mvn clean package -DskipTests
cd ..

# Build Beam fat JAR
cd beam-java
mvn clean package -DskipTests
cd ..
```

### 3. Start Docker Stack

```bash
docker compose up -d
```

**Containers started:**
- `postgres` (port 5432)
- `spring-api` (port 8081)
- `airflow-webserver` (port 8080)
- `airflow-scheduler`
- `airflow-init` (one-time)

Wait ~30 seconds for all services to be healthy:

```bash
docker compose ps
```

### 4. Trigger Ingestion (JSON Example)

```bash
curl -X POST http://localhost:8081/api/v1/ingestion/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "filePath": "/opt/airflow/data/inputs/employees_large.json",
    "schemaFilePath": "/opt/airflow/data/inputs/employees_large_schema.json",
    "fileFormat": "JSON",
    "targetTable": "stg_employees",
    "controlFilePath": "/opt/airflow/data/inputs/employees_large_control.properties"
}'
```

**Response:**

```json
{
    "status": "SUCCESS",
    "executionId": "26b8ead7-54ab-474e-bac9-5ea0e8278be3",
    "message": "DAG triggered (no split). Expected records: 100",
    "wasSplit": false,
    "partCount": 1,
    "partPaths": [
        "/opt/airflow/data/inputs/employees.xml"
    ],
    "expectedRecordCount": 100,
    "controlFilePath": "/opt/airflow/data/inputs/employees_large_control.properties"
}
```

### 5. Monitor in Airflow UI

Open browser: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

Click on `sor_ingestion_dag` → See your DAG run → Click on Graph view.

### 6. Verify in PostgreSQL

```bash
docker exec -it postgres psql -U airflow -d airflow
```

```sql
SELECT * FROM ingestion_count ORDER BY created_at DESC LIMIT 5;

-- Check inserted records
SELECT id, first_name, last_name, phone_number_encrypted, salary_encrypted
FROM stg_employees;

-- Decrypt a phone number (using CLI tool)
docker exec -it airflow-scheduler java -cp /opt/airflow/beam-java/target/beam-ingestion.jar \
  com.caseStudy.beam.tools.DecryptTool "BASE64_CIPHERTEXT_HERE" phone
```

---

## Pipeline Flow (Apache Beam)

The Beam pipeline consists of **7 stages**:

```
┌──────────┐    ┌────────┐    ┌──────────┐    ┌─────────┐    ┌──────────┐    ┌──────────┐    ┌────────────┐
│   1.     │    │   2.   │    │   3.     │    │   4.    │    │   5.     │    │   6.     │    │   7.       │
│  READ    │───▶│ PARSE  │───▶│ VALIDATE │───▶│ CLEAN   │───▶│ ENCRYPT  │───▶│ META     │───▶│ BRANCH     │
│  file    │    │ format │    │  4-level │    │ nulls   │    │ AES-GCM  │    │ columns  │    │ DB / Error │
└──────────┘    └────────┘    └──────────┘    └─────────┘    └──────────┘    └──────────┘    └────────────┘
                                                                                                       │
                                                                                              ┌────────┴────────┐
                                                                                              ▼                 ▼
                                                                                       ┌──────────┐      ┌──────────┐
                                                                                       │   DB     │      │  ERROR   │
                                                                                       │ Insert   │      │  File    │
                                                                                       └──────────┘      └──────────┘
```

---

### Status Logic

| Condition | Status |
|-----------|--------|
| `actual_record_count == expected_record_count` | `PASSED` |
| `actual_record_count < expected_record_count` | `FAILED` |
| `actual_record_count > expected_record_count` | `FAILED` |
| Pipeline error before completion | `FAILED` |

---

## Author

**Rajwansh Bhati**

