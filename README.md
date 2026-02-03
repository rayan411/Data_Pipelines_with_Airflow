
---

# **📘 Data Pipelines with Airflow – Sparkify Project**

## **📌 Project Overview**
This project builds a fully automated and scalable **ETL data pipeline** for the music streaming company **Sparkify** using **Apache Airflow**.  
The pipeline extracts raw JSON logs and song metadata from **AWS S3**, stages them in **AWS Redshift**, and transforms them into a **Star Schema** optimized for analytics.

This project demonstrates strong data engineering capabilities, including:

- Designing and orchestrating ETL pipelines with Airflow  
- Building custom Airflow Operators  
- Implementing automated data quality checks  
- Managing dependencies and scheduling  
- Integrating Airflow with AWS services (S3, Redshift, IAM)

---

## **📂 Project Structure**

```text
Sparkify_Data_Pipeline
│
├── dags
│   ├── final_project.py                 # Main DAG orchestrating the pipeline
│   └── udacity
│       └── common
│           └── final_project_sql_statements.py   # SQL helper queries
│
├── plugins
│   └── final_project_operators
│       ├── __init__.py
│       ├── stage_redshift.py            # Loads data from S3 → Redshift staging tables
│       ├── load_fact.py                 # Loads data into the Fact table
│       ├── load_dimension.py            # Loads data into Dimension tables
│       └── data_quality.py              # Runs data quality checks
│
├── create_tables.sql                    # SQL script to initialize Redshift tables
└── README.md                            # Project documentation
```

---

## **⚙️ Architecture & Data Flow**

### **1. Staging Layer**
Raw JSON files are copied from S3 (`log_data`, `song_data`) into Redshift staging tables:

- `staging_events`
- `staging_songs`

This is done using the **StageToRedshiftOperator**, which wraps the Redshift `COPY` command.

---

### **2. Fact Table**
The **LoadFactOperator** transforms and inserts data into:

- `songplays` (Fact table)

This table captures user activity and is the core analytical dataset.

---

### **3. Dimension Tables**
The **LoadDimensionOperator** loads data into:

- `users`
- `songs`
- `artists`
- `time`

It supports a **truncate-insert** strategy to ensure idempotent runs.

---

### **4. Data Quality Layer**
The **DataQualityOperator** validates:

- Row counts  
- Primary key integrity  
- Null checks  

If any check fails, the operator raises an exception and stops the pipeline.

---

## **🛠️ Custom Operators Overview**

### **1. StageToRedshiftOperator**
- Loads JSON files from S3 → Redshift  
- Supports templated S3 keys  
- Handles JSONPaths configuration  
- Uses Redshift `COPY` for fast ingestion  

---

### **2. LoadFactOperator**
- Executes an `INSERT` statement into the fact table  
- Appends data to preserve historical records  

---

### **3. LoadDimensionOperator**
- Loads dimension tables  
- Supports **truncate-insert** mode  
- Ensures clean and consistent dimension data  

---

### **4. DataQualityOperator**
- Runs SQL checks against Redshift  
- Compares results with expected values  
- Raises an exception on failure  

---

## **🚀 How to Run the Pipeline**

### **Prerequisites**
- Apache Airflow installed  
- AWS Redshift cluster running  
- IAM user with S3 + Redshift permissions  

---

### **1. Configure Airflow Connections**
In Airflow UI → **Admin → Connections**:

| Connection ID      | Type                | Description |
|--------------------|---------------------|-------------|
| `aws_credentials`  | Amazon Web Services | AWS Access Keys |
| `redshift`         | Postgres            | Redshift cluster connection |

---

### **2. Create Redshift Tables**
Run the SQL script:

```
create_tables.sql
```

in **Redshift Query Editor v2**.

---

### **3. Run the DAG**
1. Open Airflow UI  
2. Enable the DAG: `final_project`  
3. Trigger the DAG  
4. Monitor progress in **Graph View**  
5. Successful tasks appear in **green**  

---

## **📅 Schedule**
The DAG is configured to run **hourly**, ensuring continuous ingestion and transformation of new data.

---

## **🎉 Final Notes**
This project showcases a production-grade ETL pipeline using Airflow and AWS.  
It demonstrates modular design, clean operator abstraction, and automated data validation—core skills for any Data Engineer.
