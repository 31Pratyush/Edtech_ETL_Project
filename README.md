
# 🎓 EdTech Data Pipeline Project with PySpark & Redshift

This project implements a **real-world ETL pipeline** for an EdTech platform using **PySpark** and **Amazon Redshift**. It processes both static and dynamic datasets, models them into a **star schema**, and supports analytical queries for student engagement, quiz performance, and video activity tracking.

---

## 🎯 Project Objective

The goal of this project is to simulate a **daily batch data pipeline** for an EdTech company that ingests, processes, and loads data into a star schema for analytical consumption.

### Key Objectives:

- 🔁 **Daily ingestion** of time-partitioned CSV/JSON data with `batch_date` embedded in filenames.
- 📅 Ensure **idempotent processing** using `date_key` columns for safe re-runs.
- 🧹 Apply data quality checks: null validation, value filters, deduplication.
- 🔐 Use **surrogate keys (SHA2)** for dimension modeling and referential joins.
- 🗃️ Load transformed data into **Redshift staging tables** with `overwrite` mode.
- 🔄 Perform **MERGE for dimensions** and **delete-insert for fact tables** using Redshift SQL.
- ⭐ Create a **star schema** optimized for BI/reporting use cases.

---

## 📁 Project Structure

```
├── EdTech_pipeline.py  # Main PySpark ETL pipeline
├── README.md  # Project documentation
├── HLD
└── sample data
```

---

## 🏗️ Architecture Overview

**Tech Stack:**
- 🧪 PySpark (Databricks/Spark cluster)
- ☁️ AWS S3 (simulated with /FileStore/tables)
- 🛢️ Amazon Redshift (DWH)
- 📊 Star Schema for BI/Analytics

**Pipeline Flow:**

```
S3 (CSV, JSON) 
    ↓
PySpark (Read → Validate → Transform)
    ↓
Redshift Staging Tables (overwrite mode)
    ↓
Redshift Merge Logic (upsert dim / delete-insert fact)
    ↓
Redshift Final Star Schema (fact + dimensions)
```

---

## 📚 Datasets Processed

- `students.csv` – student profiles (static)
- `instructors.csv` – instructor master data (static)
- `courses_large.csv` – course catalog (static)
- `student_engagement_<batch_date>.csv` – daily engagement logins, discussions, assignments
- `quiz_attempts_<batch_date>.csv` – daily quiz scores
- `video_watch_logs_<batch_date>.json` – daily video watch activity

---

## 🛠️ Features Implemented

✅ Schema enforcement with PySpark  
✅ Null check validations & data quality logic  
✅ Quarantine of bad records  
✅ Business logic: quiz score filters, event scoring, deduplication  
✅ Surrogate key generation using `sha2()`  
✅ `date_key` for idempotent batch processing  
✅ Redshift `staging` table writes with `overwrite` mode  
✅ Redshift `MERGE` & `DELETE-INSERT` logic for upserts  
✅ Star Schema with `dim_student`, `dim_course`, `fact_quiz`, `fact_student_engagement`, etc.

---

## 🗃️ Redshift Tables

### Dimension Tables
- `dim_student`
- `dim_instructor`
- `dim_course`

### Fact Tables
- `fact_student_engagement`
- `fact_quiz`
- `fact_video_interaction`

### Staging Tables
- `staging_dim_*`
- `staging_fact_*`

---

## 🧪 Sample BI Queries

```sql
-- Top scoring students
SELECT student_name, AVG(quiz_score) 
FROM fact_quiz 
JOIN dim_student USING(student_id)
GROUP BY student_name;

-- Course login trends
SELECT course_name, date_key, COUNT(*) 
FROM fact_student_engagement 
JOIN dim_course USING(course_id)
WHERE event_type = 'login'
GROUP BY course_name, date_key;
```

---

## 🚀 Future Enhancements

- Integrate **Airflow orchestration**
- Automate Redshift MERGE logic via scripts
- Add **unit testing** for data quality
- Store bad records in S3/Delta Lake for audit

---

## 📂 How to Run

1. Load datasets into `/FileStore/tables/` (Databricks) or replace with S3 paths.
2. Set `batch_date` in the script to match incoming files.
3. Run `EdTech.py` notebook/script.
4. Ensure Redshift credentials are configured for staging writes.
5. Use the generated MERGE SQL to populate final schema.

---

## 👨‍💻 Author

**Pratyush Sahay**
pratyush(dot)310899@gmail(dot)com  
Data Engineer | 3 Years Experience  
LinkedIn: [www.linkedin.com/in/pratyush-sahay]

---
