CREATE DATABASE IF NOT EXISTS student_processed_db;

CREATE EXTERNAL TABLE IF NOT EXISTS student_processed_db.student_results (
  roll_no STRING,
  student_name STRING,
  email STRING,
  city STRING,
  subject_name STRING,
  marks_obtained INT,
  exam_date DATE,
  pass_fail STRING
)
STORED AS PARQUET
LOCATION 's3://student-performance-analytics-bucket/processed/run_20260119_rebuild_01/';

SELECT * FROM student_processed_db.student_results LIMIT 20;
