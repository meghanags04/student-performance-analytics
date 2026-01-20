from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, initcap, when, to_date

spark = SparkSession.builder.getOrCreate()

# =========================================================
# 1️⃣ READ RAW TABLES FROM GLUE CATALOG
# =========================================================
students_raw = spark.read.table("student_raw_db.students")
marks_raw = spark.read.table("student_raw_db.marks")
subjects_raw = spark.read.table("student_raw_db.subjects")

# =========================================================
# 2️⃣ CLEAN STUDENTS TABLE (fix col0, header row)
# =========================================================
students = (
    students_raw
    .filter(col("col0") != "rollno")
    .select(
        upper(col("col0")).alias("roll_no"),
        initcap(col("col1")).alias("student_name"),
        col("col2").alias("email"),
        initcap(col("col3")).alias("city")
    )
)

# =========================================================
# 3️⃣ CLEAN MARKS TABLE
# =========================================================
marks = (
    marks_raw
    .filter(col("rollno") != "rollno")
    .select(
        upper(col("rollno")).alias("roll_no"),
        col("subjectid").alias("subject_id"),
        col("marksobtained").alias("marks_obtained"),
        to_date(col("examdate"), "yyyy/MM/dd").alias("exam_date")
    )
)

# =========================================================
# 4️⃣ CLEAN SUBJECTS TABLE
# =========================================================
subjects = (
    subjects_raw
    .filter(col("col0") != "subjectid")
    .select(
        col("col0").alias("subject_id"),
        initcap(col("col1")).alias("subject_name")
    )
)

# =========================================================
# 5️⃣ JOIN TABLES
# =========================================================
student_marks = students.join(marks, on="roll_no", how="inner")

final_df = student_marks.join(subjects, on="subject_id", how="inner")

# =========================================================
# 6️⃣ DERIVED COLUMN
# =========================================================
final_df = final_df.withColumn(
    "pass_fail",
    when(col("marks_obtained") >= 40, "PASS").otherwise("FAIL")
)

# =========================================================
# 7️⃣ SELECT FINAL COLUMNS
# =========================================================
final_df = final_df.select(
    "roll_no",
    "student_name",
    "email",
    "city",
    "subject_name",
    "marks_obtained",
    "exam_date",
    "pass_fail"
)

# =========================================================
# 8️⃣ WRITE OUTPUT TO S3 (CLOSED STRING ✅)
# =========================================================
final_df.write.mode("overwrite").parquet(
    "s3://student-performance-analytics-bucket/processed/run_20260119_rebuild_01/"
)
