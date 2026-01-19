from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, upper, when, to_date

spark = SparkSession.builder.getOrCreate()

students_raw = spark.read.table("student_raw_db.students")
marks_raw = spark.read.table("student_raw_db.marks")
subjects_raw = spark.read.table("student_raw_db.subjects")

students = (
    students_raw
    .withColumnRenamed("col0", "rollno")
    .withColumnRenamed("col1", "studentname")
    .withColumnRenamed("col2", "email")
    .withColumnRenamed("col3", "city")
    .filter(col("rollno") != "rollno")   # remove header row
    .withColumn("rollno", upper(col("rollno")))
    .withColumn("studentname", initcap(col("studentname")))
    .withColumn("city", initcap(col("city")))
)

marks = (
    marks_raw
    .filter(col("rollno") != "rollno")   # remove header row
    .withColumn("examdate", to_date(col("examdate"), "yyyy/MM/dd"))
)


subjects = (
    subjects_raw
    .withColumnRenamed("col0", "subjectid")
    .withColumnRenamed("col1", "subjectname")
    .filter(col("subjectid") != "subjectid")  # remove header row
    .withColumn("subjectname", initcap(col("subjectname")))
)


student_marks = students.join(marks, on="rollno", how="left")
final_df = student_marks.join(subjects, on="subjectid", how="left")

final_df = final_df.withColumn(
    "pass_fail",
    when(col("marksobtained") >= 40, "PASS").otherwise("FAIL")
)

final_df = final_df.select(
    "rollno",
    "studentname",
    "email",
    "city",
    "subjectname",
    "marksobtained",
    "examdate",
    "pass_fail"
)

final_df.write.mode("overwrite").parquet(
    "s3://student-performance-analytics-bucket/processed/student_results_v2/"
)
