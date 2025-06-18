# Databricks notebook source
base_path = "/FileStore/tables"
batch_date = "20250601"
#path source for ingestion
students_path = f"{base_path}/students.csv"
instructors_path = f"{base_path}/instructors.csv"
courses_path = f"{base_path}/courses_large.csv"
#daily data ingestion
engagement_path = f"{base_path}/student_engagement_{batch_date}_large.csv"
quiz_path = f"{base_path}/quiz_attempts_{batch_date}_large.csv"
video_path = f"{base_path}/video_watch_logs_{batch_date}_large.json"


# COMMAND ----------

#schema definition for enforcement
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
student_schema = StructType([
    StructField("student_id",IntegerType(),False),
    StructField("student_name",StringType(),True),
    StructField("email",StringType(),True),
    StructField("enrollment_date",StringType())
])

instructors_schema = StructType([
    StructField("instructor_id",IntegerType(),False),
    StructField("instructor_name",StringType(),True),
    StructField("email",StringType(),True),
    StructField("mobile_number",IntegerType(),True)
])

courses_schema = StructType([
    StructField("course_id",IntegerType(),False),
    StructField("course_name",StringType(),True),
    StructField("instructor_id",IntegerType(),True)
])

engagement_schema = StructType([
    StructField("student_id",IntegerType(),False),
    StructField("course_id",IntegerType(),False),
    StructField("timestamp",StringType(),True),
    StructField("event_type",StringType(),True)
])

quiz_schema = StructType([
    StructField("student_id",IntegerType(),False),
    StructField("quiz_id",IntegerType(),False),
    StructField("quiz_score",IntegerType(),True),
    StructField("attempt_time",StringType(),True)
])

video_schema = StructType([
    StructField("student_id",IntegerType(),False),
    StructField("video_id",IntegerType(),False),
    StructField("watch_time_seconds",IntegerType(),True),
    StructField("watched_at",StringType(),True)
])

# COMMAND ----------

#read data using above path and schema
def read_files(f,path,schema, description):
    try:
        reader = spark.read.format(f).option("header","true")\
            .option("mode","PERMISSIVE")\
            .schema(schema)
        # allows reading json
        if f.lower()=='json':
            reader = reader.option("multiline","true")
        df=reader.load(path)
        print(f"read {description}:{df.count()} records, data format is :{f}")
        return df
    except Exception as e:
        print(f'Error reading {description}, error:{str(e)}')


# COMMAND ----------

from pyspark.sql.functions import col
student_df = read_files('csv',students_path,student_schema,"student df")
instructors_df = read_files('csv',instructors_path,instructors_schema,"instructor_df")
courses_df = read_files('csv',courses_path,courses_schema,'courses_df')
video_df = read_files('json',video_path,video_schema,'video_df')
engagement_df = read_files('csv',engagement_path,engagement_schema,"engagement_df")
quiz_df = read_files('csv',quiz_path,quiz_schema,"quiz_df")

# COMMAND ----------

from pyspark.sql import DataFrame
import sys
#validate not nulls
def validate_not_null(df:DataFrame,cols:list[str],df_name:str):
    for c in cols:
        null_count = df.filter(col(c).isNull()).count()
        if null_count>0:
            print(f"Quality check failed : {null_count} nulls found in col {c} of {df_name}")
        else:
            print(f"{c} checked in {df_name} and passed null check.")

# COMMAND ----------

validate_not_null(student_df,['student_id'],"student_df")
validate_not_null(instructors_df,['instructor_id'],'instructor_df')
validate_not_null(courses_df,['course_id'],'courses_df')
validate_not_null(video_df,['video_id','student_id'],"video_df")
validate_not_null(engagement_df,['student_id','course_id','timestamp'],'engagement_df')
validate_not_null(quiz_df,['quiz_id','attempt_time'],"quiz_df")

# COMMAND ----------

#quality check failed in students_df so bad records in students quarantined.
bad_records_df = student_df.filter(col('student_id').isNull())
#bad record is exported to bad_records directory to evaluate
# bad_records_df.write.mode('overwrite').parquet('path')
good_records_students = student_df.filter(col('student_id').isNotNull())
student_df = good_records_students

# COMMAND ----------

#enrichment
# date_key column in added into fact_tables for idempotation in redshift staging table , to filter and remove records having same date during rerun of pipeline.
from pyspark.sql.functions import to_timestamp,to_date,sha2, concat_ws, lit,when
def transform_engagement(df):
    return df.withColumn('timestamp',to_timestamp('timestamp'))\
        .dropDuplicates(['student_id','course_id','timestamp','event_type'])\
            .withColumn("date_key",to_date('timestamp'))\
                .withColumn('event_score',
                            when(col('event_type') =='login',1)
                            .when(col('event_type') == 'discussion',2)
                            .when(col('event_type') == 'assignment_submit',3)
                            .otherwise(0)
                            )\
                                .withColumn('engagement_id',sha2(concat_ws("",col('student_id'),col("timestamp")),256))

def transform_quiz(df):
    return df.withColumn('attempt_time',to_timestamp('attempt_time'))\
        .withColumn('date_key',to_date('attempt_time')) \
        .dropDuplicates(['student_id','quiz_id','quiz_score','attempt_time'])\
                .filter((col("quiz_score") >= 0) & (col('quiz_score')<=100)) # quiz score validation , should not be less than 0 or more than 100

def transform_student(df):
     return (
        df.withColumn('enrollment_date', to_date('enrollment_date'))\
          .dropDuplicates(['student_id', 'student_name', 'email', 'enrollment_date'])\
          .withColumn("student_sk", sha2(concat_ws("", col("student_id")), 256)))    

def transform_instructor(df):
    return df.withColumn('instructor_sk',sha2(concat_ws("",col('instructor_id')),256))\
        .dropDuplicates(['instructor_id','instructor_name','email','mobile_number'])

def transform_courses(df):
    return df.dropDuplicates(['course_id','course_name','instructor_id'])\
        .withColumn('course_sk',sha2(concat_ws("",col('course_id')),256))

def transform_video(df):
    return df.withColumn('watched_at',to_timestamp('watched_at'))\
        .withColumn("date_key",to_date("watched_at")) \
        .dropDuplicates(['student_id','video_id','watch_time_seconds','watched_at'])\
            .withColumn('video_watch_sk',sha2(concat_ws("",col('video_id')),256))

# COMMAND ----------


#Transform datasets and get fact and dim tables
fact_student_engagement = transform_engagement(engagement_df)
fact_quiz = transform_quiz(quiz_df)
fact_video_interaction = transform_video(video_df)
dim_student = transform_student(student_df)
dim_instructor = transform_instructor(instructors_df)
dim_course = transform_courses(courses_df)

# COMMAND ----------

#fact and dimension tables created.
fact_student_engagement.printSchema()
fact_quiz.printSchema()
fact_video_interaction.printSchema()
dim_student.printSchema()
dim_instructor.printSchema()
dim_course.printSchema()

# COMMAND ----------

# now writing into staging table in redshift.Staging table is used to implement upsert logic and idempotency. Overwrite mode is used during writing because staging table is used.
from pyspark.sql import DataFrame
def write_to_redshift_staging_table(df:DataFrame,jdbc_url:str,user:str,password:str,table_name:str):
    try:
        df.write.format('jdbc')\
        .option('url',jdbc_url)\
        .option('user',user) \
        .option('password',password) \
        .option('dbtable',f"staging_{table_name}") \
        .mode("overwrite") \
        .save()
    except Exception as e:
        print(f"Failed to write staging table staging_{table_name} to Redshift : {e}")
    

# COMMAND ----------

# === 9. Upsert/Merge logic (to be done inside Redshift) ===
# IMPORTANT:
# - After loading staging tables, execute MERGE SQL in Redshift:
  
#   -- For dimension tables (upsert logic):
#   MERGE INTO dim_student d
#   USING staging_dim_student s
#   ON d.student_sk = s.student_sk
#   WHEN MATCHED THEN UPDATE SET ...
#   WHEN NOT MATCHED THEN INSERT ...;

#   -- Similar for dim_course, dim_quiz, dim_video

#   -- For fact tables (idempotent insert):
#   DELETE FROM fact_student_engagement WHERE date_key = '{batch_date}';
#   INSERT INTO fact_student_engagement SELECT * FROM staging_fact_student_engagement;

#   -- Same pattern for fact_quiz_scores, fact_video_interactions, fact_daily_summary.

# This ensures:
# - No duplicate records for the batch
# - Safe reprocessing and idempotency


# COMMAND ----------

