##import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp

# build a SparkSession
spark = SparkSession.builder.appName(
    "adzuna-job-transformer"
).getOrCreate()

# define the input path for the raw job postings stored in Cloud Storage
input_path = (
    "gs://adzuna-etl-pipeline-1068906544488/"
    "raw/to_process/*.json"
)

# Read the raw job postings from Cloud Storage into a DataFrame, specifying that the JSON files may contain multiple lines
df = (
    spark.read
    .option("multiline", "true")
    .json(input_path)
)

# select the extracted_at timestamp and explode the items array to create a new row for each job posting
jobs_df = df.select(
    col("extracted_at"),
    explode("items").alias("job"),
)

# transform the DataFrame to select and rename the required fields, and cast them to appropriate data types
clean_df = jobs_df.select(
    col("job.id").alias("job_id"),
    col("job.title").alias("title"),
    col("job.company.display_name").alias("company_name"),
    col("job.location.display_name").alias("location_name"),
    col("job.location.area").alias("location_area"),
    col("job.category.label").alias("category"),
    col("job.category.tag").alias("category_tag"),
    col("job.salary_min").cast("double").alias("salary_min"),
    col("job.salary_max").cast("double").alias("salary_max"),
    col("job.salary_is_predicted").cast("boolean").alias(
        "salary_is_predicted"
    ),
    col("job.contract_type").alias("contract_type"),
    col("job.contract_time").alias("contract_time"),
    col("job.latitude").cast("double").alias("latitude"),
    col("job.longitude").cast("double").alias("longitude"),
    to_timestamp(col("job.created")).alias("created_at"),
    col("job.description").alias("description"),
    col("job.redirect_url").alias("redirect_url"),
    to_timestamp(col("extracted_at")).alias("extracted_at"),
)

# drop duplicates based on job_id to ensure unique job postings
clean_df = clean_df.dropDuplicates(["job_id"])

# define the output path for the cleaned job postings in Cloud Storage
output_path = (
    "gs://adzuna-etl-pipeline-1068906544488/"
    "transformed/jobs/"
)

# write the cleaned DataFrame to Cloud Storage in Parquet format, overwriting any existing files at the output path
(
    clean_df.write
    .mode("overwrite")
    .parquet(output_path)
)

spark.stop()