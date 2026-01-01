# Example: Extract EHR Data (Spark)
from pyspark.sql import SparkSession

def extract_ehr_data(source_path):
    spark = SparkSession.builder.appName("EHRExtract").getOrCreate()
    df = spark.read.json(source_path)
    return df
