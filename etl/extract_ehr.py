# Example: Extract EHR Data (Spark)
from pyspark.sql import SparkSession
import sys

def extract_ehr_data(source_path):
    spark = SparkSession.builder.appName("EHRExtract").getOrCreate()
    df = spark.read.json(source_path)
    return df

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit etl/extract_ehr.py <input_json>")
        sys.exit(1)
    input_path = sys.argv[1]
    df = extract_ehr_data(input_path)
    print("Loaded DataFrame:")
    df.show()
