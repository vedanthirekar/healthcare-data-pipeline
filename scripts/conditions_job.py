"""Extract conditions data from FHIR bundles using PySpark."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    """Main function to be called by Airflow"""
    # Initialize Spark
    spark = SparkSession.builder.appName("ConditionsTable").getOrCreate()
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Read from processing folder to avoid race conditions
    input_path = os.path.join(BASE_DIR, "data", "generated_data", "processing", "*.json")
    output_path = os.path.join(BASE_DIR, "data", "processed_data", "conditions")
    
    # Read all patient bundle JSON files
    df = spark.read.option("multiline", "true").json(input_path)
    
    # Flatten nested entries
    df_flat = df.selectExpr("inline(entry)").select("resource.*")
    
    # Extract only Condition resources
    conditions_df = df_flat.filter(col("resourceType") == "Condition").select(
        "id",
        col("subject.reference").alias("patient_ref"),
        col("code.text").alias("condition_name"),
        col("recordedDate").alias("recorded_date"),
    )
    
    # Show a quick preview
    conditions_df.show(5, truncate=False)
    
    # Write to CSV (append mode)
    conditions_df.write.mode("append").option("header", True).csv(output_path)
    
    print(f"✅ Conditions data written to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()