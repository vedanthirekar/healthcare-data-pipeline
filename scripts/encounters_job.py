"""Extract encounters data from FHIR bundles using PySpark."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    """Main function to be called by Airflow"""
    # Initialize Spark
    spark = SparkSession.builder.appName("EncountersTable").getOrCreate()
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    input_path = os.path.join(BASE_DIR, "data", "generated_data", "fhir", "*.json")
    output_path = os.path.join(BASE_DIR, "data", "processed_data", "encounters")
    
    # Read and flatten JSON files
    df = spark.read.option("multiline", "true").json(input_path)
    df_flat = df.selectExpr("inline(entry)").select("resource.*")
    
    # Filter for Encounter resources
    encounters_df = df_flat.filter(col("resourceType") == "Encounter")
    
    # Select relevant fields
    encounters_simple = encounters_df.select(
        "id",
        col("subject.reference").alias("patient_ref"),
        col("class.code").alias("class_code"),
        col("period.start").alias("start_date"),
        col("period.end").alias("end_date"),
    )
    
    # Preview sample records
    encounters_simple.show(5, truncate=False)
    
    # Write output in append mode
    encounters_simple.write.mode("append").option("header", True).csv(output_path)
    
    print(f"✅ Encounters data written to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
