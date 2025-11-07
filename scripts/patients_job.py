"""Extract patient data from FHIR bundles using PySpark."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws


def main():
    """Main function to be called by Airflow"""
    # Initialize Spark
    spark = SparkSession.builder.appName("PatientTable").getOrCreate()
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Read from processing folder to avoid race conditions
    input_path = os.path.join(BASE_DIR, "data", "generated_data", "processing", "*.json")
    output_path = os.path.join(BASE_DIR, "data", "processed_data", "patients")
    
    # Read and flatten
    df = spark.read.option("multiline", "true").json(input_path)
    df_flat = df.selectExpr("inline(entry)").select("resource.*")
    
    # Extract Patient data
    # Filter for Patient resources first
    patients_raw = df_flat.filter(col("resourceType") == "Patient")
    
    # Handle both array and string types for name field
    from pyspark.sql.functions import when, size, expr
    
    patients_df = patients_raw.select(
        "id",
        "gender",
        "birthDate",
        # Handle name field - check if it's an array or string
        when(
            col("name").isNotNull(),
            concat_ws(
                " ",
                expr("name[0].given[0]"),
                expr("name[0].family")
            )
        ).otherwise("Unknown").alias("full_name")
    )
    
    patients_df.show(5, truncate=False)
    
    # Write the patients_df
    patients_df.write.mode("append").option("header", True).csv(output_path)
    
    print(f"✅ Patients data written to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
