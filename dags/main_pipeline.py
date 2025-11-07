"""Medical data pipeline DAG for processing FHIR patient bundles."""
import sys
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# Add the scripts directory to Python path
scripts_path = Path(__file__).parent.parent / 'scripts'
sys.path.insert(0, str(scripts_path))

# Import your scripts
from cleanfolder import main as cleanfolder_main
from patients_job import main as patients_main
from conditions_job import main as conditions_main
from encounters_job import main as encounters_main
from cleanup import main as cleanup_main

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'healthcare_data_pipeline',
    default_args=default_args,
    description='Pipeline to process medical data when files are detected',
    schedule='*/5 * * * *',  # Check every 5 minutes (Airflow 3.x uses 'schedule')
    catchup=False,
    tags=['medical', 'etl', 'file-triggered'],
)

def check_for_files():
    """Check if there are any JSON files in the generated_data/fhir folder."""
    base_dir = Path('/opt/airflow')
    fhir_dir = base_dir / 'data' / 'generated_data' / 'fhir'
    
    if not fhir_dir.exists():
        fhir_dir.mkdir(parents=True, exist_ok=True)
        raise Exception(f"No files found in {fhir_dir}")
    
    json_files = list(fhir_dir.glob('*.json'))
    if not json_files:
        raise Exception(f"No JSON files found in {fhir_dir}")
    
    print(f"Found {len(json_files)} JSON files to process")
    return True

# Check for new files
task_check_files = PythonOperator(
    task_id='check_for_files',
    python_callable=check_for_files,
    dag=dag,
)

# Define tasks
task_cleanfolder = PythonOperator(
    task_id='cleanfolder',
    python_callable=cleanfolder_main,
    dag=dag,
)

task_patients = PythonOperator(
    task_id='patients_job',
    python_callable=patients_main,
    dag=dag,
)

task_conditions = PythonOperator(
    task_id='conditions_job',
    python_callable=conditions_main,
    dag=dag,
)

task_encounters = PythonOperator(
    task_id='encounters_job',
    python_callable=encounters_main,
    dag=dag,
)

task_cleanup = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_main,
    dag=dag,
)

# Set task dependencies (sequential execution)
(task_check_files >> task_cleanfolder >> task_patients >> 
 task_conditions >> task_encounters >> task_cleanup)
