"""Medical data pipeline DAG for processing FHIR patient bundles."""
import sys
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
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
    """Check if there are any JSON files and move them to processing folder.
    
    This prevents race conditions where new files arrive during processing.
    Returns False to skip pipeline if no files, True to continue processing.
    """
    import shutil
    
    base_dir = Path('/opt/airflow')
    fhir_dir = base_dir / 'data' / 'generated_data' / 'fhir'
    processing_dir = base_dir / 'data' / 'generated_data' / 'processing'
    
    # Create directories if they don't exist
    fhir_dir.mkdir(parents=True, exist_ok=True)
    processing_dir.mkdir(parents=True, exist_ok=True)
    
    # Check for files in the incoming folder
    json_files = list(fhir_dir.glob('*.json'))
    if not json_files:
        print(f"⚠️ No JSON files found in {fhir_dir} - skipping pipeline")
        return False
    
    # Move files to processing folder atomically
    print(f"📦 Moving {len(json_files)} files to processing folder...")
    for file in json_files:
        try:
            dest = processing_dir / file.name
            shutil.move(str(file), str(dest))
            print(f"  ✓ Moved: {file.name}")
        except Exception as e:
            print(f"  ✗ Failed to move {file.name}: {e}")
    
    # Verify files were moved
    processing_files = list(processing_dir.glob('*.json'))
    print(f"✅ Ready to process {len(processing_files)} files from processing folder")
    return True

# Check for new files - skips downstream tasks if no files found
task_check_files = ShortCircuitOperator(
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
