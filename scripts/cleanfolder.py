import os
import json
from pathlib import Path

# Dynamically resolve the path so it works no matter where you run it
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "generated_data" / "fhir"

def is_patient_bundle(file_path):
    """Check if the file is a FHIR Patient bundle."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        if data.get("resourceType") != "Bundle" or "entry" not in data:
            return False

        # Safely check first resource
        entries = data.get("entry", [])
        if not entries:
            return False

        first_resource = entries[0].get("resource", {}).get("resourceType")
        return first_resource == "Patient"

    except json.JSONDecodeError:
        print(f"⚠️ Skipping {file_path}: Invalid JSON")
        return False
    except Exception as e:
        print(f"⚠️ Skipping {file_path}: {e}")
        return False


def clean_folder():
    """Remove all non-patient bundle JSON files from the folder."""
    if not DATA_DIR.exists():
        print(f"❌ Data directory not found: {DATA_DIR}")
        return

    print(f"🧹 Cleaning folder: {DATA_DIR}\n")
    for filename in os.listdir(DATA_DIR):
        if not filename.endswith(".json"):
            continue

        file_path = DATA_DIR / filename
        
        # Check if file still exists (might have been deleted by another process)
        if not file_path.exists():
            print(f"⚠️ File already deleted: {filename}")
            continue
            
        try:
            if not is_patient_bundle(file_path):
                os.remove(file_path)
                print(f"🗑️ Removed non-patient file: {filename}")
            else:
                print(f"✅ Kept patient file: {filename}")
        except FileNotFoundError:
            print(f"⚠️ File disappeared during processing: {filename}")
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")

def main():
    """Main function to be called by Airflow"""
    clean_folder()

if __name__ == "__main__":
    main()