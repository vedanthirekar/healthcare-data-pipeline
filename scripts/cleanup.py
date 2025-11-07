import os
import shutil
from pathlib import Path

# Dynamically resolve the raw data folder path
BASE_DIR = Path(__file__).resolve().parent.parent
# Clean up processing folder after pipeline completes
RAW_DATA_PATH = BASE_DIR / "data" / "generated_data" / "processing"

def cleanup_raw_folder(path: Path):
    """Delete all files and folders inside the raw data folder."""
    if not path.exists():
        print(f"⚠️ Path not found: {path}")
        return

    files = list(path.iterdir())
    if not files:
        print(f"✅ No files to delete in {path}")
        return

    print(f"🧹 Cleaning up folder: {path}\n")
    for item in files:
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
            print(f"🗑️ Deleted: {item}")
        except Exception as e:
            print(f"❌ Failed to delete {item}: {e}")

    print(f"\n✅ Cleanup complete for folder: {path}")

def main():
    """Main function to be called by Airflow"""
    cleanup_raw_folder(RAW_DATA_PATH)

if __name__ == "__main__":
    main()