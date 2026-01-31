from datetime import datetime
import subprocess

def run_pipeline():
    print(f"[{datetime.utcnow()}] Starting pipeline run")
    result = subprocess.run("python scripts/pipeline_orchestrator.py", shell=True)
    if result.returncode != 0:
        print(f"[{datetime.utcnow()}] Pipeline failed with code {result.returncode}")
    else:
        print(f"[{datetime.utcnow()}] Pipeline completed successfully")

if __name__ == '__main__':
    # For this project, a single run is enough
    run_pipeline()
