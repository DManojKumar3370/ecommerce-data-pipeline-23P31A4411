import subprocess

def run(cmd: str):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise SystemExit(f"Command failed: {cmd}")

def main():
    # 1) Ingest raw data into staging
    run("python scripts/ingestion/ingest_to_staging.py")

    # 2) Transform staging -> warehouse
    run("python scripts/transformation/staging_to_production.py")

    # 3) Run data quality checks
    run("python scripts/quality_checks/validate_data.py")
