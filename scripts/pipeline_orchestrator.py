import subprocess

def run(cmd: str):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise SystemExit(f"Command failed: {cmd}")

def main():
    # 1) Ingest raw data into staging (update path if your script name is different)
    run("python scripts/ingestion/load_raw_to_staging.py")

    # 2) Transform staging -> warehouse
    run("python scripts/transformation/staging_to_production.py")

    # 3) Run data quality checks (validate_data.py already exists)
    run("python scripts/quality_checks/validate_data.py")

if __name__ == '__main__':
    main()
