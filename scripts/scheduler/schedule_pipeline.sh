#!/bin/bash

# Pipeline Scheduler for Linux/Mac
# Add to crontab: 0 2 * * * /path/to/schedule_pipeline.sh

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

# Activate virtual environment
source venv/bin/activate

# Run orchestrator
python scripts/orchestration/orchestrator.py

# Exit with status
exit $?
