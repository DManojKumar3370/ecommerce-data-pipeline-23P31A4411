#!/bin/bash
# Test Runner Script for Linux/Mac

echo ""
echo "==============================================="
echo "Running Pytest with Coverage Analysis"
echo "==============================================="
echo ""

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment
source venv/bin/activate

# Run pytest with coverage
pytest tests/ --cov=scripts --cov-report=html --cov-report=term-missing --verbose

# Check result
if [ $? -eq 0 ]; then
    echo ""
    echo "==============================================="
    echo "Tests completed successfully!"
    echo "Coverage report: htmlcov/index.html"
    echo "==============================================="
else
    echo ""
    echo "==============================================="
    echo "Tests FAILED!"
    echo "==============================================="
    exit 1
fi
