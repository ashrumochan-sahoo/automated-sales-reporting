#!/bin/bash

# ============================================
# Automated Sales Reporting Pipeline
# Author: Ashrumochan Sahoo
# Purpose: Shell script to execute ETL pipeline
#          with environment setup and logging
# Usage: ./scripts/run_pipeline.sh
# ============================================

# ============================================
# CONFIGURATION
# ============================================

# Get project root directory
# (directory where this script lives)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/pipeline_run.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# ============================================
# HELPER FUNCTIONS
# ============================================

# Print colored output
print_info()    { echo -e "\033[0;34m[INFO]\033[0m  $1"; }
print_success() { echo -e "\033[0;32m[OK]\033[0m    $1"; }
print_error()   { echo -e "\033[0;31m[ERROR]\033[0m $1"; }
print_warning() { echo -e "\033[0;33m[WARN]\033[0m  $1"; }

# ============================================
# STEP 1: BANNER
# ============================================

echo ""
echo "============================================"
echo "  AUTOMATED SALES REPORTING PIPELINE"
echo "  Started: $TIMESTAMP"
echo "============================================"
echo ""

# ============================================
# STEP 2: NAVIGATE TO PROJECT ROOT
# ============================================

print_info "Navigating to project root..."
cd "$PROJECT_DIR" || {
    print_error "Cannot find project directory: $PROJECT_DIR"
    exit 1
}
print_success "Project root: $PROJECT_DIR"

# ============================================
# STEP 3: CHECK PYTHON IS AVAILABLE
# ============================================

print_info "Checking Python installation..."
if ! command -v python3 &> /dev/null; then
    print_error "Python3 not found! Please install Python 3.9+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1)
print_success "Python found: $PYTHON_VERSION"

# ============================================
# STEP 4: ACTIVATE VIRTUAL ENVIRONMENT
# ============================================

print_info "Activating virtual environment..."
if [ ! -d "$VENV_DIR" ]; then
    print_warning "Virtual environment not found. Creating one..."
    python3 -m venv venv
    source "$VENV_DIR/bin/activate"
    print_info "Installing dependencies..."
    pip install -r requirements.txt -q
    print_success "Dependencies installed"
else
    source "$VENV_DIR/bin/activate"
    print_success "Virtual environment activated"
fi

# ============================================
# STEP 5: CHECK DATA FILE EXISTS
# ============================================

print_info "Checking data file..."
if [ ! -f "$PROJECT_DIR/data/raw/sales_data.csv" ]; then
    print_error "Data file not found!"
    print_error "Please place sales_data.csv in data/raw/"
    exit 1
fi
print_success "Data file found: data/raw/sales_data.csv"

# ============================================
# STEP 6: CREATE LOGS DIRECTORY
# ============================================

mkdir -p "$LOG_DIR"
print_success "Logs directory ready: $LOG_DIR"

# ============================================
# STEP 7: RUN THE PIPELINE
# ============================================

echo ""
print_info "Starting ETL pipeline..."
echo "--------------------------------------------"

# Record start time
START_TIME=$(date +%s)

# Run pipeline and capture exit code
python3 -m pipeline.main
EXIT_CODE=$?

# Record end time
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "--------------------------------------------"
echo ""

# ============================================
# STEP 8: CHECK RESULT
# ============================================

if [ $EXIT_CODE -eq 0 ]; then
    print_success "Pipeline completed successfully!"
    print_success "Total execution time: ${DURATION}s"
    print_success "Logs saved to: $LOG_FILE"

    echo ""
    echo "============================================"
    echo "  ✅ PIPELINE STATUS: SUCCESS"
    echo "  Duration : ${DURATION} seconds"
    echo "  Finished : $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================"
    echo ""

    exit 0

else
    print_error "Pipeline FAILED with exit code: $EXIT_CODE"
    print_error "Check logs for details: $LOG_FILE"

    echo ""
    echo "============================================"
    echo "  ❌ PIPELINE STATUS: FAILED"
    echo "  Exit Code : $EXIT_CODE"
    echo "  Duration  : ${DURATION} seconds"
    echo "  Finished  : $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================"
    echo ""

    exit 1
fi
