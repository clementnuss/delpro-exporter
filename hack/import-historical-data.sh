#!/bin/bash

# Script to import historical DelPro data month by month from restic backups
# Usage: ./import-historical-data.sh [backup_root_dir] [victoriametrics_url]

set -e

# Configuration
BACKUP_ROOT="${1:-./tmp/E}"
VICTORIAMETRICS_URL="${2:-http://localhost:8428}"
CONTAINER_NAME="delpro-sql"
SQL_PASSWORD="DelPro123!"
DATABASE_NAME="DDM"
EXPORTER_PORT="9090"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1"
    exit 1
}

check_dependencies() {
    log "Checking dependencies..."

    if ! command -v docker &> /dev/null; then
        error "Docker is required but not installed"
    fi

    if ! command -v curl &> /dev/null; then
        error "curl is required but not installed"
    fi

    if ! command -v unzip &> /dev/null; then
        error "unzip is required but not installed"
    fi

    if [ ! -d "$BACKUP_ROOT" ]; then
        error "Backup directory $BACKUP_ROOT does not exist"
    fi
}

find_backup_files() {
    log "Scanning for weekly DelPro backup zip files in $BACKUP_ROOT/Backup..."

    local backup_dir="$BACKUP_ROOT/Backup"
    if [ ! -d "$backup_dir" ]; then
        error "Backup directory $backup_dir does not exist"
    fi

    # Find all weekly backup zip files and sort them by date
    ls "$backup_dir"/FermeRoySA_WeeklyDelPro10.5_*_V_TAI_DR.bak.zip 2>/dev/null | sort > /tmp/weekly_backups.txt

    local count=$(wc -l < /tmp/weekly_backups.txt)
    log "Found $count weekly backup zip files"

    if [ "$count" -eq 0 ]; then
        error "No weekly DelPro backup zip files found in $backup_dir"
    fi

    # Find the latest daily backup
    local latest_daily=$(ls "$backup_dir"/FermeRoySA_DailyDelPro10.5_*_V_TAI_DR.bak.zip 2>/dev/null | sort | tail -1)
    if [ -n "$latest_daily" ]; then
        log "Found latest daily backup: $(basename "$latest_daily")"
        echo "$latest_daily" > /tmp/latest_daily_backup.txt
    else
        warn "No daily backup files found"
        touch /tmp/latest_daily_backup.txt
    fi

    # Extract first backup of each month and map to previous month
    log "Selecting first weekly backup of each month for historical data import..."

    > /tmp/monthly_backups.txt
    local prev_month=""
    local prev_year=""

    while IFS= read -r zip_file; do
        # Extract date from filename: FermeRoySA_WeeklyDelPro10.5_20241202T021028_V_TAI_DR.bak.zip
        local date_part=$(basename "$zip_file" | sed 's/.*_\([0-9]\{8\}\)T.*/\1/')
        local year=${date_part:0:4}
        local month=${date_part:4:2}

        # Check if this is the first backup of a new month
        if [ "$month$year" != "$prev_month$prev_year" ]; then
            if [ ! -z "$prev_month" ]; then
                # Map this backup to previous month's data
                local target_month=$prev_month
                local target_year=$prev_year

                # Handle year transition (January backup -> December of previous year)
                if [ "$month" = "01" ]; then
                    target_month="12"
                    target_year=$((year - 1))
                else
                    target_month=$(printf "%02d" $((10#$month - 1)))
                    target_year=$year
                fi

                echo "$zip_file|$target_year-$target_month" >> /tmp/monthly_backups.txt
            fi
            prev_month=$month
            prev_year=$year
        fi
    done < /tmp/weekly_backups.txt

    # Handle the last backup if needed
    if [ ! -z "$prev_month" ]; then
        local last_zip=$(tail -1 /tmp/weekly_backups.txt)
        local date_part=$(basename "$last_zip" | sed 's/.*_\([0-9]\{8\}\)T.*/\1/')
        local year=${date_part:0:4}
        local month=${date_part:4:2}
        local target_month=$(printf "%02d" $((10#$month - 1)))
        local target_year=$year

        if [ "$month" = "01" ]; then
            target_month="12"
            target_year=$((year - 1))
        fi

        echo "$last_zip|$target_year-$target_month" >> /tmp/monthly_backups.txt
    fi

    local monthly_count=$(wc -l < /tmp/monthly_backups.txt)
    log "Selected $monthly_count monthly backups for historical data import"

    # Show the user what was selected
    echo "Monthly backups selected (Backup File -> Target Month):"
    while IFS='|' read -r zip_file target_month; do
        local backup_name=$(basename "$zip_file")
        echo "  $backup_name -> $target_month"
    done < /tmp/monthly_backups.txt
}

restore_database() {
    local zip_file="$1"
    local target_month="$2"
    local backup_name=$(basename "$zip_file" .zip)

    log "Restoring database from: $zip_file"
    log "Target month: $target_month"
    log "Backup identifier: $backup_name"

    # Extract DelPro.bak from zip file
    log "Extracting DelPro.bak from zip file..."
    local temp_dir="/tmp/delpro_extract_$$"
    mkdir -p "$temp_dir"

    if ! unzip -j "$zip_file" DelPro.bak -d "$temp_dir"; then
        error "Failed to extract DelPro.bak from $zip_file"
    fi

    local backup_file="$temp_dir/DelPro.bak"
    if [ ! -f "$backup_file" ]; then
        error "DelPro.bak not found after extraction from $zip_file"
    fi

    # Stop existing container
    log "Stopping existing container if present..."
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true

    # Start fresh container
    log "Starting SQL Server container..."
    docker run -e 'ACCEPT_EULA=Y' -e "MSSQL_SA_PASSWORD=$SQL_PASSWORD" \
       --name "$CONTAINER_NAME" -p 1433:1433 \
       -v delpro-data:/var/opt/mssql \
       -d mcr.microsoft.com/mssql/server:2019-latest

    # Wait for SQL Server to start
    log "Waiting for SQL Server to start..."
    sleep 5

    # Create backup directory in container
    docker exec $CONTAINER_NAME mkdir -p /var/opt/mssql/backup

    # Copy backup file to container
    log "Copying backup file to container..."
    docker cp "$backup_file" "$CONTAINER_NAME:/var/opt/mssql/backup/DelPro.bak"

    # Clean up extracted file
    rm -rf "$temp_dir"

    # Restore database
    log "Restoring DelPro database..."
    docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
       -S localhost -U sa -P "$SQL_PASSWORD" -C \
       -Q "RESTORE DATABASE $DATABASE_NAME FROM DISK = '/var/opt/mssql/backup/DelPro.bak' WITH REPLACE,
           MOVE 'DDM' TO '/var/opt/mssql/data/DDM.mdf',
           MOVE 'DDM_Log' TO '/var/opt/mssql/data/DDM_Log.ldf'"

    # Verify database restoration
    log "Verifying database restoration..."
    docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
       -S localhost -U sa -P "$SQL_PASSWORD" -C \
       -Q "SELECT name FROM sys.databases WHERE name = '$DATABASE_NAME'"
}

start_exporter() {
    log "Starting DelPro exporter..."

    # Kill any existing exporter process
    pkill -f "delpro-exporter" || true
    pkill -f "go run.*main.go" || true

    # Set environment and start exporter in background
    export SQL_PASSWORD="$SQL_PASSWORD"

    log "Building and starting exporter on port $EXPORTER_PORT..."
    go build -o /tmp/delpro-exporter . || error "Failed to build exporter"

    /tmp/delpro-exporter --listen-address=":$EXPORTER_PORT" --db-host=localhost --db-name=$DATABASE_NAME &
    EXPORTER_PID=$!

    # Wait for exporter to start
    log "Waiting for exporter to start..."
    for i in {1..30}; do
        if curl -s "http://localhost:$EXPORTER_PORT/" > /dev/null 2>&1; then
            log "Exporter started successfully (PID: $EXPORTER_PID)"
            return 0
        fi
        sleep 2
    done

    error "Exporter failed to start within 60 seconds"
}

export_historical_data() {
    local backup_identifier="$1"
    local target_month="$2"
    local output_file="/tmp/historical_data_${backup_identifier}.txt"

    # Calculate start and end dates for the target month
    local year_month="$target_month"
    local year="${year_month%-*}"
    local month="${year_month#*-}"

    local start_date="${year}-${month}-01"

    # Calculate last day of month
    local next_month=$((10#$month + 1))
    local next_year=$year
    if [ "$next_month" -gt 12 ]; then
        next_month=1
        next_year=$((year + 1))
    fi
    local next_month_formatted=$(printf "%02d" $next_month)
    local last_day=$(date -d "${next_year}-${next_month_formatted}-01 -1 day" +%Y-%m-%d 2>/dev/null || \
                     date -v1d -v+1m -v-1d -j -f "%Y-%m-%d" "${year}-${month}-01" +%Y-%m-%d 2>/dev/null)

    # Fallback for last day calculation if date commands fail
    if [ -z "$last_day" ]; then
        case "$month" in
            "02")
                # Check for leap year
                if [ $((year % 4)) -eq 0 ] && ([ $((year % 100)) -ne 0 ] || [ $((year % 400)) -eq 0 ]); then
                    last_day="${year}-02-29"
                else
                    last_day="${year}-02-28"
                fi
                ;;
            "04"|"06"|"09"|"11")
                last_day="${year}-${month}-30"
                ;;
            *)
                last_day="${year}-${month}-31"
                ;;
        esac
    fi

    log "Exporting historical data for $target_month ($start_date to $last_day)..."
    log "Output file: $output_file"

    # Export historical metrics with date range
    local url="http://localhost:$EXPORTER_PORT/historical-metrics?start=${start_date}&end=${last_day}"
    log "Requesting: $url"

    if ! curl -s "$url" > "$output_file"; then
        error "Failed to export historical metrics"
    fi

    local line_count=$(wc -l < "$output_file")
    log "Exported $line_count lines of historical metrics for $target_month"

    if [ "$line_count" -eq 0 ]; then
        warn "No historical data exported for $target_month ($start_date to $last_day)"
        return 1
    fi

    return 0
}

export_historical_data_for_latest() {
    local backup_identifier="$1"
    local year_month="$2"
    local end_date="$3"
    local output_file="/tmp/historical_data_${backup_identifier}.txt"

    # Calculate start date (first day of the month)
    local start_date="${year_month}-01"

    log "Exporting historical data for latest backup: $year_month ($start_date to $end_date)..."
    log "Output file: $output_file"

    # Export historical metrics with date range from month start to backup date
    local url="http://localhost:$EXPORTER_PORT/historical-metrics?start=${start_date}&end=${end_date}"
    log "Requesting: $url"

    if ! curl -s "$url" > "$output_file"; then
        error "Failed to export historical metrics for latest backup"
    fi

    local line_count=$(wc -l < "$output_file")
    log "Exported $line_count lines of historical metrics for latest backup ($start_date to $end_date)"

    if [ "$line_count" -eq 0 ]; then
        warn "No historical data exported for latest backup ($start_date to $end_date)"
        return 1
    fi

    return 0
}

check_highest_oid() {
    log "Checking X-Highest-OID header from historical-metrics endpoint..."

    local headers_file="/tmp/exporter_headers.txt"

    # Get headers from the historical-metrics endpoint
    if curl -I -s "http://localhost:$EXPORTER_PORT/historical-metrics" > "$headers_file"; then
        local highest_oid=$(grep -i "x-highest-oid" "$headers_file" | cut -d' ' -f2 | tr -d '\r\n')
        if [ -n "$highest_oid" ]; then
            log "X-Highest-OID: $highest_oid"
            echo "HIGHEST_OID: $highest_oid" >> /tmp/import_summary.txt
        else
            warn "X-Highest-OID header not found in historical-metrics response"
        fi
    else
        warn "Failed to get headers from historical-metrics endpoint"
    fi

    rm -f "$headers_file"
}

process_latest_daily_backup() {
    local latest_daily_file="/tmp/latest_daily_backup.txt"

    if [ ! -s "$latest_daily_file" ]; then
        log "No latest daily backup to process"
        return 0
    fi

    local latest_daily=$(cat "$latest_daily_file")
    local backup_name=$(basename "$latest_daily" .zip)

    # Extract date from filename for logging
    local date_part=$(basename "$latest_daily" | sed 's/.*_\([0-9]\{8\}\)T.*/\1/')
    local formatted_date="${date_part:0:4}-${date_part:4:2}-${date_part:6:2}"
    local year_month="${formatted_date:0:7}"

    log "Processing latest daily backup: $backup_name"
    log "Backup date: $formatted_date"
    log "Target month: $year_month"

    # Restore the latest daily backup
    restore_database "$latest_daily" "latest"

    # Start exporter
    start_exporter

    # Export and import data for the current month up to the backup date
    local backup_identifier="latest_${backup_name}"
    if export_historical_data_for_latest "$backup_identifier" "$year_month" "$formatted_date"; then
        local data_file="/tmp/historical_data_${backup_identifier}.txt"

        # Try to import to VictoriaMetrics
        if ! import_to_victoriametrics "$data_file" "$backup_identifier"; then
            warn "Keeping data file for manual import: $data_file"
        else
            rm -f "$data_file"
        fi
    fi

    # Check the highest OID after processing
    check_highest_oid

    log "Latest daily backup processing completed"
    echo "LATEST_BACKUP_DATE: $formatted_date" >> /tmp/import_summary.txt
    echo "LATEST_BACKUP_FILE: $backup_name" >> /tmp/import_summary.txt
}

import_to_victoriametrics() {
    local data_file="$1"
    local backup_identifier="$2"

    log "Importing data to VictoriaMetrics at $VICTORIAMETRICS_URL..."

    if ! curl -X POST "$VICTORIAMETRICS_URL/api/v1/import/prometheus" \
         --data-binary "@$data_file" \
         -w "HTTP Status: %{http_code}\n"; then
        warn "Failed to import data for $backup_identifier to VictoriaMetrics"
        return 1
    fi

    log "Successfully imported data for $backup_identifier"
    return 0
}

cleanup() {
    log "Cleaning up..."

    # Stop exporter
    if [ ! -z "$EXPORTER_PID" ]; then
        kill $EXPORTER_PID 2>/dev/null || true
    fi
    pkill -f "delpro-exporter" || true

    # Stop container
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
}

process_backup() {
    local zip_file="$1"
    local target_month="$2"
    local backup_identifier="${target_month}_$(basename "$zip_file" .zip)"

    log "Processing backup for month: $target_month"
    log "Using zip file: $zip_file"
    log "Backup identifier: $backup_identifier"

    # Restore database
    restore_database "$zip_file" "$target_month"

    # Start exporter
    start_exporter

    # Export and import data
    if export_historical_data "$backup_identifier" "$target_month"; then
        local data_file="/tmp/historical_data_${backup_identifier}.txt"

        # Try to import to VictoriaMetrics
        if ! import_to_victoriametrics "$data_file" "$backup_identifier"; then
            warn "Keeping data file for manual import: $data_file"
        else
            rm -f "$data_file"
        fi
    fi

    # Clean up for next iteration
    cleanup

    log "Completed processing backup: $backup_identifier"
    echo "----------------------------------------"
}

main() {
    log "Starting historical data import process"
    log "Backup root: $BACKUP_ROOT"
    log "VictoriaMetrics URL: $VICTORIAMETRICS_URL"

    # Trap to ensure cleanup on exit
    trap cleanup EXIT

    check_dependencies
    find_backup_files

    # Ask user for confirmation
    echo
    read -p "Do you want to process all $(wc -l < /tmp/monthly_backups.txt) monthly backup files? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Operation cancelled by user"
        exit 0
    fi

    # Process each monthly backup file
    local count=0
    local total=$(wc -l < /tmp/monthly_backups.txt)

    while IFS='|' read -r zip_file target_month; do
        count=$((count + 1))
        log "Processing backup $count of $total"

        if ! process_backup "$zip_file" "$target_month"; then
            error "Failed to process backup: $zip_file for month $target_month"
        fi

        # Brief pause between backups
        sleep 2
    done < /tmp/monthly_backups.txt

    log "All monthly backups processed successfully!"

    # Initialize summary file
    echo "DELPRO HISTORICAL DATA IMPORT SUMMARY" > /tmp/import_summary.txt
    echo "=====================================" >> /tmp/import_summary.txt
    echo "MONTHLY_BACKUPS_PROCESSED: $total" >> /tmp/import_summary.txt
    echo "" >> /tmp/import_summary.txt

    # Process latest daily backup
    echo
    log "Processing latest daily backup to capture current state..."
    process_latest_daily_backup

    # Show final summary
    echo
    log "=== IMPORT SUMMARY ==="
    cat /tmp/import_summary.txt

    # Cleanup temp files
    rm -f /tmp/weekly_backups.txt /tmp/monthly_backups.txt /tmp/latest_daily_backup.txt
}

# Show usage if help requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Usage: $0 [backup_root_dir] [victoriametrics_url]"
    echo
    echo "Arguments:"
    echo "  backup_root_dir      Root directory containing Backup/ subdirectory with weekly zip files (default: ./tmp/E)"
    echo "  victoriametrics_url  VictoriaMetrics URL for importing data (default: http://localhost:8428)"
    echo
    echo "The script expects weekly backup zip files in backup_root_dir/Backup/ with the format:"
    echo "  FermeRoySA_WeeklyDelPro10.5_YYYYMMDDTHHMMSS_V_TAI_DR.bak.zip"
    echo
    echo "Example:"
    echo "  $0 ./tmp/E http://localhost:8428"
    echo "  $0 /path/to/backup/root http://your-victoriametrics:8428"
    exit 0
fi

main "$@"
