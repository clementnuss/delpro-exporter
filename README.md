# DelPro Exporter

A Prometheus exporter for DeLaval DelPro farm management system milking data.

## Project Structure

```
├── main.go                     # HTTP server and application entry point
├── internal/
│   ├── models/                 # Data structures and constants
│   │   └── models.go
│   ├── database/               # Database access layer
│   │   └── database.go
│   ├── metrics/                # Metrics creation and export logic
│   │   └── metrics.go
│   └── exporter/               # Main service layer
│       └── exporter.go
└── README.md
```

## Features

Exports the following metrics with comprehensive animal labeling:
- `delpro_milk_yield_liters` - Total milk yield per session in liters
- `delpro_milk_sessions_total` - Total number of milking sessions
- `delpro_milk_conductivity_avg` - Average milk conductivity
- `delpro_milking_duration_seconds` - Duration of milking session in seconds
- `delpro_device_utilization_sessions_per_hour` - Device utilization in sessions per hour

All metrics include detailed labels:
- `animal_number` - Farm animal number
- `animal_name` - Animal name
- `animal_reg_no` - Official registration number
- `breed` - Breed name in French (Holstein Frisonne, Montbéliarde, etc.)
- `milk_device_id` - Milking device identifier

## Usage

```bash
export SQL_PASSWORD="DelPro123!"
go run . --web.listen-address=:9090 --db.host=localhost
```

## Endpoints

- `http://localhost:9090/metrics` - Current metrics in Prometheus format (no timestamps)
- `http://localhost:9090/historical-metrics` - Historical metrics with timestamps for VictoriaMetrics import
- `http://localhost:9090/` - Web interface with links to both endpoints

## Configuration

- `--web.listen-address`: Address to listen on (default: `:9090`)
- `--db.host`: Database host (default: `localhost`)
- `--db.port`: Database port (default: `1433`)
- `--db.name`: Database name (default: `DelPro`)
- `--db.user`: Database user (default: `sa`)
- `SQL_PASSWORD`: Environment variable for database password (required)

## Historical Data Import

To import historical data into VictoriaMetrics:

```bash
# Import historical metrics with timestamps
curl -X POST 'http://your-victoriametrics:8428/api/v1/import/prometheus' \
  --data-binary @<(curl -s http://localhost:9090/historical-metrics)
```

Or save to file for batch import:
```bash
curl -s http://localhost:9090/historical-metrics > historical_data.txt
curl -X POST 'http://your-victoriametrics:8428/api/v1/import/prometheus' \
  --data-binary @historical_data.txt
```

The historical endpoint provides metrics with millisecond timestamps matching the actual milking session times from the DelPro database.