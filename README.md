# DelPro Exporter

A Prometheus exporter for DeLaval DelPro farm management system milking data.

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
export SQL_PASSWORD="YourStrong@Passw0rd"
go run . --web.listen-address=:9090 --db.host=localhost
```

## Configuration

- `--web.listen-address`: Address to listen on (default: `:9090`)
- `--db.host`: Database host (default: `localhost`)
- `--db.port`: Database port (default: `1433`)
- `--db.name`: Database name (default: `DelPro`)
- `--db.user`: Database user (default: `sa`)
- `SQL_PASSWORD`: Environment variable for database password (required)

## Metrics Endpoint

Visit `http://localhost:9090/metrics` for Prometheus-formatted metrics.