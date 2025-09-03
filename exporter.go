package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/VictoriaMetrics/metrics"
	_ "github.com/microsoft/go-mssqldb"
)

type DelProExporter struct {
	db *sql.DB
}

func NewDelProExporter(host, port, dbname, user, password string) *DelProExporter {
	connString := fmt.Sprintf("server=%s;port=%s;database=%s;user id=%s;password=%s;encrypt=disable",
		host, port, dbname, user, password)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	return &DelProExporter{
		db: db,
	}
}

func (e *DelProExporter) UpdateMetrics() {
	e.collectMilkingMetrics()
	e.collectDeviceUtilization()
}

func (e *DelProExporter) collectMilkingMetrics() {
	query := `
		SELECT 
			CAST(BasicAnimal AS VARCHAR(10)) as animal_id,
			CAST(MilkingDevice AS VARCHAR(10)) as device_id,
			TotalYield,
			AvgConductivity,
			DATEDIFF(SECOND, BeginTime, EndTime) as duration_seconds
		FROM SessionMilkYield 
		WHERE BeginTime >= DATEADD(hour, -1, GETDATE())
		AND TotalYield IS NOT NULL
	`

	rows, err := e.db.Query(query)
	if err != nil {
		log.Printf("Error querying milking metrics: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var animalID, deviceID string
		var yield float64
		var conductivity *int
		var duration *int

		if err := rows.Scan(&animalID, &deviceID, &yield, &conductivity, &duration); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milk_yield_liters{animal_id="%s",device_id="%s"}`, animalID, deviceID), nil).Set(yield)
		metrics.GetOrCreateCounter(fmt.Sprintf(`delpro_milk_sessions_total{animal_id="%s",device_id="%s"}`, animalID, deviceID)).Inc()

		if conductivity != nil {
			metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milk_conductivity_avg{animal_id="%s",device_id="%s"}`, animalID, deviceID), nil).Set(float64(*conductivity))
		}

		if duration != nil {
			metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milking_duration_seconds{animal_id="%s",device_id="%s"}`, animalID, deviceID), nil).Set(float64(*duration))
		}
	}
}

func (e *DelProExporter) collectDeviceUtilization() {
	query := `
		SELECT 
			CAST(MilkingDevice AS VARCHAR(10)) as device_id,
			COUNT(*) as session_count
		FROM SessionMilkYield 
		WHERE BeginTime >= DATEADD(hour, -1, GETDATE())
		AND TotalYield IS NOT NULL
		GROUP BY MilkingDevice
	`

	rows, err := e.db.Query(query)
	if err != nil {
		log.Printf("Error querying device utilization: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var deviceID string
		var sessionCount int

		if err := rows.Scan(&deviceID, &sessionCount); err != nil {
			log.Printf("Error scanning device utilization row: %v", err)
			continue
		}

		metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_device_utilization_sessions_per_hour{device_id="%s"}`, deviceID), nil).Set(float64(sessionCount))
	}
}
