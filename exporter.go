package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

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

func cleanLabelValue(value string) string {
	// Replace problematic characters for Prometheus labels
	value = strings.ReplaceAll(value, "\"", "")
	value = strings.ReplaceAll(value, "\\", "")
	value = strings.ReplaceAll(value, "\n", "")
	value = strings.ReplaceAll(value, "\r", "")
	return value
}

func (e *DelProExporter) UpdateMetrics() {
	e.collectMilkingMetrics()
	e.collectDeviceUtilization()
}

func (e *DelProExporter) collectMilkingMetrics() {
	query := `
		SELECT 
			CAST(ba.Number AS VARCHAR(10)) as animal_number,
			COALESCE(ba.Name, 'Unknown') as animal_name,
			COALESCE(ba.OfficialRegNo, 'Unknown') as animal_reg_no,
			CAST(smy.MilkingDevice AS VARCHAR(10)) as device_id,
			smy.TotalYield,
			smy.AvgConductivity,
			DATEDIFF(SECOND, smy.BeginTime, smy.EndTime) as duration_seconds
		FROM SessionMilkYield smy
		INNER JOIN BasicAnimal ba ON smy.BasicAnimal = ba.OID
		WHERE smy.BeginTime >= DATEADD(day, -1, GETDATE())
		AND smy.TotalYield IS NOT NULL
		AND ba.Number IS NOT NULL
	`

	rows, err := e.db.Query(query)
	if err != nil {
		log.Printf("Error querying milking metrics: %v", err)
		return
	}
	defer rows.Close()

	recordCount := 0
	for rows.Next() {
		recordCount++
		var animalNumber, animalName, animalRegNo, deviceID string
		var yield float64
		var conductivity *int
		var duration *int

		if err := rows.Scan(&animalNumber, &animalName, &animalRegNo, &deviceID, &yield, &conductivity, &duration); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Clean label values for Prometheus (remove quotes and special characters)
		animalName = cleanLabelValue(animalName)
		animalRegNo = cleanLabelValue(animalRegNo)

		labelStr := fmt.Sprintf(`animal_number="%s",animal_name="%s",animal_reg_no="%s",device_id="%s"`, animalNumber, animalName, animalRegNo, deviceID)
		
		metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milk_yield_liters{%s}`, labelStr), nil).Set(yield)
		metrics.GetOrCreateCounter(fmt.Sprintf(`delpro_milk_sessions_total{%s}`, labelStr)).Inc()

		if conductivity != nil {
			metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milk_conductivity_avg{%s}`, labelStr), nil).Set(float64(*conductivity))
		}

		if duration != nil {
			metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milking_duration_seconds{%s}`, labelStr), nil).Set(float64(*duration))
		}
	}
	log.Printf("Collected milking metrics for %d records", recordCount)
}

func (e *DelProExporter) collectDeviceUtilization() {
	query := `
		SELECT 
			CAST(MilkingDevice AS VARCHAR(10)) as device_id,
			COUNT(*) as session_count
		FROM SessionMilkYield 
		WHERE BeginTime >= DATEADD(day, -1, GETDATE())
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
