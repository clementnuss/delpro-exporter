package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	_ "github.com/microsoft/go-mssqldb"
)

const (
	MilkYieldTotal = "delpro_milk_yield_liters"
)

type DelProExporter struct {
	db *sql.DB
}

type MilkingRecord struct {
	AnimalNumber string
	AnimalName   string
	AnimalRegNo  string
	BreedName    string
	DeviceID     string
	Yield        float64
	Conductivity *int
	Duration     *int
	BeginTime    time.Time
	EndTime      time.Time
}

func (r *MilkingRecord) labelStr() string {
	return fmt.Sprintf(`animal_number="%s",animal_name="%s",animal_reg_no="%s",breed="%s",milk_device_id="%s"`,
		r.AnimalNumber, r.AnimalName, r.AnimalRegNo, r.BreedName, r.DeviceID)
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

func translateBreedToFrench(englishBreed string) string {
	frenchBreeds := map[string]string{
		"Holstein Friesian":     "Holstein",
		"Montbeliard":           "Montbéliarde",
		"Swedish Red-and-White": "Rouge Suédoise",
		"Cross Breed":           "Croisée",
		"Unknown Breed":         "Race Inconnue",
	}

	if frenchName, exists := frenchBreeds[englishBreed]; exists {
		return frenchName
	}
	// Return original name if no translation found
	return englishBreed
}

func (e *DelProExporter) UpdateMetrics() {
	records, err := e.collectMilkingMetrics(24 * time.Hour)
	if err != nil {
		log.Printf("Error collecting milking metrics: %v", err)
		return
	}

	e.createMetricsFromRecords(records)
	e.collectDeviceUtilization()
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
func (e *DelProExporter) WriteHistoricalMetrics(w io.Writer) {
	records, err := e.collectMilkingMetrics(365 * 24 * time.Hour)
	if err != nil {
		log.Printf("unable to collect historical milking metrics: %v", err)
	}

	s := metrics.NewSet()

	for _, r := range records {
		labelStr := r.labelStr()
		milkCounter := s.GetOrCreateGauge(fmt.Sprintf("%s{%s}", MilkYieldTotal, labelStr), nil)
		milkCounter.Add(r.Yield)

		marshalGauge(w, fmt.Sprintf("%s{%s}", MilkYieldTotal, labelStr), milkCounter, r.EndTime)
	}
}

func (e *DelProExporter) collectMilkingMetrics(lookbackDuration time.Duration) ([]MilkingRecord, error) {
	query := `
		SELECT 
			CAST(ba.Number AS VARCHAR(10)) as animal_number,
			COALESCE(ba.Name, 'Unknown') as animal_name,
			COALESCE(ba.OfficialRegNo, 'Unknown') as animal_reg_no,
			COALESCE(tli.ItemValue, CAST(ba.Breed AS VARCHAR(10))) as breed_name,
			CAST(smy.MilkingDevice AS VARCHAR(10)) as device_id,
			smy.TotalYield,
			smy.AvgConductivity,
			DATEDIFF(SECOND, smy.BeginTime, smy.EndTime) as duration_seconds,
			smy.BeginTime,
			smy.EndTime
		FROM SessionMilkYield smy
		INNER JOIN BasicAnimal ba ON smy.BasicAnimal = ba.OID
		LEFT JOIN TextLookupItem tli ON ba.Breed = tli.ItemID AND tli.Collection = 6
		WHERE smy.EndTime >= DATEADD(second, @TimeQty, GETDATE())
		AND smy.TotalYield IS NOT NULL
		AND ba.Number IS NOT NULL
	`

	rows, err := e.db.Query(query, sql.Named("TimeQty", -lookbackDuration.Seconds()))
	if err != nil {
		log.Printf("Error querying milking metrics: %v", err)
		return nil, err
	}
	defer rows.Close()

	var records []MilkingRecord
	for rows.Next() {
		var record MilkingRecord

		if err := rows.Scan(&record.AnimalNumber, &record.AnimalName, &record.AnimalRegNo, &record.BreedName, &record.DeviceID, &record.Yield, &record.Conductivity, &record.Duration, &record.BeginTime, &record.EndTime); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Clean label values for Prometheus (remove quotes and special characters)
		record.AnimalName = cleanLabelValue(record.AnimalName)
		record.AnimalRegNo = cleanLabelValue(record.AnimalRegNo)
		record.BreedName = cleanLabelValue(record.BreedName)

		// Translate breed name to French
		record.BreedName = translateBreedToFrench(record.BreedName)

		records = append(records, record)
	}

	log.Printf("Collected milking metrics for %d records", len(records))
	return records, nil
}

func (e *DelProExporter) createMetricsFromRecords(records []MilkingRecord) {
	for _, record := range records {

		labelStr := record.labelStr()
		metrics.GetOrCreateGauge(fmt.Sprintf(`%s{%s}`, MilkYieldTotal, labelStr), nil).Set(record.Yield)
		metrics.GetOrCreateCounter(fmt.Sprintf(`delpro_milk_sessions_total{%s}`, labelStr)).Inc()

		if record.Conductivity != nil {
			metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milk_conductivity_avg{%s}`, labelStr), nil).Set(float64(*record.Conductivity))
		}

		if record.Duration != nil {
			metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_milking_duration_seconds{%s}`, labelStr), nil).Set(float64(*record.Duration))
		}
	}
}

// GetMilkingRecords is a public method to get milking records for a specific duration
func (e *DelProExporter) GetMilkingRecords(lookbackDuration time.Duration) ([]MilkingRecord, error) {
	return e.collectMilkingMetrics(lookbackDuration)
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

		metrics.GetOrCreateGauge(fmt.Sprintf(`delpro_device_utilization_sessions_per_hour{milk_device_id="%s"}`, deviceID), nil).Set(float64(sessionCount))
	}
}

func marshalGauge(w io.Writer, prefix string, g *metrics.Gauge, t time.Time) {
	v := g.Get()
	if float64(int64(v)) == v {
		// Marshal integer values without scientific notation
		fmt.Fprintf(w, "%s %d %d\n", prefix, int64(v), t.UnixMilli())
	} else {
		fmt.Fprintf(w, "%s %g %d\n", prefix, v, t.UnixMilli())
	}
}
