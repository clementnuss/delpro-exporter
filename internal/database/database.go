package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/clementnuss/delpro-exporter/internal/models"
	_ "github.com/microsoft/go-mssqldb"
)

// Client handles database connections and operations
type Client struct {
	db *sql.DB
}

// NewClient creates a new database client instance
func NewClient(host, port, dbname, user, password string) *Client {
	connString := fmt.Sprintf("server=%s;port=%s;database=%s;user id=%s;password=%s;encrypt=disable",
		host, port, dbname, user, password)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	return &Client{
		db: db,
	}
}

// Close closes the database connection
func (c *Client) Close() error {
	return c.db.Close()
}

// GetMilkingRecords retrieves milking records from the database for the specified duration
func (c *Client) GetMilkingRecords(start, end time.Time) ([]models.MilkingRecord, error) {
	query := `
		SELECT 
			smy.OID,
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
		WHERE smy.EndTime >= @StartTime AND smy.EndTime < @EndTime
		AND smy.TotalYield IS NOT NULL
		AND ba.Number IS NOT NULL
		ORDER BY smy.OID
	`

	rows, err := c.db.Query(query, sql.Named("StartTime", start), sql.Named("EndTime", end))
	if err != nil {
		log.Printf("Error querying milking metrics: %v", err)
		return nil, err
	}
	defer rows.Close()

	var records []models.MilkingRecord
	for rows.Next() {
		var record models.MilkingRecord

		if err := rows.Scan(&record.OID, &record.AnimalNumber, &record.AnimalName, &record.AnimalRegNo, &record.BreedName, &record.DeviceID, &record.Yield, &record.Conductivity, &record.Duration, &record.BeginTime, &record.EndTime); err != nil {
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

// GetDeviceUtilization retrieves device utilization metrics
func (c *Client) GetDeviceUtilization() (map[string]int, error) {
	query := `
		SELECT 
			CAST(MilkingDevice AS VARCHAR(10)) as device_id,
			COUNT(*) as session_count
		FROM SessionMilkYield 
		WHERE BeginTime >= DATEADD(day, -1, GETDATE())
		AND TotalYield IS NOT NULL
		GROUP BY MilkingDevice
	`

	rows, err := c.db.Query(query)
	if err != nil {
		log.Printf("Error querying device utilization: %v", err)
		return nil, err
	}
	defer rows.Close()

	utilization := make(map[string]int)
	for rows.Next() {
		var deviceID string
		var sessionCount int

		if err := rows.Scan(&deviceID, &sessionCount); err != nil {
			log.Printf("Error scanning device utilization row: %v", err)
			continue
		}

		utilization[deviceID] = sessionCount
	}

	return utilization, nil
}

// cleanLabelValue removes problematic characters from Prometheus label values
func cleanLabelValue(value string) string {
	value = strings.ReplaceAll(value, "\"", "")
	value = strings.ReplaceAll(value, "\\", "")
	value = strings.ReplaceAll(value, "\n", "")
	value = strings.ReplaceAll(value, "\r", "")
	return value
}

// translateBreedToFrench converts English breed names to French equivalents
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
	return englishBreed
}

