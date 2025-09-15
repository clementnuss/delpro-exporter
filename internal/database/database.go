package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
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
	// Add explicit timeout parameters and packet size limit for MTU issues
	connString := fmt.Sprintf("server=%s;port=%s;database=%s;user id=%s;password=%s;encrypt=disable;connection timeout=10;dial timeout=10",
		host, port, dbname, user, password)

	log.Printf("Attempting to connect to database at %s:%s", host, port)

	// Test network connectivity first
	if !testNetworkConnectivity(host, port) {
		log.Fatal("Network connectivity test failed")
	}

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Failed to create database connection:", err)
	}

	// Set connection pool timeouts
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// Try to ping with multiple retries
	const maxRetries = 3
	for i := range maxRetries {
		log.Printf("Database ping attempt %d/%d", i+1, maxRetries)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := db.PingContext(ctx)
		cancel()

		if err == nil {
			log.Printf("Database connection successful")
			return &Client{db: db}
		}

		log.Printf("Database ping failed (attempt %d/%d): %v", i+1, maxRetries, err)

		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * 2 * time.Second) // Exponential backoff
		}
	}

	log.Fatal("Failed to connect to database after all retries")
	return nil
}

// Close closes the database connection
func (c *Client) Close() error {
	return c.db.Close()
}

// testNetworkConnectivity tests basic TCP connectivity to the database
func testNetworkConnectivity(host, port string) bool {
	log.Printf("Testing network connectivity to %s:%s", host, port)

	timeout := 10 * time.Second
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		log.Printf("Network connectivity test failed: %v", err)
		return false
	}

	conn.Close()
	log.Printf("Network connectivity test successful")
	return true
}

// GetMilkingRecords retrieves milking records from the database for the specified duration
func (c *Client) GetMilkingRecords(ctx context.Context, start, end time.Time, lastOID int64) ([]*models.MilkingRecord, error) {
	return c.GetMilkingRecordsWithOIDRange(ctx, start, end, lastOID, 0)
}

// GetMilkingRecordsWithOIDRange retrieves milking records from the database for the specified duration and OID range
func (c *Client) GetMilkingRecordsWithOIDRange(ctx context.Context, start, end time.Time, startOID, endOID int64) ([]*models.MilkingRecord, error) {
	query := `
		SELECT 
			smy.OID,
			CAST(ba.Number AS VARCHAR(10)) as animal_number,
			COALESCE(ba.Name, 'Unknown') as animal_name,
			COALESCE(ba.OfficialRegNo, 'Unknown') as animal_reg_no,
			COALESCE(tli.ItemValue, CAST(ba.Breed AS VARCHAR(10))) as breed_name,
			CAST(smy.MilkingDevice AS VARCHAR(10)) as device_id,
			COALESCE(md.Name, 'Unknown') as destination_name,
			als.LactationNumber as lactation_number,
			DATEDIFF(day, als.StartDate, smy.EndTime) as days_in_lactation,
			smy.TotalYield,
			smy.AvgConductivity,
			DATEDIFF(SECOND, smy.BeginTime, smy.EndTime) as duration_seconds,
			vmy.Occ as somatic_cell_count,
			vmy.Incomplete as incomplete,
			vmy.Kickoff as kickoff,
			smy.BeginTime,
			smy.EndTime
		FROM SessionMilkYield smy
		INNER JOIN BasicAnimal ba ON smy.BasicAnimal = ba.OID
		LEFT JOIN TextLookupItem tli ON ba.Breed = tli.ItemID AND tli.Collection = 6
		LEFT JOIN VoluntarySessionMilkYield vmy ON smy.OID = vmy.OID
		LEFT JOIN MilkDestination md ON smy.Destination = md.OID
		LEFT JOIN AnimalLactationSummary als ON ba.OID = als.Animal AND als.EndDate IS NULL
		WHERE smy.EndTime >= @StartTime AND smy.EndTime < @EndTime
		AND smy.OID > @StartOID
		AND smy.TotalYield IS NOT NULL
		AND ba.Number IS NOT NULL`

	// Add optional end OID condition
	var params []any
	params = append(params, sql.Named("StartTime", start), sql.Named("EndTime", end), sql.Named("StartOID", startOID))

	if endOID > 0 {
		query += ` AND smy.OID <= @EndOID`
		params = append(params, sql.Named("EndOID", endOID))
	}

	query += ` ORDER BY smy.OID`

	rows, err := c.db.QueryContext(ctx, query, params...)
	if err != nil {
		log.Printf("Error querying milking metrics: %v", err)
		return nil, err
	}
	defer rows.Close()

	var records []*models.MilkingRecord
	for rows.Next() {
		record := &models.MilkingRecord{}

		if err := rows.Scan(
			&record.OID,
			&record.AnimalNumber,
			&record.AnimalName,
			&record.AnimalRegNo,
			&record.BreedName,
			&record.DeviceID,
			&record.DestinationName,
			&record.LactationNumber,
			&record.DaysInLactation,
			&record.Yield,
			&record.Conductivity,
			&record.Duration,
			&record.SomaticCellCount,
			&record.Incomplete,
			&record.Kickoff,
			&record.BeginTime,
			&record.EndTime,
		); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Clean label values for Prometheus (remove quotes and special characters)
		record.AnimalName = cleanLabelValue(record.AnimalName)
		record.AnimalRegNo = cleanLabelValue(record.AnimalRegNo)
		record.BreedName = cleanLabelValue(record.BreedName)
		record.DestinationName = cleanLabelValue(record.DestinationName)

		// Translate breed name to French
		record.BreedName = translateBreedToFrench(record.BreedName)

		records = append(records, record)
	}

	return records, nil
}

// GetDeviceUtilization retrieves device utilization metrics
func (c *Client) GetDeviceUtilization(ctx context.Context) (map[string]int, error) {
	query := `
		SELECT 
			CAST(MilkingDevice AS VARCHAR(10)) as device_id,
			COUNT(*) as session_count
		FROM SessionMilkYield 
		WHERE BeginTime >= DATEADD(day, -1, GETDATE())
		AND TotalYield IS NOT NULL
		GROUP BY MilkingDevice
	`

	rows, err := c.db.QueryContext(ctx, query)
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
