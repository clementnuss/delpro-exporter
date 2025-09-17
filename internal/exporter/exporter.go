package exporter

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/clementnuss/delpro-exporter/internal/database"
	delprometrics "github.com/clementnuss/delpro-exporter/internal/metrics"
	"github.com/clementnuss/delpro-exporter/internal/models"
)

// DelProExporter combines database and metrics operations
type DelProExporter struct {
	db         *database.Client
	metrics    *delprometrics.Exporter
	oidFile    string
	lastOID    int64
	dbLocation *time.Location
}

// NewDelProExporter creates a new DelPro exporter instance
func NewDelProExporter(host, port, dbname, user, password string, dbLocation *time.Location) *DelProExporter {
	// Determine OID file path - use working directory if available
	oidFilePath := "delpro_last_oid.txt"
	if wd, err := os.Getwd(); err == nil {
		oidFilePath = wd + "/delpro_last_oid.txt"
	}

	exporter := &DelProExporter{
		db:         database.NewClient(host, port, dbname, user, password, dbLocation),
		metrics:    delprometrics.NewExporter(),
		oidFile:    oidFilePath,
		dbLocation: dbLocation,
	}

	log.Printf("Using OID file path: %s", oidFilePath)

	// Load last processed OID from file
	exporter.loadLastOID()

	// Initialize counters for animals from past 24h to ensure proper increase() calculations
	exporter.initializeCounters()

	return exporter
}

// Close closes the database connection
func (e *DelProExporter) Close() error {
	return e.db.Close()
}

// UpdateMetrics collects and updates current metrics from the database
func (e *DelProExporter) UpdateMetrics() {
	// Create context with timeout for database operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get records since last processed OID to prevent duplicate counter increments
	// Add 5 minute delay in live mode to ensure voluntary session milk yield data is populated
	now := time.Now().Add(-5 * time.Minute)

	records, err := e.db.GetMilkingRecords(ctx, now.Add(-models.DefaultLookbackWindow), now, e.lastOID)
	if err != nil {
		log.Printf("Error collecting milking metrics: %v", err)
		return
	}

	// Update metrics only for new records
	e.metrics.CreateMetricsFromRecords(nil, nil, records)

	// Update last processed OID if we have new records
	if len(records) > 0 {
		var highestOID int64
		for _, record := range records {
			if record.OID > highestOID {
				highestOID = record.OID
			}
		}
		if highestOID > e.lastOID {
			e.lastOID = highestOID
			e.saveLastOID()
			log.Printf("Updated last processed OID to: %d", e.lastOID)
		}
	}

	utilization, err := e.db.GetDeviceUtilization(ctx)
	if err != nil {
		log.Printf("Error collecting device utilization: %v", err)
		return
	}

	e.metrics.CreateDeviceUtilizationMetrics(utilization)
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
func (e *DelProExporter) WriteHistoricalMetrics(r *http.Request, w http.ResponseWriter) {
	// Use request context with additional timeout for database operations
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	query := r.URL.Query()
	var records []*models.MilkingRecord

	// Check if OID range is specified
	if query.Has("start_oid") {
		// Parse OID range parameters
		startOID, endOID, err := parseOIDRange(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Use time range for context, but OID range for filtering
		startTime, endTime, err := e.parseTimeRangeWithLocation(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		records, err = e.db.GetMilkingRecordsWithOIDRange(ctx, startTime, endTime, startOID, endOID)
		if err != nil {
			log.Printf("Unable to collect historical milking metrics by OID range: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	} else {
		// Parse query parameters for start and end dates
		startTime, endTime, err := e.parseTimeRangeWithLocation(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		records, err = e.db.GetMilkingRecords(ctx, startTime, endTime, 0)
		if err != nil {
			log.Printf("Unable to collect historical milking metrics: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}

	// Find highest OID processed
	var highestOID int64
	for _, record := range records {
		if record.OID > highestOID {
			highestOID = record.OID
		}
	}

	// Set HTTP header with highest Object Identifier processed
	if highestOID > 0 {
		w.Header().Set("X-Highest-OID", strconv.FormatInt(highestOID, 10))
	}

	// Check if client accepts gzip compression
	var writer io.Writer = w
	acceptsGzip := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")

	if acceptsGzip {
		w.Header().Set("Content-Encoding", "gzip")
		gzWriter := gzip.NewWriter(w)
		defer gzWriter.Close()
		writer = gzWriter
	}

	e.metrics.WriteHistoricalMetricsWithInit(writer, records)
	log.Printf("Collected historical milking metrics for %d records", len(records))
}

// parseTimeRangeWithLocation parses start and end time from HTTP request query parameters using database location
func (e *DelProExporter) parseTimeRangeWithLocation(r *http.Request) (time.Time, time.Time, error) {
	now := time.Now()

	// Default to historical lookback period if no parameters provided
	defaultStart := now.Add(-models.HistoricalLookbackHours)
	defaultEnd := now

	query := r.URL.Query()

	// Parse start parameter
	startTime := defaultStart
	if startStr := query.Get("start"); startStr != "" {
		if parsedStart, err := time.Parse(time.RFC3339, startStr); err == nil {
			startTime = parsedStart
		} else if parsedStart, err := time.Parse("2006-01-02", startStr); err == nil {
			// For date-only format, interpret in database timezone
			startTime = time.Date(parsedStart.Year(), parsedStart.Month(), parsedStart.Day(), 0, 0, 0, 0, e.dbLocation)
		} else {
			return time.Time{}, time.Time{}, errors.New("invalid start time format, use RFC3339 (2006-01-02T15:04:05Z) or date format (2006-01-02)")
		}
	}

	// Parse end parameter
	endTime := defaultEnd
	if endStr := query.Get("end"); endStr != "" {
		if parsedEnd, err := time.Parse(time.RFC3339, endStr); err == nil {
			endTime = parsedEnd
		} else if parsedEnd, err := time.Parse("2006-01-02", endStr); err == nil {
			// For date-only format, set to end of day in database timezone
			endTime = time.Date(parsedEnd.Year(), parsedEnd.Month(), parsedEnd.Day(), 23, 59, 59, 999999999, e.dbLocation)
		} else {
			return time.Time{}, time.Time{}, errors.New("invalid end time format, use RFC3339 (2006-01-02T15:04:05Z) or date format (2006-01-02)")
		}
	}

	// Ensure start is before end
	if startTime.After(endTime) {
		return time.Time{}, time.Time{}, errors.New("start time must be before end time")
	}

	return startTime, endTime, nil
}

// parseOIDRange parses start and optional end OID from HTTP request query parameters
func parseOIDRange(r *http.Request) (int64, int64, error) {
	query := r.URL.Query()

	// Parse start_oid parameter (required)
	startOID := int64(0)
	if startOIDStr := query.Get("start_oid"); startOIDStr != "" {
		if parsedStartOID, err := strconv.ParseInt(startOIDStr, 10, 64); err == nil {
			startOID = parsedStartOID
		} else {
			return 0, 0, errors.New("invalid start_oid format, must be a valid integer")
		}
	}

	// Parse end_oid parameter (optional)
	endOID := int64(0) // 0 means no end limit
	if endOIDStr := query.Get("end_oid"); endOIDStr != "" {
		if parsedEndOID, err := strconv.ParseInt(endOIDStr, 10, 64); err == nil {
			endOID = parsedEndOID
		} else {
			return 0, 0, errors.New("invalid end_oid format, must be a valid integer")
		}
	}

	// Ensure start is before or equal to end (if end is specified)
	if endOID > 0 && startOID > endOID {
		return 0, 0, errors.New("start_oid must be less than or equal to end_oid")
	}

	return startOID, endOID, nil
}

// loadLastOID loads the last processed OID from file
func (e *DelProExporter) loadLastOID() {
	if data, err := os.ReadFile(e.oidFile); err == nil {
		if oid, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			e.lastOID = oid
			log.Printf("Loaded last processed OID: %d", e.lastOID)
		}
	}
}

// saveLastOID saves the last processed OID to file
func (e *DelProExporter) saveLastOID() {
	data := strconv.FormatInt(e.lastOID, 10)
	if err := os.WriteFile(e.oidFile, []byte(data), 0644); err != nil {
		log.Printf("Failed to save last OID: %v", err)
	}
}

// SetLastOID sets the last processed OID if the new value is larger than current
func (e *DelProExporter) SetLastOID(newOID int64) {
	if newOID > e.lastOID {
		log.Printf("Overriding last processed OID from %d to %d", e.lastOID, newOID)
		e.lastOID = newOID
		e.saveLastOID()
	} else {
		log.Printf("Specified OID %d is not larger than current OID %d, ignoring", newOID, e.lastOID)
	}
}

// initializeCounters sets all counters to 0 for animals that have milked in the past 24h
func (e *DelProExporter) initializeCounters() {
	log.Printf("Initializing counters for animals from past 24h...")

	// Create context with timeout for database operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Query last 24h of records to get all animals that might need initialization
	now := time.Now()
	records, err := e.db.GetMilkingRecords(ctx, now.Add(-24*time.Hour), now, 0)
	if err != nil {
		log.Printf("Error getting records for counter initialization: %v", err)
		return
	}

	// Create a set to track unique animal combinations to avoid duplicate initializations
	seenAnimals := make(map[string]bool)
	initializedCount := 0

	for _, record := range records {
		// Create a unique key for this animal's metric labels
		key := record.LabelStr()

		if !seenAnimals[key] {
			// Initialize all counter metrics to 0 for this animal
			e.metrics.InitializeCountersToZero(record)
			seenAnimals[key] = true
			initializedCount++
		}
	}

	log.Printf("Initialized counters for %d unique animals from past 24h", initializedCount)
}

// WritePrometheus writes current metrics in standard Prometheus format
func (e *DelProExporter) WritePrometheus(w io.Writer, exposeProcessMetrics bool) {
	metrics.WritePrometheus(w, exposeProcessMetrics)
}
