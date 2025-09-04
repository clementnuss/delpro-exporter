package exporter

import (
	"compress/gzip"
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
	db      *database.Client
	metrics *delprometrics.Exporter
	oidFile string
	lastOID int64
}

// NewDelProExporter creates a new DelPro exporter instance
func NewDelProExporter(host, port, dbname, user, password string) *DelProExporter {
	exporter := &DelProExporter{
		db:      database.NewClient(host, port, dbname, user, password),
		metrics: delprometrics.NewExporter(),
		oidFile: "delpro_last_oid.txt",
	}

	// Load last processed OID from file
	exporter.loadLastOID()

	return exporter
}

// Close closes the database connection
func (e *DelProExporter) Close() error {
	return e.db.Close()
}

// UpdateMetrics collects and updates current metrics from the database
func (e *DelProExporter) UpdateMetrics() {
	// Get records since last processed OID to prevent duplicate counter increments
	now := time.Now()
	records, err := e.db.GetMilkingRecords(now.Add(-models.DefaultLookbackWindow), now, e.lastOID)
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

	utilization, err := e.db.GetDeviceUtilization()
	if err != nil {
		log.Printf("Error collecting device utilization: %v", err)
		return
	}

	e.metrics.CreateDeviceUtilizationMetrics(utilization)
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
func (e *DelProExporter) WriteHistoricalMetrics(r *http.Request, w http.ResponseWriter) {
	// Parse query parameters for start and end dates
	startTime, endTime, err := parseTimeRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	records, err := e.db.GetMilkingRecords(startTime, endTime, 0)
	if err != nil {
		log.Printf("Unable to collect historical milking metrics: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
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

	e.metrics.WriteHistoricalMetrics(writer, records)
}

// parseTimeRange parses start and end time from HTTP request query parameters
func parseTimeRange(r *http.Request) (time.Time, time.Time, error) {
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
			startTime = parsedStart
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
			// For date-only format, set to end of day
			endTime = parsedEnd.Add(24*time.Hour - time.Nanosecond)
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

// WritePrometheus writes current metrics in standard Prometheus format
func (e *DelProExporter) WritePrometheus(w io.Writer, exposeProcessMetrics bool) {
	metrics.WritePrometheus(w, exposeProcessMetrics)
}
