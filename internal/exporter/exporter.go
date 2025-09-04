package exporter

import (
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
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
}

// NewDelProExporter creates a new DelPro exporter instance
func NewDelProExporter(host, port, dbname, user, password string) *DelProExporter {
	return &DelProExporter{
		db:      database.NewClient(host, port, dbname, user, password),
		metrics: delprometrics.NewExporter(),
	}
}

// Close closes the database connection
func (e *DelProExporter) Close() error {
	return e.db.Close()
}

// UpdateMetrics collects and updates current metrics from the database
func (e *DelProExporter) UpdateMetrics() {
	now := time.Now()
	records, err := e.db.GetMilkingRecords(now.Add(-models.DefaultLookbackWindow), now)
	if err != nil {
		log.Printf("Error collecting milking metrics: %v", err)
		return
	}

	// Use the default global metrics set with no writer for current metrics
	e.metrics.CreateMetricsFromRecords(nil, nil, records)

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

	records, err := e.db.GetMilkingRecords(startTime, endTime)
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

	e.metrics.WriteHistoricalMetrics(w, records)
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

// WritePrometheus writes current metrics in standard Prometheus format
func (e *DelProExporter) WritePrometheus(w io.Writer, exposeProcessMetrics bool) {
	metrics.WritePrometheus(w, exposeProcessMetrics)
}
