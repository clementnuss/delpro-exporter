package exporter

import (
	"io"
	"log"
	"net/http"
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
	records, err := e.db.GetMilkingRecords(models.DefaultLookbackHours * time.Hour)
	if err != nil {
		log.Printf("Error collecting milking metrics: %v", err)
		return
	}

	e.metrics.CreateMetricsFromRecords(records)

	utilization, err := e.db.GetDeviceUtilization()
	if err != nil {
		log.Printf("Error collecting device utilization: %v", err)
		return
	}

	e.metrics.CreateDeviceUtilizationMetrics(utilization)
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
func (e *DelProExporter) WriteHistoricalMetrics(r *http.Request, w io.Writer) {
	records, err := e.db.GetMilkingRecords(models.HistoricalLookbackHours * time.Hour)
	if err != nil {
		log.Printf("Unable to collect historical milking metrics: %v", err)
		return
	}

	e.metrics.WriteHistoricalMetrics(w, records)
}

// GetMilkingRecords is a public method to get milking records for a specific duration
func (e *DelProExporter) GetMilkingRecords(lookbackDuration time.Duration) ([]models.MilkingRecord, error) {
	return e.db.GetMilkingRecords(lookbackDuration)
}

// WritePrometheus writes current metrics in standard Prometheus format
func (e *DelProExporter) WritePrometheus(w io.Writer, exposeProcessMetrics bool) {
	metrics.WritePrometheus(w, exposeProcessMetrics)
}

