package metrics

import (
	"fmt"
	"io"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/clementnuss/delpro-exporter/internal/models"
)

// Exporter handles metrics creation and exposition
type Exporter struct{}

// NewExporter creates a new metrics exporter instance
func NewExporter() *Exporter {
	return &Exporter{}
}

// CreateMetricsFromRecords creates VictoriaMetrics from milking records
func (e *Exporter) CreateMetricsFromRecords(records []models.MilkingRecord) {
	for _, r := range records {
		// Core metrics
		metrics.GetOrCreateGauge(r.MetricName(models.MetricMilkYield), nil).Set(r.Yield)
		metrics.GetOrCreateCounter(r.MetricName(models.MetricMilkSessions)).Inc()

		// Optional metrics
		if r.Conductivity != nil {
			metrics.GetOrCreateGauge(r.MetricName(models.MetricConductivity), nil).Set(float64(*r.Conductivity))
		}
		if r.Duration != nil {
			metrics.GetOrCreateGauge(r.MetricName(models.MetricMilkingDuration), nil).Set(float64(*r.Duration))
		}
	}
}

// CreateDeviceUtilizationMetrics creates device utilization metrics
func (e *Exporter) CreateDeviceUtilizationMetrics(utilization map[string]int) {
	for deviceID, sessionCount := range utilization {
		metrics.GetOrCreateGauge(fmt.Sprintf(`%s{milk_device_id="%s"}`, models.MetricDeviceUtilization, deviceID), nil).Set(float64(sessionCount))
	}
}

// WriteHistoricalMetrics writes metrics with timestamps in Prometheus exposition format
func (e *Exporter) WriteHistoricalMetrics(w io.Writer, records []models.MilkingRecord) {
	s := metrics.NewSet()
	for _, r := range records {
		milkGauge := s.GetOrCreateGauge(r.MetricName(models.MetricMilkYield), nil)
		milkGauge.Add(r.Yield)
		marshalGauge(w, r.MetricName(models.MetricMilkYield), milkGauge, r.EndTime)
	}
}

// marshalGauge writes a gauge metric with timestamp in Prometheus format
func marshalGauge(w io.Writer, prefix string, g *metrics.Gauge, t time.Time) {
	v := g.Get()
	timestampMs := t.UnixMilli()

	if float64(int64(v)) == v {
		// Use integer format for whole numbers to avoid scientific notation
		fmt.Fprintf(w, "%s %d %d\n", prefix, int64(v), timestampMs)
	} else {
		fmt.Fprintf(w, "%s %g %d\n", prefix, v, timestampMs)
	}
}