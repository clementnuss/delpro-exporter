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
func (e *Exporter) CreateMetricsFromRecords(s *metrics.Set, w io.Writer, records []models.MilkingRecord) {
	if s == nil {
		s = metrics.GetDefaultSet()
	}

	for _, r := range records {
		yieldGauge := s.GetOrCreateGauge(r.MetricName(models.MetricMilkYield), nil)
		sessionCounter := s.GetOrCreateCounter(r.MetricName(models.MetricMilkSessions))

		yieldGauge.Set(r.Yield)
		sessionCounter.Inc()

		if w != nil {
			marshalGauge(w, r.MetricName(models.MetricMilkYield), yieldGauge, r.EndTime)
			marshalCounter(w, r.MetricName(models.MetricMilkSessions), sessionCounter, r.EndTime)
		}

		// Optional metrics
		if r.Conductivity != nil {
			conductivityGauge := s.GetOrCreateGauge(r.MetricName(models.MetricConductivity), nil)
			conductivityGauge.Set(float64(*r.Conductivity))
			if w != nil {
				marshalGauge(w, r.MetricName(models.MetricConductivity), conductivityGauge, r.EndTime)
			}
		}
		if r.Duration != nil {
			durationGauge := s.GetOrCreateGauge(r.MetricName(models.MetricMilkingDuration), nil)
			durationGauge.Set(float64(*r.Duration))
			if w != nil {
				marshalGauge(w, r.MetricName(models.MetricMilkingDuration), durationGauge, r.EndTime)
			}
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
	e.CreateMetricsFromRecords(s, w, records)
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

// marshalCounter writes a counter metric with timestamp in Prometheus format
func marshalCounter(w io.Writer, prefix string, c *metrics.Counter, t time.Time) {
	v := c.Get()
	timestampMs := t.UnixMilli()
	fmt.Fprintf(w, "%s %d %d\n", prefix, v, timestampMs)
}
